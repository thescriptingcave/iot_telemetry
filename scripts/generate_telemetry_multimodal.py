#!/usr/bin/env python3
"""
IoT Telemetry Generator → MinIO (Parquet ZSTD, partitioned by sensor_type + time)

This version is designed specifically to support multivariable analysis by ensuring:
- The SAME asset/device_id emits temperature + humidity + vibration data.
- Timestamps are generated from a shared "tick time" so streams overlap in time.
- Optional fault injection creates correlated anomalies across modalities (temp + vibration).

Requires:
- minio
- fastavro
- pyarrow

Run:
  python generate_telemetry_multimodal.py

Common env vars:
  MINIO_ENDPOINT           (default: localhost:9000)
  MINIO_ACCESS_KEY         (default: minioadmin)
  MINIO_SECRET_KEY         (default: minioadmin)
  MINIO_SECURE             (default: false)
  MINIO_BUCKET             (default: iot-telemetry)

  SCHEMAS_DIR              (default: ./schemas)  # expects temperature.avsc, humidity.avsc, vibration.avsc
  DATA_DIR                 (default: ./data/avro_incoming)

  SENSOR_COUNT             (default: 5)          # number of assets to simulate
  ASSET_PREFIX             (default: sensor-)    # device_id will be like sensor-01, sensor-02, ...

  TICK_SECONDS             (default: 5)          # seconds between batches
  BATCH_SIZE_PER_TICK       (default: 1)          # records per asset per sensor per tick

  MAX_RECORDS_PER_TYPE     (default: 0)          # stop when EACH type reaches this count (0 = ignore)
  MAX_RECORDS              (default: 0)          # stop when total across all types reaches this count (0 = ignore)

Fault injection:
  FAULT_PROB_PER_TICK      (default: 0.01)       # probability an asset enters a fault state on a tick
  FAULT_TICKS_MIN          (default: 12)         # min ticks a fault lasts
  FAULT_TICKS_MAX          (default: 48)         # max ticks a fault lasts
  FAULT_TEMP_DELTA_C       (default: 20.0)       # temperature uplift during fault
  FAULT_RMS_MULTIPLIER     (default: 2.5)        # multiply vibration RMS during fault
  FAULT_FREQ_DELTA_HZ      (default: 10.0)       # shift peak frequency during fault
"""

from __future__ import annotations

import json
import os
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from fastavro import parse_schema

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False
    print("WARNING: pyarrow not installed. Install with: pip install pyarrow")

from minio import Minio


# ---------------------------
# Config
# ---------------------------

def env_bool(name: str, default: bool) -> bool:
    return os.getenv(name, str(default)).strip().lower() in ("1", "true", "t", "yes", "y")

def env_int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)).strip())

def env_float(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)).strip())


MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = env_bool("MINIO_SECURE", False)
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "iot-telemetry")

SCHEMAS_DIR = Path(os.getenv("SCHEMAS_DIR", "./schemas")).resolve()
DATA_DIR = Path(os.getenv("DATA_DIR", "./data/avro_incoming")).resolve()

SENSOR_COUNT = env_int("SENSOR_COUNT", 5)
ASSET_PREFIX = os.getenv("ASSET_PREFIX", "sensor-")

TICK_SECONDS = env_int("TICK_SECONDS", 5)
BATCH_SIZE_PER_TICK = env_int("BATCH_SIZE_PER_TICK", 1)

MAX_RECORDS_PER_TYPE = env_int("MAX_RECORDS_PER_TYPE", 0)
MAX_RECORDS = env_int("MAX_RECORDS", 0)

FAULT_PROB_PER_TICK = env_float("FAULT_PROB_PER_TICK", 0.01)
FAULT_TICKS_MIN = env_int("FAULT_TICKS_MIN", 12)
FAULT_TICKS_MAX = env_int("FAULT_TICKS_MAX", 48)
FAULT_TEMP_DELTA_C = env_float("FAULT_TEMP_DELTA_C", 20.0)
FAULT_RMS_MULTIPLIER = env_float("FAULT_RMS_MULTIPLIER", 2.5)
FAULT_FREQ_DELTA_HZ = env_float("FAULT_FREQ_DELTA_HZ", 10.0)

LOCATION_POOL = [x.strip() for x in os.getenv("LOCATION_POOL", "warehouse-A,workshop-B,outdoor").split(",") if x.strip()]

SENSOR_MODELS = {
    "vibration": os.getenv("VIB_SENSOR_MODEL", "ADXL345"),
}


# ---------------------------
# Avro schema loading
# ---------------------------

def load_schema(schema_name: str) -> Dict[str, Any]:
    path = SCHEMAS_DIR / f"{schema_name}.avsc"
    if not path.exists():
        raise FileNotFoundError(f"Missing schema file: {path}")
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    return parse_schema(raw)

SCHEMAS: Dict[str, Dict[str, Any]] = {
    "temperature": load_schema("temperature"),
    "humidity": load_schema("humidity"),
    "vibration": load_schema("vibration"),
}

SENSOR_TYPES = ["temperature", "humidity", "vibration"]


# ---------------------------
# Asset + fault model
# ---------------------------

@dataclass
class Asset:
    device_id: str
    location: str

@dataclass
class FaultState:
    ticks_left: int


def build_assets(n: int) -> List[Asset]:
    assets: List[Asset] = []
    for i in range(1, n + 1):
        device_id = f"{ASSET_PREFIX}{i:02d}"
        location = random.choice(LOCATION_POOL) if LOCATION_POOL else "unknown"
        assets.append(Asset(device_id=device_id, location=location))
    return assets


# ---------------------------
# Record generators (schema-aligned)
# ---------------------------

def now_ms(ts: datetime) -> int:
    return int(ts.timestamp() * 1000)

def gen_temperature(asset: Asset, ts: datetime, fault: bool) -> Dict[str, Any]:
    # Normal operating temperature distribution
    base = random.gauss(25.0, 2.5)

    if fault:
        base += FAULT_TEMP_DELTA_C + random.gauss(0.0, 1.0)

    value = max(-40.0, min(125.0, base))

    # accuracy_pct is ["null", "float"] in schema
    accuracy: Optional[float] = None
    if random.random() < 0.85:
        accuracy = round(random.uniform(0.1, 0.6), 2)

    status = "CRITICAL" if value >= 55 else "HIGH" if value >= 40 else "NORMAL"

    return {
        "device_id": asset.device_id,
        "timestamp": now_ms(ts),
        "value_celsius": float(round(value, 2)),
        "unit": "C",
        "accuracy_pct": accuracy,
        "status": status,
    }

def gen_humidity(asset: Asset, ts: datetime, fault: bool) -> Dict[str, Any]:
    # Humidity varies by location; keep it simple but plausible.
    loc_bias = {"warehouse-A": 5.0, "workshop-B": 0.0, "outdoor": 10.0}.get(asset.location, 0.0)
    rh = random.gauss(45.0 + loc_bias, 8.0)
    if fault:
        # Fault could correspond to overheating / seal issues affecting humidity
        rh += random.gauss(-5.0, 4.0)
    rh = max(0.0, min(100.0, rh))

    # Very rough dew point approximation (good enough for synthetic telemetry)
    # dew_point_c tends to increase with humidity; clamp to reasonable range.
    dew = (rh - 40.0) * 0.2 + random.gauss(0.0, 1.0)
    dew = max(-20.0, min(35.0, dew))

    return {
        "device_id": asset.device_id,
        "timestamp": now_ms(ts),
        "relative_humidity_pct": float(round(rh, 1)),
        "dew_point_c": float(round(dew, 1)),
        "location": asset.location,
    }

def gen_vibration(asset: Asset, ts: datetime, fault: bool) -> Dict[str, Any]:
    # Normal vibration
    rms = random.uniform(0.05, 2.0)
    peak_freq = random.gauss(50.0, 5.0)
    bandwidth = random.uniform(5.0, 20.0)

    if fault:
        rms = min(12.0, rms * FAULT_RMS_MULTIPLIER + random.uniform(0.0, 1.0))
        peak_freq = peak_freq + FAULT_FREQ_DELTA_HZ + random.gauss(0.0, 1.0)
        bandwidth = min(50.0, bandwidth * random.uniform(1.2, 2.0))

    return {
        "device_id": asset.device_id,
        "timestamp": now_ms(ts),
        "rms_acceleration": float(round(rms, 3)),
        "peak_frequency_hz": float(round(peak_freq, 1)),
        "bandwidth_hz": float(round(bandwidth, 1)),
        "sensor_model": SENSOR_MODELS["vibration"],
    }


# ---------------------------
# Storage / partitioning
# ---------------------------

def partition_prefix(sensor_type: str, ts: datetime) -> str:
    # s3://bucket/<sensor_type>/year=YYYY/month=MM/day=DD/hour=HH/...
    return (
        f"{sensor_type}/"
        f"year={ts.year}/"
        f"month={ts.month:02d}/"
        f"day={ts.day:02d}/"
        f"hour={ts.hour:02d}"
    )


def ensure_bucket(client: Minio, bucket: str) -> None:
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)


def write_parquet_zstd(records: List[Dict[str, Any]], output_path: str) -> int:
    """Write records to Parquet file with ZSTD compression."""
    if not PYARROW_AVAILABLE:
        raise ImportError("pyarrow is required. Install with: pip install pyarrow")
    
    if not records:
        return 0
    
    table = pa.Table.from_pylist(records)
    
    pq.write_table(
        table,
        str(output_path),
        compression='zstd',
        compression_level=3,
        row_group_size=10000,
        use_dictionary=True
    )
    
    return len(records)


def should_stop(total_count: int, per_type_counts: Dict[str, int]) -> bool:
    if MAX_RECORDS and total_count >= MAX_RECORDS:
        return True
    if MAX_RECORDS_PER_TYPE:
        return all(per_type_counts.get(st, 0) >= MAX_RECORDS_PER_TYPE for st in SENSOR_TYPES)
    return False


# ---------------------------
# Main loop
# ---------------------------

def main() -> None:
    if SENSOR_COUNT <= 0:
        raise ValueError("SENSOR_COUNT must be > 0")

    for st in SENSOR_TYPES:
        schema_file = SCHEMAS_DIR / f"{st}.avsc"
        if not schema_file.exists():
            raise FileNotFoundError(f"Expected schema file not found: {schema_file}")

    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )
    ensure_bucket(client, MINIO_BUCKET)

    assets = build_assets(SENSOR_COUNT)
    faults: Dict[str, FaultState] = {}

    per_type_counts: Dict[str, int] = {st: 0 for st in SENSOR_TYPES}
    total_count = 0
    file_seq = 0

    print(f"✅ Connected to MinIO: endpoint={MINIO_ENDPOINT} secure={MINIO_SECURE}")
    print(f"✅ Bucket: {MINIO_BUCKET}")
    print(f"✅ Schemas dir: {SCHEMAS_DIR}")
    print(f"✅ Data dir: {DATA_DIR}")
    print(f"✅ Assets ({len(assets)}): {[a.device_id for a in assets]}")
    print(f"✅ Tick: every {TICK_SECONDS}s | batch_size_per_tick={BATCH_SIZE_PER_TICK}")
    if MAX_RECORDS_PER_TYPE:
        print(f"✅ Stop when EACH type reaches {MAX_RECORDS_PER_TYPE} records")
    if MAX_RECORDS:
        print(f"✅ Stop when TOTAL records reaches {MAX_RECORDS} records")
    print(f"✅ Fault injection: prob/tick={FAULT_PROB_PER_TICK}, duration=[{FAULT_TICKS_MIN},{FAULT_TICKS_MAX}] ticks")

    while True:
        tick_time = datetime.now(timezone.utc)

        # Update/enter fault states
        for a in assets:
            st = faults.get(a.device_id)
            if st and st.ticks_left > 0:
                st.ticks_left -= 1
                if st.ticks_left <= 0:
                    faults.pop(a.device_id, None)
            else:
                if random.random() < FAULT_PROB_PER_TICK:
                    faults[a.device_id] = FaultState(ticks_left=random.randint(FAULT_TICKS_MIN, FAULT_TICKS_MAX))

        # Build batches per sensor_type
        batches: Dict[str, List[Dict[str, Any]]] = {st: [] for st in SENSOR_TYPES}

        for _ in range(BATCH_SIZE_PER_TICK):
            for a in assets:
                in_fault = a.device_id in faults
                batches["temperature"].append(gen_temperature(a, tick_time, in_fault))
                batches["humidity"].append(gen_humidity(a, tick_time, in_fault))
                batches["vibration"].append(gen_vibration(a, tick_time, in_fault))

        # Write to local file then upload to MinIO
        for sensor_type, records in batches.items():
            if not records:
                continue

            prefix = partition_prefix(sensor_type, tick_time)
            local_dir = DATA_DIR / prefix
            local_dir.mkdir(parents=True, exist_ok=True)

            filename = f"{sensor_type}_{int(time.time())}_{file_seq}.parquet"
            file_seq += 1

            local_path = local_dir / filename
            count = write_parquet_zstd(records, local_path)

            object_key = f"{prefix}/{filename}"
            client.fput_object(MINIO_BUCKET, object_key, str(local_path))

            per_type_counts[sensor_type] += count
            total_count += count

        # Progress
        active_faults = len(faults)
        print(
            f"📦 tick={tick_time.isoformat()} "
            f"uploaded(temp={len(batches['temperature'])}, hum={len(batches['humidity'])}, vib={len(batches['vibration'])}) "
            f"totals={per_type_counts} faults_active={active_faults}"
        )

        if should_stop(total_count, per_type_counts):
            print("✅ Stop condition met.")
            print(f"   Total records: {total_count}")
            print(f"   Per-type counts: {per_type_counts}")
            break

        time.sleep(max(1, TICK_SECONDS))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n🛑 Stopped by user.")
