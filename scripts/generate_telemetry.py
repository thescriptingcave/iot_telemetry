#!/usr/bin/env python3
"""
IoT Telemetry Generator → MinIO (with Parquet ZSTD partitioning)

Usage:
    python generate_telemetry.py
    MAX_RECORDS=1000 python generate_telemetry.py

Dependencies:
    pip install minio fastavro pyarrow
"""

import os
import json
import random
import time
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Any

from minio import Minio
from minio.error import S3Error
from fastavro import parse_schema

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False
    print("WARNING: pyarrow not installed. Install with: pip install pyarrow")

# --- Load environment variables (env vars or defaults) ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "iot-telemetry")
MAX_RECORDS = int(os.getenv("MAX_RECORDS", "0"))

# Sensor devices
DEVICES = [
    {"id": "sensor-01", "type": "temperature", "model": "DS18B20"},
    {"id": "sensor-02", "type": "vibration", "model": "ADXL345"},
    {"id": "sensor-03", "type": "humidity", "model": "SHT31"},
    {"id": "sensor-04", "type": "temperature", "model": "BME280"},
    {"id": "sensor-05", "type": "vibration", "model": "MPU6050"},
]

# --- Project structure helpers ---
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
SCHEMAS_DIR = PROJECT_ROOT / "schemas"
DATA_DIR = PROJECT_ROOT / "data" / "avro_incoming"

# --- Load Avro schemas from JSON files ---
def load_schema(name: str):
    schema_path = SCHEMAS_DIR / f"{name}.avsc"
    if not schema_path.exists():
        raise FileNotFoundError(f"Missing schema file: {schema_path}")
    try:
        with open(schema_path, "r") as f:
            return parse_schema(json.load(f))
    except Exception as e:
        raise ValueError(f"Failed to parse Avro schema for '{name}': {e}")

SCHEMAS = {
    "temperature": load_schema("temperature"),
    "vibration": load_schema("vibration"),
    "humidity": load_schema("humidity"),
}


# --- Simulation logic ---
def generate_reading(sensor: Dict[str, Any], ts: datetime) -> Dict[str, Any]:
    """Generate realistic sensor data based on type."""
    device_id = sensor["id"]
    sensor_type = sensor["type"]
    base_time_ms = int(ts.timestamp() * 1000)

    if sensor_type == "temperature":
        base_temp = random.gauss(25.0, 3.0)
        value = max(-40.0, min(125.0, base_temp))
        return {
            "device_id": device_id,
            "timestamp": base_time_ms,
            "value_celsius": round(value, 2),
            "unit": "C",
            "accuracy_pct": round(random.uniform(0.1, 0.5), 2),
            "status": "CRITICAL" if value > 45 else "HIGH" if value > 35 else "NORMAL",
        }

    elif sensor_type == "vibration":
        freq = random.gauss(50.0, 5.0)
        rms = random.uniform(0.1, 8.0)
        return {
            "device_id": device_id,
            "timestamp": base_time_ms,
            "rms_acceleration": round(rms, 3),
            "peak_frequency_hz": round(freq, 1),
            "bandwidth_hz": round(random.uniform(5.0, 25.0), 1),
            "sensor_model": sensor["model"],
        }

    elif sensor_type == "humidity":
        rh = random.gauss(50.0, 10.0)
        rh = max(0.0, min(100.0, rh))

        # dew point approximation (Magnus formula)
        # T_d = (243.04 * α) / (17.625 - α), where α = ln(RH/100) + (17.625 * T)/(243.04 + T)
        # simplified to direct RH-only (no temp input): approximate for 25°C ambient
        alpha = rh / (17.625 + rh)
        dew = round(243.04 * alpha / (1 - alpha) * 0.07 + random.uniform(-2, 2), 1)
        dew = max(-50.0, min(50.0, dew))

        return {
            "device_id": device_id,
            "timestamp": base_time_ms,
            "relative_humidity_pct": round(rh, 1),
            "dew_point_c": dew,
            "location": random.choice(["warehouse-A", "workshop-B", "outdoor"]),
        }

    else:
        raise ValueError(f"Unknown sensor type: {sensor_type}")


def get_partition_path(ts: datetime, sensor_type: str) -> str:
    """Return partition path like: sensor_type/year=YYYY/month=MM/day=DD/hour=HH/"""
    return (
        f"{sensor_type}/"
        f"year={ts.year}/"
        f"month={ts.month:02}/"
        f"day={ts.day:02}/"
        f"hour={ts.hour:02}/"
    )


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


def main():
    # Ensure schema directory and files exist
    if not SCHEMAS_DIR.exists():
        raise FileNotFoundError(f"Schema directory missing: {SCHEMAS_DIR}")
    
    # Setup MinIO client
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )

    # Ensure bucket exists
    try:
        if not client.bucket_exists(MINIO_BUCKET):
            client.make_bucket(MINIO_BUCKET)
            print(f"✅ Created bucket: {MINIO_BUCKET}")
    except Exception as e:
        raise RuntimeError(f"Failed to connect or create bucket '{MINIO_BUCKET}': {e}")

    print(f"🚀 Starting telemetry generator → MinIO bucket: {MINIO_BUCKET}")

    record_count = 0
    try:
        while True:
            now = datetime.now(timezone.utc)
            hour_partition = get_partition_path(now, "")

            # Initialize batch per sensor type
            batch_by_type: Dict[str, List[Dict]] = {
                st: [] for st in SCHEMAS.keys()
            }

            for sensor in DEVICES:
                reading = generate_reading(sensor, now)
                st = sensor["type"]
                batch_by_type[st].append(reading)

            # Process each sensor type
            for sensor_type, records in batch_by_type.items():
                if not records:
                    continue

                # Partitioned path (without trailing slash)
                partition_prefix = get_partition_path(now, sensor_type)
                partition_prefix = partition_prefix.rstrip("/")

                # Write to temp (in-project data dir)
                temp_dir = DATA_DIR / partition_prefix
                temp_dir.mkdir(parents=True, exist_ok=True)

                filename = temp_dir / f"{sensor_type}_{int(time.time())}_{record_count}.parquet"
                count = write_parquet_zstd(records, filename)

                # Upload to MinIO with same partitioned key
                minio_key = f"{partition_prefix}/{filename.name}"
                client.fput_object(MINIO_BUCKET, minio_key, filename)

                print(f"📤 Uploaded {count} × {sensor_type} → s3://{MINIO_BUCKET}/{minio_key}")
                record_count += count

            # Rate limiting
            if MAX_RECORDS and record_count >= MAX_RECORDS:
                print(f"✅ Generated {record_count} records — stopping.")
                break

            time.sleep(random.uniform(1.0, 2.0))

    except KeyboardInterrupt:
        print("\n🛑 Interrupted. Shutdown complete.")
    except S3Error as e:
        print(f"❌ MinIO error: {e}")
    except Exception as e:
        print(f"💥 Unexpected error: {e}")
        raise


if __name__ == "__main__":
    main()
