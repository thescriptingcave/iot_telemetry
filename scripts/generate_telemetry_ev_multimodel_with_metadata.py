#!/usr/bin/env python3
"""
EV Telemetry & Device Metadata Simulator with MinIO Sink
- Emits device_metadata ONCE at startup (not in telemetry loop).
- Emits streaming sensor data (temperature, humidity, etc.) in batches.
"""

from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional
import time
import random
import sys

# MinIO client (pip install minio)
from minio import Minio
from minio.error import S3Error

# Avro (pip install avro-python3 — NOT 'avro'!)
from avro.schema import parse
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumWriter, DatumReader
from avro.io import BinaryEncoder, BinaryDecoder

# --- CONFIGURATION (adjust as needed) ---
SCHEMAS_DIR = Path("./schemas")
DATA_DIR = Path("./data")
MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = env_str("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = env_str("MINIO_ROOT_PASSWORD", "minioadmin")
MINIO_SECURE = False
MINIO_BUCKET = "ev-telemetry"

DATASET_PREFIX = "ev_telemetry"
SENSOR_TYPES = ["temperature", "humidity", "vibration", "evse_electrical", "evse_state"]

MAX_RECORDS = None          # e.g., 1_000_000 or None for unlimited
MAX_RECORDS_PER_TYPE = 10_000  # stop each stream individually
BATCH_SIZE_PER_TICK = 5
FLUSH_EVERY_N_TICKS = 20
MAX_BUFFER_RECORDS = 1000
TICK_SECONDS = 1.0

CHARGER_COUNT = 2
CONNECTORS_PER_CHARGER = 3
SITE_COUNT = 3
SESSION_START_PROB_PER_TICK = 0.02
SESSION_STOP_PROB_PER_TICK = 0.01
FAULT_PROB_PER_TICK = 0.005

# --- SCHEMAS (loaded at runtime) ---
SCHEMAS: Dict[str, Any] = {}

# --- MOCKED WORLD (for simulation only; adapt to your real world) ---
class Connector:
    def __init__(self, device_id, site_id, asset_id, connector_id, charger_kind, firmware_version):
        self.device_id = device_id
        self.site_id = site_id
        self.asset_id = asset_id
        self.connector_id = connector_id
        self.charger_kind = charger_kind
        self.firmware_version = firmware_version

def now_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)

def build_world() -> Any:
    """Return a mock world object with connectors."""
    connectors = []
    for c in range(CHARGER_COUNT):
        for i in range(CONNECTORS_PER_CHARGER):
            conn = Connector(
                device_id=f"dev-{c:03d}-{i}",
                site_id=f"site-{c % SITE_COUNT + 1:03d}",
                asset_id=f"asset-{c:03d}",
                connector_id=i + 1,
                charger_kind="CCS1" if i % 2 == 0 else "CHAdeMO",
                firmware_version="2.3.1"
            )
            connectors.append(conn)
    return type("SimWorld", (), {"connectors": connectors})()

# --- SCHEMA LOADING & VALIDATION ---
def load_schemas():
    for st in list(SCHEMAS.keys()) + SENSOR_TYPES + ["device_metadata"]:
        schema_file = SCHEMAS_DIR / f"{st}.avsc"
        if not schema_file.exists():
            raise FileNotFoundError(f"Schema file missing: {schema_file}")
        with open(schema_file, "r") as f:
            SCHEMAS[st] = parse(f.read())

# --- DEVICE METADATA (one-time snapshot) ---
def build_device_metadata_record(conn: Connector, startup_time: datetime) -> Dict[str, Any]:
    site_mapping = {
        "site-001": {"city": "Sacramento", "state": "CA", "region": "Capital Region",
                     "grid_zone": "CAISO-NORTH", "lat": 38.5816, "lon": -121.4944},
        "site-002": {"city": "Fresno", "state": "CA", "region": "Central Valley",
                     "grid_zone": "CAISO-CENTRAL", "lat": 36.7378, "lon": -119.7871},
        "site-003": {"city": "Stockton", "state": "CA", "region": "Central Valley",
                     "grid_zone": "CAISO-CENTRAL", "lat": 37.9577, "lon": -121.2908},
    }

    site_key = conn.site_id
    mapping = site_mapping.get(site_key, site_mapping["site-001"])

    # Random install date: 30–365 days before startup (stable per connector)
    days_offset = random.randint(30, 365)
    installed_at = startup_time.replace(hour=12, minute=0, second=0, microsecond=0) - timedelta(days=days_offset)

    return {
        "device_id": conn.device_id,
        "site_id": conn.site_id,
        "asset_id": conn.asset_id,
        "connector_id": conn.connector_id,
        "location_name": f"EV Site {conn.site_id}",
        "city": mapping["city"],
        "state": mapping["state"],
        "region": mapping["region"],
        "grid_zone": mapping["grid_zone"],
        "latitude": mapping["lat"],
        "longitude": mapping["lon"],
        "charger_kind": conn.charger_kind,
        "firmware_version": conn.firmware_version,
        "installed_at_ms": now_ms(installed_at),
    }

def emit_device_metadata(world: Any, client: Minio, file_seq: int, startup_time: datetime) -> int:
    records = [build_device_metadata_record(conn, startup_time) for conn in world.connectors]
    if not records:
        print("⚠️ No connectors — skipping device metadata.")
        return file_seq

    snapshot_date = startup_time.strftime("%Y%m%d")
    prefix = f"{DATASET_PREFIX}/device_metadata/snapshot={snapshot_date}/"
    filename = f"devices_{int(time.time())}_{file_seq}.avro"
    object_key = f"{prefix}{filename}"

    local_dir = DATA_DIR / prefix
    local_dir.mkdir(parents=True, exist_ok=True)
    local_path = local_dir / filename

    # Write Avro
    with open(local_path, "wb") as out:
        writer = DatumWriter(SCHEMAS["device_metadata"])
        dw = DataFileWriter(out, writer, SCHEMAS["device_metadata"])
        for rec in records:
            dw.append(rec)
        dw.close()

    # Upload to MinIO (with retry)
    retries = 3
    for attempt in range(1, retries + 1):
        try:
            client.fput_object(MINIO_BUCKET, object_key, str(local_path))
            break
        except S3Error as e:
            if attempt == retries:
                raise
            print(f"⚠️ MinIO upload failed (attempt {attempt}): {e}; retrying...")
            time.sleep(1)

    print(f"✅ Device metadata snapshot: {len(records)} devices → {object_key}")
    return file_seq + 1

# --- PARTITION & UTILS ---
def partition_prefix(sensor_type: str, ts: datetime) -> str:
    return f"{DATASET_PREFIX}/{sensor_type}/year={ts.year}/month={ts.month:02d}/day={ts.day:02d}/hour={ts.hour:02d}"

def ensure_bucket(client: Minio, bucket: str, retries: int = 3) -> None:
    for attempt in range(1, retries + 1):
        try:
            if not client.bucket_exists(bucket):
                client.make_bucket(bucket)
            break
        except S3Error as e:
            if attempt == retries:
                raise
            print(f"⚠️ Bucket check/create failed (attempt {attempt}): {e}; retrying...")
            time.sleep(1)

def should_stop(timestamp: datetime, count_by_type: Dict[str, int]) -> bool:
    """Stop when *any* sensor type hits MAX_RECORDS_PER_TYPE (common for simulations)."""
    if MAX_RECORDS is not None:
        total = sum(count_by_type.values())
        if total >= MAX_RECORDS:
            return True
    for st in SENSOR_TYPES:
        if count_by_type.get(st, 0) >= MAX_RECORDS_PER_TYPE:
            return True
    return False

# --- SENSOR DATA GENERATION (mocked) ---
def generate_sensor_record(sensor_type: str, connector: Connector, ts: datetime) -> Dict[str, Any]:
    # Default values for all sensor types
    base = {
        "device_id": connector.device_id,
        "site_id": connector.site_id,
        "connector_id": connector.connector_id,
        "asset_id": connector.asset_id,
        "timestamp_ms": now_ms(ts),
        "record_type": sensor_type,
    }

    # Type-specific fields (extend as needed)
    if sensor_type == "temperature":
        base["temp_c"] = round(random.uniform(15.0, 45.0), 2)
        base["ambient_c"] = round(random.uniform(10.0, 35.0), 2)
    elif sensor_type == "humidity":
        base["humidity_pct"] = round(random.uniform(20.0, 80.0), 2)
    elif sensor_type == "vibration":
        base["vibration_x"] = round(random.uniform(-0.5, 0.5), 3)
        base["vibration_y"] = round(random.uniform(-0.5, 0.5), 3)
        base["vibration_z"] = round(random.uniform(-0.5, 0.5), 3)
    elif sensor_type == "evse_electrical":
        base["voltage_v"] = round(random.uniform(350.0, 450.0), 1)
        base["current_a"] = round(random.uniform(0.0, 150.0), 1)
        base["power_kw"] = round(base["voltage_v"] * base["current_a"] / 1000, 2)
    elif sensor_type == "evse_state":
        base["state"] = random.choice(["idle", "session", "fault"])
        base["session_id"] = None if base["state"] == "idle" else f"sess_{random.randint(10000, 99999)}"
    return base

# --- MAIN SIMULATOR LOOP ---
def main():
    # Setup
    load_schemas()
    client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=MINIO_SECURE)
    ensure_bucket(client, MINIO_BUCKET)
    world = build_world()
    startup_time = datetime.now(timezone.utc)

    # Track records
    count_by_type: Dict[str, int] = {st: 0 for st in SENSOR_TYPES}
    buffer: Dict[str, List[Dict[str, Any]]] = {st: [] for st in SENSOR_TYPES}
    file_seq = 0

    # Emit device metadata ONCE at startup
    file_seq = emit_device_metadata(world, client, file_seq, startup_time)

    # Simulation loop
    tick_count = 0
    while not should_stop(startup_time, count_by_type):
        tick_count += 1
        ts = datetime.now(timezone.utc)

        # Collect sensor data for each connector
        for connector in world.connectors:
            for st in SENSOR_TYPES:
                # Probabilistic generation (optional, or always generate)
                if random.random() < 0.7:  # 70% chance per tick
                    rec = generate_sensor_record(st, connector, ts)
                    buffer[st].append(rec)

        # Flush buffer when full
        for st in SENSOR_TYPES:
            if len(buffer[st]) >= MAX_BUFFER_RECORDS:
                # Generate local file
                part = partition_prefix(st, ts)
                filename = f"{st}_{int(time.time())}_{file_seq}.avro"
                local_path = DATA_DIR / part / filename
                local_path.parent.mkdir(parents=True, exist_ok=True)

                with open(local_path, "wb") as out:
                    writer = DatumWriter(SCHEMAS[st])
                    dw = DataFileWriter(out, writer, SCHEMAS[st])
                    for rec in buffer[st]:
                        dw.append(rec)
                    dw.close()

                # Upload to MinIO (with retry)
                retries = 3
                for attempt in range(1, retries + 1):
                    try:
                        object_key = f"{part}/{filename}"
                        client.fput_object(MINIO_BUCKET, object_key, str(local_path))
                        count_by_type[st] += len(buffer[st])
                        break
                    except S3Error as e:
                        if attempt == retries:
                            raise
                        print(f"⚠️ MinIO upload failed (attempt {attempt}): {e}; retrying...")
                        time.sleep(1)

                buffer[st] = []

        # Periodic flush
        if tick_count % FLUSH_EVERY_N_TICKS == 0:
            for st in SENSOR_TYPES:
                if buffer[st]:
                    # Write & upload as above (reuse logic)
                    part = partition_prefix(st, ts)
                    filename = f"{st}_{int(time.time())}_{file_seq}.avro"
                    local_path = DATA_DIR / part / filename
                    local_path.parent.mkdir(parents=True, exist_ok=True)

                    with open(local_path, "wb") as out:
                        writer = DatumWriter(SCHEMAS[st])
                        dw = DataFileWriter(out, writer, SCHEMAS[st])
                        for rec in buffer[st]:
                            dw.append(rec)
                        dw.close()

                    retries = 3
                    for attempt in range(1, retries + 1):
                        try:
                            object_key = f"{part}/{filename}"
                            client.fput_object(MINIO_BUCKET, object_key, str(local_path))
                            count_by_type[st] += len(buffer[st])
                            break
                        except S3Error as e:
                            if attempt == retries:
                                raise
                            print(f"⚠️ MinIO upload failed (attempt {attempt}): {e}; retrying...")
                            time.sleep(1)

                    buffer[st] = []

        # Sleep for TICK_SECONDS (simulate real-time)
        time.sleep(TICK_SECONDS)

    # Final flush
    for st in SENSOR_TYPES:
        if buffer[st]:
            part = partition_prefix(st, ts)
            filename = f"{st}_{int(time.time())}_{file_seq}.avro"
            local_path = DATA_DIR / part / filename
            local_path.parent.mkdir(parents=True, exist_ok=True)

            with open(local_path, "wb") as out:
                writer = DatumWriter(SCHEMAS[st])
                dw = DataFileWriter(out, writer, SCHEMAS[st])
                for rec in buffer[st]:
                    dw.append(rec)
                dw.close()

            retries = 3
            for attempt in range(1, retries + 1):
                try:
                    object_key = f"{part}/{filename}"
                    client.fput_object(MINIO_BUCKET, object_key, str(local_path))
                    count_by_type[st] += len(buffer[st])
                    break
                except S3Error as e:
                    if attempt == retries:
                        raise
                    print(f"⚠️ MinIO upload failed (attempt {attempt}): {e}; retrying...")
                    time.sleep(1)

            buffer[st] = []

    print(f"✅ Simulation complete: {count_by_type}")

if __name__ == "__main__":
    main()
