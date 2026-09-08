#!/usr/bin/env python3
"""
EV/Grid-focused IoT Telemetry Generator → MinIO (Avro, partitioned by dataset_prefix + sensor_type + time)

Streams (Avro):
- temperature
- humidity
- vibration
- evse_electrical
- evse_state
- evse_session_event (event stream; sparse)
- device_metadata (dimension stream; written once at startup)

Key features:
1) DATASET_PREFIX: all objects are written under a versioned prefix (default: ev_v1)
2) Small-files fix: in-memory buffering + periodic flush (FLUSH_EVERY_N_TICKS)
3) US-wide sites with lat/lon + region/grid_zone stored in device_metadata for BI + geospatial analytics.

Dependencies:
  pip install minio fastavro

Required Avro schemas in SCHEMAS_DIR:
  temperature.avsc
  humidity.avsc
  vibration.avsc
  evse_electrical.avsc
  evse_state.avsc
  evse_session_event.avsc
  device_metadata.avsc

Env vars (common):
  MINIO_ENDPOINT           default: localhost:9000
  MINIO_ACCESS_KEY         default: minioadmin
  MINIO_SECRET_KEY         default: minioadmin
  MINIO_SECURE             default: false
  MINIO_BUCKET             default: iot-telemetry

  SCHEMAS_DIR              default: <project_root>/schemas
  DATA_DIR                 default: <project_root>/data/avro_incoming

Dataset / partitioning:
  DATASET_PREFIX           default: ev_v1

Ticking / flush:
  TICK_SECONDS             default: 2
  BATCH_SIZE_PER_TICK      default: 1        # records per connector per tick per continuous stream
  FLUSH_EVERY_N_TICKS      default: 300      # 10 minutes if TICK_SECONDS=2
  MAX_BUFFER_RECORDS       default: 50000    # safety cap; flush if any stream buffer exceeds this count

Stopping:
  MAX_RECORDS_PER_TYPE     default: 0        # stop when EACH telemetry stream reaches this count
  MAX_RECORDS              default: 0        # stop when total across telemetry streams reaches this count

World model:
  CHARGER_COUNT            default: 6
  CONNECTORS_PER_CHARGER   default: 2

Session simulation:
  SESSION_START_PROB_PER_TICK   default: 0.02
  SESSION_STOP_PROB_PER_TICK    default: 0.01
  AUTH_FAIL_PROB                default: 0.03

Fault injection:
  FAULT_PROB_PER_TICK      default: 0.01
  FAULT_TICKS_MIN          default: 12
  FAULT_TICKS_MAX          default: 48
  FAULT_TEMP_DELTA_C       default: 20.0
  FAULT_RMS_MULTIPLIER     default: 2.5
  FAULT_FREQ_DELTA_HZ      default: 10.0

Derating:
  DERATE_TEMP_START_C      default: 45.0
  DERATE_TEMP_FULL_C       default: 60.0

Electrical ranges:
  CHARGER_PROFILE          default: mixed  # mixed | L2 | DCFC
  L2_POWER_KW              default: 7.2
  DCFC_POWER_KW            default: 50.0

Notes on IDs:
- site_id is a stable identifier (string). In real systems it's often an opaque ID, but it's common to
  use a structured code for readability (e.g., site-us-ca-sacramento-001). The metadata table carries
  the human-friendly fields + geocoords.
"""

from __future__ import annotations

import json
import os
import random
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastavro import parse_schema, writer
from minio import Minio


def env_bool(name: str, default: bool) -> bool:
    return os.getenv(name, str(default)).strip().lower() in ("1", "true", "t", "yes", "y")


def env_int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)).strip())


def env_float(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)).strip())


def env_str(name: str, default: str) -> str:
    v = os.getenv(name)
    return v.strip() if v is not None and v.strip() else default


MINIO_ENDPOINT = env_str("MINIO_ENDPOINT", "127.0.0.1:9000")
MINIO_ACCESS_KEY = env_str("MINIO_ROOT_USER", "admin")
MINIO_SECRET_KEY = env_str("MINIO_ROOT_PASSWORD", "password")
MINIO_SECURE = env_bool("MINIO_SECURE", False)
MINIO_BUCKET = env_str("MINIO_BUCKET", "iot-telemetry")

DATASET_PREFIX = env_str("DATASET_PREFIX", "ev_v1")

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent if (SCRIPT_DIR.name == "scripts") else SCRIPT_DIR

SCHEMAS_DIR = Path(env_str("SCHEMAS_DIR", str(PROJECT_ROOT / "schemas"))).resolve()
DATA_DIR = Path(env_str("DATA_DIR", str(PROJECT_ROOT / "data" / "avro_incoming"))).resolve()

CHARGER_COUNT = env_int("CHARGER_COUNT", 6)
CONNECTORS_PER_CHARGER = env_int("CONNECTORS_PER_CHARGER", 2)

ASSET_PREFIX = env_str("ASSET_PREFIX", "charger-")
DEVICE_PREFIX = env_str("DEVICE_PREFIX", "sensor-")

TICK_SECONDS = env_int("TICK_SECONDS", 2)
BATCH_SIZE_PER_TICK = env_int("BATCH_SIZE_PER_TICK", 1)
FLUSH_EVERY_N_TICKS = env_int("FLUSH_EVERY_N_TICKS", 300)
MAX_BUFFER_RECORDS = env_int("MAX_BUFFER_RECORDS", 50000)

MAX_RECORDS_PER_TYPE = env_int("MAX_RECORDS_PER_TYPE", 0)
MAX_RECORDS = env_int("MAX_RECORDS", 0)

FAULT_PROB_PER_TICK = env_float("FAULT_PROB_PER_TICK", 0.01)
FAULT_TICKS_MIN = env_int("FAULT_TICKS_MIN", 12)
FAULT_TICKS_MAX = env_int("FAULT_TICKS_MAX", 48)
FAULT_TEMP_DELTA_C = env_float("FAULT_TEMP_DELTA_C", 20.0)
FAULT_RMS_MULTIPLIER = env_float("FAULT_RMS_MULTIPLIER", 2.5)
FAULT_FREQ_DELTA_HZ = env_float("FAULT_FREQ_DELTA_HZ", 10.0)

SESSION_START_PROB_PER_TICK = env_float("SESSION_START_PROB_PER_TICK", 0.02)
SESSION_STOP_PROB_PER_TICK = env_float("SESSION_STOP_PROB_PER_TICK", 0.01)
AUTH_FAIL_PROB = env_float("AUTH_FAIL_PROB", 0.03)

DERATE_TEMP_START_C = env_float("DERATE_TEMP_START_C", 45.0)
DERATE_TEMP_FULL_C = env_float("DERATE_TEMP_FULL_C", 60.0)

CHARGER_PROFILE = env_str("CHARGER_PROFILE", "mixed").lower()
L2_POWER_KW = env_float("L2_POWER_KW", 7.2)
DCFC_POWER_KW = env_float("DCFC_POWER_KW", 50.0)

FIRMWARE_POOL = [x.strip() for x in env_str("FIRMWARE_POOL", "1.0.0,1.1.0,1.2.0").split(",") if x.strip()]
VIB_SENSOR_MODEL = env_str("VIB_SENSOR_MODEL", "ADXL345")


US_SITES = [
    {"site_id": "site-us-ca-sacramento-001", "location_name": "Sacramento, CA", "city": "Sacramento", "state": "CA", "region": "West", "grid_zone": "CAISO", "lat": 38.5816, "lon": -121.4944},
    {"site_id": "site-us-ca-losangeles-001", "location_name": "Los Angeles, CA", "city": "Los Angeles", "state": "CA", "region": "West", "grid_zone": "CAISO", "lat": 34.0522, "lon": -118.2437},
    {"site_id": "site-us-wa-seattle-001", "location_name": "Seattle, WA", "city": "Seattle", "state": "WA", "region": "West", "grid_zone": "BPA", "lat": 47.6062, "lon": -122.3321},
    {"site_id": "site-us-az-phoenix-001", "location_name": "Phoenix, AZ", "city": "Phoenix", "state": "AZ", "region": "West", "grid_zone": "AZPS", "lat": 33.4484, "lon": -112.0740},
    {"site_id": "site-us-nv-lasvegas-001", "location_name": "Las Vegas, NV", "city": "Las Vegas", "state": "NV", "region": "West", "grid_zone": "NVENERGY", "lat": 36.1699, "lon": -115.1398},
    {"site_id": "site-us-tx-dallas-001", "location_name": "Dallas, TX", "city": "Dallas", "state": "TX", "region": "South", "grid_zone": "ERCOT", "lat": 32.7767, "lon": -96.7970},
    {"site_id": "site-us-tx-houston-001", "location_name": "Houston, TX", "city": "Houston", "state": "TX", "region": "South", "grid_zone": "ERCOT", "lat": 29.7604, "lon": -95.3698},
    {"site_id": "site-us-ga-atlanta-001", "location_name": "Atlanta, GA", "city": "Atlanta", "state": "GA", "region": "Southeast", "grid_zone": "SERC", "lat": 33.7490, "lon": -84.3880},
    {"site_id": "site-us-il-chicago-001", "location_name": "Chicago, IL", "city": "Chicago", "state": "IL", "region": "Midwest", "grid_zone": "PJM", "lat": 41.8781, "lon": -87.6298},
    {"site_id": "site-us-co-denver-001", "location_name": "Denver, CO", "city": "Denver", "state": "CO", "region": "Mountain", "grid_zone": "WECC", "lat": 39.7392, "lon": -104.9903},
    {"site_id": "site-us-ny-newyork-001", "location_name": "New York, NY", "city": "New York", "state": "NY", "region": "Northeast", "grid_zone": "NYISO", "lat": 40.7128, "lon": -74.0060},
    {"site_id": "site-us-ma-boston-001", "location_name": "Boston, MA", "city": "Boston", "state": "MA", "region": "Northeast", "grid_zone": "ISO-NE", "lat": 42.3601, "lon": -71.0589},
]


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
    "evse_electrical": load_schema("evse_electrical"),
    "evse_state": load_schema("evse_state"),
    "evse_session_event": load_schema("evse_session_event"),
    "device_metadata": load_schema("device_metadata"),
}

CONTINUOUS_STREAMS = ["temperature", "humidity", "vibration", "evse_electrical", "evse_state"]
EVENT_STREAMS = ["evse_session_event"]
SENSOR_TYPES = CONTINUOUS_STREAMS + EVENT_STREAMS


def now_ms(ts: datetime) -> int:
    return int(ts.timestamp() * 1000)


@dataclass
class FaultState:
    ticks_left: int


@dataclass
class Connector:
    site_id: str
    asset_id: str
    connector_id: int
    device_id: str

    location_name: str
    city: str
    state: str
    region: str
    grid_zone: str
    latitude: float
    longitude: float

    firmware_version: str
    charger_kind: str

    state_machine: str = "IDLE"
    available: bool = True
    fault_active: bool = False
    fault_code: Optional[str] = None
    derate_pct: Optional[float] = None
    session_id: Optional[str] = None
    energy_kwh_total: float = 0.0
    uptime_s: int = 0
    _was_derated: bool = False


@dataclass
class SimWorld:
    connectors: List[Connector] = field(default_factory=list)
    faults: Dict[str, FaultState] = field(default_factory=dict)


def choose_charger_kind() -> str:
    if CHARGER_PROFILE == "l2":
        return "L2"
    if CHARGER_PROFILE == "dcfc":
        return "DCFC"
    return "DCFC" if random.random() < 0.4 else "L2"


def build_world() -> SimWorld:
    if CHARGER_COUNT <= 0 or CONNECTORS_PER_CHARGER <= 0:
        raise ValueError("CHARGER_COUNT and CONNECTORS_PER_CHARGER must be > 0")

    if not US_SITES:
        raise ValueError("US_SITES is empty; need at least one site definition.")

    world = SimWorld()
    for i in range(1, CHARGER_COUNT + 1):
        site = US_SITES[(i - 1) % len(US_SITES)]
        asset_id = f"{ASSET_PREFIX}{i:02d}"
        firmware = random.choice(FIRMWARE_POOL) if FIRMWARE_POOL else "1.0.0"
        kind = choose_charger_kind()

        for c in range(1, CONNECTORS_PER_CHARGER + 1):
            device_id = f"{DEVICE_PREFIX}{i:02d}-c{c}"
            world.connectors.append(
                Connector(
                    site_id=site["site_id"],
                    asset_id=asset_id,
                    connector_id=c,
                    device_id=device_id,
                    location_name=site["location_name"],
                    city=site["city"],
                    state=site["state"],
                    region=site["region"],
                    grid_zone=site["grid_zone"],
                    latitude=float(site["lat"]),
                    longitude=float(site["lon"]),
                    firmware_version=firmware,
                    charger_kind=kind,
                )
            )
    return world


def gen_temperature(conn: Connector, ts: datetime, in_fault: bool, est_power_kw: float) -> Dict[str, Any]:
    base = random.gauss(28.0, 2.0)
    heat = 0.10 * est_power_kw if conn.charger_kind == "L2" else 0.18 * est_power_kw
    base += heat + random.gauss(0.0, 0.5)

    if in_fault:
        base += FAULT_TEMP_DELTA_C + random.gauss(0.0, 1.0)

    value = max(-20.0, min(125.0, base))
    accuracy = float(round(random.uniform(0.1, 0.6), 2)) if random.random() < 0.85 else None
    status = "CRITICAL" if value >= 60 else "HIGH" if value >= 45 else "NORMAL"

    return {
        "device_id": conn.device_id,
        "timestamp": now_ms(ts),
        "value_celsius": float(round(value, 2)),
        "unit": "C",
        "accuracy_pct": accuracy,
        "status": status,
    }


def gen_humidity(conn: Connector, ts: datetime, in_fault: bool) -> Dict[str, Any]:
    region_bias = {"West": 2.0, "Mountain": 1.0, "Midwest": 6.0, "South": 10.0, "Southeast": 12.0, "Northeast": 7.0}.get(conn.region, 5.0)
    rh = random.gauss(45.0 + region_bias, 8.0)
    if in_fault:
        rh += random.gauss(-3.0, 3.0)
    rh = max(0.0, min(100.0, rh))

    dew = (rh - 40.0) * 0.2 + random.gauss(0.0, 1.0)
    dew = max(-20.0, min(35.0, dew))

    return {
        "device_id": conn.device_id,
        "timestamp": now_ms(ts),
        "relative_humidity_pct": float(round(rh, 1)),
        "dew_point_c": float(round(dew, 1)),
        "location": conn.location_name,
    }


def gen_vibration(conn: Connector, ts: datetime, in_fault: bool, est_power_kw: float) -> Dict[str, Any]:
    rms = random.uniform(0.05, 0.8) + (0.02 * est_power_kw)
    peak_freq = random.gauss(50.0, 4.0)
    bandwidth = random.uniform(5.0, 18.0)

    if in_fault:
        rms = min(12.0, rms * FAULT_RMS_MULTIPLIER + random.uniform(0.0, 1.0))
        peak_freq = peak_freq + FAULT_FREQ_DELTA_HZ + random.gauss(0.0, 1.0)
        bandwidth = min(50.0, bandwidth * random.uniform(1.2, 2.0))

    return {
        "device_id": conn.device_id,
        "timestamp": now_ms(ts),
        "rms_acceleration": float(round(rms, 3)),
        "peak_frequency_hz": float(round(peak_freq, 1)),
        "bandwidth_hz": float(round(bandwidth, 1)),
        "sensor_model": VIB_SENSOR_MODEL,
    }


def compute_derate_pct(cabinet_temp_c: float) -> Optional[float]:
    if cabinet_temp_c < DERATE_TEMP_START_C:
        return None
    if cabinet_temp_c >= DERATE_TEMP_FULL_C:
        return 100.0
    frac = (cabinet_temp_c - DERATE_TEMP_START_C) / max(1e-6, (DERATE_TEMP_FULL_C - DERATE_TEMP_START_C))
    return float(round(100.0 * frac, 1))


def pick_fault_code() -> str:
    return random.choice(["OVER_TEMP", "GROUND_FAULT", "COMM_LOSS", "CONTACTOR_FAIL", "METER_FAIL"])


def maybe_start_session(conn: Connector) -> Optional[Dict[str, Any]]:
    if conn.state_machine != "IDLE" or not conn.available or conn.fault_active:
        return None
    if random.random() > SESSION_START_PROB_PER_TICK:
        return None

    if random.random() < AUTH_FAIL_PROB:
        sess_id = str(uuid.uuid4())
        return {
            "site_id": conn.site_id,
            "asset_id": conn.asset_id,
            "connector_id": conn.connector_id,
            "device_id": conn.device_id,
            "timestamp": None,
            "session_id": sess_id,
            "event_type": "AUTH_FAIL",
            "reason_code": "PAYMENT_DECLINED",
            "severity": "WARN",
            "meter_start_kwh": None,
            "meter_end_kwh": None,
            "energy_delivered_kwh": None,
            "duration_s": None,
            "user_id_hash": None,
        }

    conn.session_id = str(uuid.uuid4())
    conn.state_machine = "CHARGING"
    conn.available = False

    return {
        "site_id": conn.site_id,
        "asset_id": conn.asset_id,
        "connector_id": conn.connector_id,
        "device_id": conn.device_id,
        "timestamp": None,
        "session_id": conn.session_id,
        "event_type": "CHARGE_START",
        "reason_code": None,
        "severity": "INFO",
        "meter_start_kwh": None,
        "meter_end_kwh": None,
        "energy_delivered_kwh": None,
        "duration_s": None,
        "user_id_hash": None,
    }


def maybe_stop_session(conn: Connector) -> Optional[Dict[str, Any]]:
    if conn.state_machine != "CHARGING" or conn.session_id is None:
        return None
    if random.random() > SESSION_STOP_PROB_PER_TICK:
        return None

    evt = {
        "site_id": conn.site_id,
        "asset_id": conn.asset_id,
        "connector_id": conn.connector_id,
        "device_id": conn.device_id,
        "timestamp": None,
        "session_id": conn.session_id,
        "event_type": "CHARGE_STOP",
        "reason_code": "USER_STOP",
        "severity": "INFO",
        "meter_start_kwh": None,
        "meter_end_kwh": None,
        "energy_delivered_kwh": None,
        "duration_s": None,
        "user_id_hash": None,
    }
    conn.state_machine = "IDLE"
    conn.available = True
    conn.session_id = None
    conn._was_derated = False
    return evt


def fault_transition_event(conn: Connector, to_faulted: bool) -> Dict[str, Any]:
    if to_faulted:
        sess_id = conn.session_id or str(uuid.uuid4())
        return {
            "site_id": conn.site_id,
            "asset_id": conn.asset_id,
            "connector_id": conn.connector_id,
            "device_id": conn.device_id,
            "timestamp": None,
            "session_id": sess_id,
            "event_type": "FAULT",
            "reason_code": conn.fault_code or "UNKNOWN",
            "severity": "ERROR",
            "meter_start_kwh": None,
            "meter_end_kwh": None,
            "energy_delivered_kwh": None,
            "duration_s": None,
            "user_id_hash": None,
        }
    sess_id = conn.session_id or str(uuid.uuid4())
    return {
        "site_id": conn.site_id,
        "asset_id": conn.asset_id,
        "connector_id": conn.connector_id,
        "device_id": conn.device_id,
        "timestamp": None,
        "session_id": sess_id,
        "event_type": "RECOVERED",
        "reason_code": None,
        "severity": "INFO",
        "meter_start_kwh": None,
        "meter_end_kwh": None,
        "energy_delivered_kwh": None,
        "duration_s": None,
        "user_id_hash": None,
    }


def derate_event(conn: Connector) -> Dict[str, Any]:
    return {
        "site_id": conn.site_id,
        "asset_id": conn.asset_id,
        "connector_id": conn.connector_id,
        "device_id": conn.device_id,
        "timestamp": None,
        "session_id": conn.session_id or str(uuid.uuid4()),
        "event_type": "DERATE",
        "reason_code": "OVER_TEMP",
        "severity": "WARN",
        "meter_start_kwh": None,
        "meter_end_kwh": None,
        "energy_delivered_kwh": None,
        "duration_s": None,
        "user_id_hash": None,
    }


def estimate_target_power_kw(conn: Connector) -> float:
    if conn.charger_kind == "L2":
        return max(0.0, random.gauss(L2_POWER_KW, 0.6))
    return max(0.0, random.gauss(DCFC_POWER_KW, 6.0))


def gen_evse_state(conn: Connector, ts: datetime, cabinet_temp_c: float) -> Dict[str, Any]:
    state = conn.state_machine
    if conn.fault_active:
        state = "FAULTED"
    elif conn.derate_pct is not None and conn.derate_pct >= 1.0 and conn.state_machine == "CHARGING":
        state = "DERATED"

    return {
        "site_id": conn.site_id,
        "asset_id": conn.asset_id,
        "connector_id": int(conn.connector_id),
        "device_id": conn.device_id,
        "timestamp": now_ms(ts),
        "state": state,
        "available": bool(conn.available),
        "fault_active": bool(conn.fault_active),
        "fault_code": conn.fault_code,
        "derate_pct": conn.derate_pct,
        "charger_temp_c": float(round(cabinet_temp_c, 2)),
        "uptime_s": int(conn.uptime_s),
        "firmware_version": conn.firmware_version,
    }


def gen_evse_electrical(conn: Connector, ts: datetime, cabinet_temp_c: float, dt_hours: float) -> Dict[str, Any]:
    if conn.fault_active or conn.state_machine == "FAULTED":
        voltage = float(round(random.gauss(240.0, 2.0), 1)) if conn.charger_kind == "L2" else float(round(random.gauss(480.0, 5.0), 1))
        current = 0.0
        power = 0.0
        status = "FAULTED"
    elif conn.state_machine != "CHARGING":
        voltage = float(round(random.gauss(240.0, 2.0), 1)) if conn.charger_kind == "L2" else float(round(random.gauss(480.0, 5.0), 1))
        current = float(round(max(0.0, random.gauss(0.5, 0.5)), 2))
        power = float(round(max(0.0, voltage * current / 1000.0), 3))
        status = "OK"
    else:
        target_kw = estimate_target_power_kw(conn)
        if conn.derate_pct is not None:
            target_kw *= max(0.0, (100.0 - conn.derate_pct) / 100.0)

        voltage = float(round(random.gauss(240.0, 2.0), 1)) if conn.charger_kind == "L2" else float(round(random.gauss(480.0, 5.0), 1))
        current = float(round(max(0.0, (target_kw * 1000.0) / max(1.0, voltage) + random.gauss(0.0, 1.5)), 2))
        power = float(round(max(0.0, voltage * current / 1000.0), 3))
        status = "DERATED" if (conn.derate_pct is not None and conn.derate_pct >= 1.0) else "OK"

    conn.energy_kwh_total += float(power) * dt_hours

    pf = float(round(max(0.6, min(1.0, random.gauss(0.97, 0.03))), 3)) if random.random() < 0.8 else None
    gf = float(round(random.gauss(60.0, 0.04), 3)) if random.random() < 0.7 else None
    phase = "SINGLE_PHASE" if conn.charger_kind == "L2" else "THREE_PHASE"

    return {
        "site_id": conn.site_id,
        "asset_id": conn.asset_id,
        "connector_id": int(conn.connector_id),
        "device_id": conn.device_id,
        "timestamp": now_ms(ts),
        "voltage_v": float(voltage),
        "current_a": float(current),
        "power_kw": float(power),
        "energy_kwh_total": float(round(conn.energy_kwh_total, 6)),
        "power_factor": pf,
        "grid_frequency_hz": gf,
        "phase": phase,
        "temperature_cabinet_c": float(round(cabinet_temp_c, 2)),
        "derate_pct": conn.derate_pct,
        "status": status,
    }


def partition_prefix(sensor_type: str, ts: datetime) -> str:
    return f"{DATASET_PREFIX}/{sensor_type}/year={ts.year}/month={ts.month:02d}/day={ts.day:02d}/hour={ts.hour:02d}"


def ensure_bucket(client: Minio, bucket: str) -> None:
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)


def should_stop(total_count: int, per_type_counts: Dict[str, int]) -> bool:
    if MAX_RECORDS and total_count >= MAX_RECORDS:
        return True
    if MAX_RECORDS_PER_TYPE:
        return all(per_type_counts.get(st, 0) >= MAX_RECORDS_PER_TYPE for st in SENSOR_TYPES)
    return False


def write_avro_and_upload(client: Minio, sensor_type: str, records: List[Dict[str, Any]], ts: datetime, file_seq: int) -> int:
    if not records:
        return file_seq

    prefix = partition_prefix(sensor_type, ts)
    local_dir = DATA_DIR / prefix
    local_dir.mkdir(parents=True, exist_ok=True)

    filename = f"{sensor_type}_{int(time.time())}_{file_seq}.avro"
    file_seq += 1
    local_path = local_dir / filename

    with open(local_path, "wb") as out:
        writer(out, SCHEMAS[sensor_type], records)

    object_key = f"{prefix}/{filename}"
    client.fput_object(MINIO_BUCKET, object_key, str(local_path))

    return file_seq


def build_device_metadata_records(world: SimWorld, ts: datetime) -> List[Dict[str, Any]]:
    installed_at_ms = now_ms(ts) - random.randint(30, 365) * 24 * 3600 * 1000
    out: List[Dict[str, Any]] = []
    for c in world.connectors:
        out.append({
            "device_id": c.device_id,
            "site_id": c.site_id,
            "asset_id": c.asset_id,
            "connector_id": int(c.connector_id),
            "location_name": c.location_name,
            "city": c.city,
            "state": c.state,
            "region": c.region,
            "grid_zone": c.grid_zone,
            "latitude": float(c.latitude),
            "longitude": float(c.longitude),
            "charger_kind": c.charger_kind,
            "firmware_version": c.firmware_version,
            "installed_at_ms": int(installed_at_ms),
        })
    return out


def main() -> None:
    if not SCHEMAS_DIR.exists():
        raise FileNotFoundError(f"Schema directory not found: {SCHEMAS_DIR}")

    for st in SCHEMAS.keys():
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

    world = build_world()

    per_type_counts: Dict[str, int] = {st: 0 for st in SENSOR_TYPES}
    total_count = 0
    file_seq = 0
    tick_idx = 0

    buffers: Dict[str, List[Dict[str, Any]]] = {st: [] for st in SENSOR_TYPES}

    print(f"✅ Connected to MinIO: endpoint={MINIO_ENDPOINT} secure={MINIO_SECURE}")
    print(f"✅ Bucket: {MINIO_BUCKET}")
    print(f"✅ Dataset prefix: {DATASET_PREFIX}")
    print(f"✅ Schemas: {SCHEMAS_DIR}")
    print(f"✅ Data dir: {DATA_DIR}")
    print(f"✅ World: chargers={CHARGER_COUNT} connectors/charger={CONNECTORS_PER_CHARGER} total_connectors={len(world.connectors)}")
    print(f"✅ Tick: every {TICK_SECONDS}s | batch_size_per_tick={BATCH_SIZE_PER_TICK}")
    print(f"✅ Flush: every {FLUSH_EVERY_N_TICKS} ticks (or any buffer >= {MAX_BUFFER_RECORDS} records)")
    print(f"✅ Streams (telemetry): {SENSOR_TYPES}")
    print("✅ Dimension stream: device_metadata (written once at startup)")

    start_ts = datetime.now(timezone.utc)
    meta_records = build_device_metadata_records(world, start_ts)
    file_seq = write_avro_and_upload(client, "device_metadata", meta_records, start_ts, file_seq)
    print(f"✅ Wrote device_metadata records={len(meta_records)}")

    def should_flush() -> bool:
        if FLUSH_EVERY_N_TICKS > 0 and tick_idx > 0 and (tick_idx % FLUSH_EVERY_N_TICKS == 0):
            return True
        if any(len(buffers[st]) >= MAX_BUFFER_RECORDS for st in SENSOR_TYPES):
            return True
        return False

    while True:
        tick_time = datetime.now(timezone.utc)
        dt_hours = max(1, TICK_SECONDS) / 3600.0

        # Fault updates
        for conn in world.connectors:
            conn.uptime_s += max(1, TICK_SECONDS)

            fs = world.faults.get(conn.device_id)
            if fs and fs.ticks_left > 0:
                fs.ticks_left -= 1
                if fs.ticks_left <= 0:
                    world.faults.pop(conn.device_id, None)
                    if conn.fault_active:
                        conn.fault_active = False
                        conn.fault_code = None
                        conn.state_machine = "IDLE"
                        conn.available = True
                        conn.session_id = None
                        conn._was_derated = False
            else:
                if random.random() < FAULT_PROB_PER_TICK:
                    world.faults[conn.device_id] = FaultState(ticks_left=random.randint(FAULT_TICKS_MIN, FAULT_TICKS_MAX))
                    conn.fault_active = True
                    conn.fault_code = pick_fault_code()
                    conn.state_machine = "FAULTED"
                    conn.available = False

        # Generate into buffers
        for _ in range(BATCH_SIZE_PER_TICK):
            for conn in world.connectors:
                in_fault = conn.device_id in world.faults

                # Events
                if conn.fault_active and in_fault:
                    if random.random() < 0.15:
                        evt = fault_transition_event(conn, to_faulted=True)
                        evt["timestamp"] = now_ms(tick_time)
                        if evt["event_type"] == "FAULT":
                            evt["meter_start_kwh"] = float(round(conn.energy_kwh_total, 6))
                        buffers["evse_session_event"].append(evt)
                    conn.session_id = None
                    conn.state_machine = "FAULTED"
                    conn.available = False
                else:
                    if (not conn.fault_active) and (random.random() < 0.03):
                        evt = fault_transition_event(conn, to_faulted=False)
                        evt["timestamp"] = now_ms(tick_time)
                        buffers["evse_session_event"].append(evt)

                    evt = maybe_start_session(conn)
                    if evt is not None:
                        evt["timestamp"] = now_ms(tick_time)
                        if evt["event_type"] == "CHARGE_START":
                            evt["meter_start_kwh"] = float(round(conn.energy_kwh_total, 6))
                        buffers["evse_session_event"].append(evt)

                    evt2 = maybe_stop_session(conn)
                    if evt2 is not None:
                        evt2["timestamp"] = now_ms(tick_time)
                        evt2["meter_end_kwh"] = float(round(conn.energy_kwh_total, 6))
                        buffers["evse_session_event"].append(evt2)

                est_power_kw = estimate_target_power_kw(conn) if (conn.state_machine == "CHARGING" and not conn.fault_active) else 0.0

                temp_rec = gen_temperature(conn, tick_time, in_fault, est_power_kw)
                cabinet_temp_c = float(temp_rec["value_celsius"])

                conn.derate_pct = compute_derate_pct(cabinet_temp_c) if (conn.state_machine == "CHARGING" and not conn.fault_active) else None

                if conn.derate_pct is not None and conn.derate_pct >= 1.0 and not conn._was_derated:
                    evt = derate_event(conn)
                    evt["timestamp"] = now_ms(tick_time)
                    evt["meter_start_kwh"] = float(round(conn.energy_kwh_total, 6))
                    buffers["evse_session_event"].append(evt)
                    conn._was_derated = True
                if conn.derate_pct is None:
                    conn._was_derated = False

                buffers["temperature"].append(temp_rec)
                buffers["humidity"].append(gen_humidity(conn, tick_time, in_fault))
                buffers["vibration"].append(gen_vibration(conn, tick_time, in_fault, est_power_kw))
                buffers["evse_state"].append(gen_evse_state(conn, tick_time, cabinet_temp_c))
                buffers["evse_electrical"].append(gen_evse_electrical(conn, tick_time, cabinet_temp_c, dt_hours))

        tick_idx += 1

        if should_flush():
            flushed_counts: Dict[str, int] = {}
            for sensor_type, records in list(buffers.items()):
                if not records:
                    continue
                file_seq = write_avro_and_upload(client, sensor_type, records, tick_time, file_seq)
                per_type_counts[sensor_type] += len(records)
                total_count += len(records)
                flushed_counts[sensor_type] = len(records)
                buffers[sensor_type] = []

            print(f"✅ FLUSH tick={tick_idx} time={tick_time.isoformat()} uploaded={flushed_counts} faults_active={len(world.faults)}")
        else:
            if tick_idx % max(1, FLUSH_EVERY_N_TICKS // 5) == 0:
                approx_buf = {k: len(v) for k, v in buffers.items() if len(v) > 0}
                print(f"… buffered tick={tick_idx} (no flush yet) buffers={approx_buf} faults_active={len(world.faults)}")

        if should_stop(total_count, per_type_counts):
            for st, recs in list(buffers.items()):
                if recs:
                    file_seq = write_avro_and_upload(client, st, recs, tick_time, file_seq)
                    per_type_counts[st] += len(recs)
                    total_count += len(recs)
                    buffers[st] = []

            print("✅ Stop condition met.")
            print(f"   Total telemetry records: {total_count}")
            print(f"   Per-type counts: {per_type_counts}")
            break

        time.sleep(max(1, TICK_SECONDS))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n🛑 Stopped by user.")
