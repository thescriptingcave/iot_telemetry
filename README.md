# EV Telemetry Lakehouse - Quick Start Guide

This guide gets the EV Telemetry Lakehouse platform up and running end-to-end:
telemetry generation → MinIO raw files → Iceberg curated tables → Trino → dbt → Superset.

## 🏗️ Architecture

```
Generator ─▶ MinIO (S3) ──raw────▶ s3://iot-telemetry/ev_v1/{stream}/...
     │
     └──▶ PyIceberg ─▶ iceberg-rest (s3://warehouse/curated) ─▶ Trino ─▶ dbt ─▶ Superset
```

**Components:**
- **MinIO** - S3-compatible object storage (port 9000)
- **iceberg-rest** - Iceberg REST catalog (port 8181, warehouse `s3://warehouse/`)
- **Trino** - Query engine (port 8080)
- **PyIceberg sink** - part of the generator; appends every batch to `iceberg.curated.*` tables
- **dbt** - data transformation tool
- **Superset** - BI/visualization (port 8088)

## 📦 Prerequisites

- Docker Desktop installed
- Docker Compose v2+
- Python 3.13 (managed via `uv`)
- ~4GB RAM free

```bash
# Install uv (once)
curl -LsSf https://astral.sh/uv/install.sh | sh
```

## 🚀 Quick Start

### Step 1: Sort environment + Python deps

```bash
cd /Users/dev/Documents/iot_telemetry

cp .env.example .env   # then edit .env (MinIO creds etc.)

uv venv --python 3.13
uv pip install --python .venv/bin/python minio fastavro pyarrow trino python-dotenv "pyiceberg[pyiceberg-core]"
```

> `pyiceberg-core` is required — the `day(timestamp)` partition transform is Rust-based (missing it → `NotInstalledError`).

### dbt setup (optional)

Note: Use Python 3.13 for dbt (Python 3.14 has compatibility issues with protobuf):

```bash
cd ev_dbt
uv venv --python 3.13
source .venv/bin/activate
pip install dbt-core dbt-trino
dbt debug
```

### Step 2: Start services

```bash
docker compose -f docker-compose.phase3.yml up -d
docker compose -f docker-compose.phase3.yml ps
```

Healthy: `minio`, `trino`, `superset`, `iceberg-rest`.

### Step 3: Generate telemetry data

```bash
.venv/bin/python scripts/generate_telemetry_ev_multimodel.py
```

On first run this:
- connects to MinIO and creates the raw bucket layout
- creates the 6 Iceberg curated tables (`iceberg.curated.*`) if missing
- on each flush appends rows to both MinIO raw Parquet **and** Iceberg

This runs until Ctrl-C (final flush runs before exit). "Get up and running" quicker test:

```bash
ICEBERG_SINK_ENABLED=true TICK_SECONDS=1 FLUSH_EVERY_N_TICKS=2 \
CHARGER_COUNT=2 CONNECTORS_PER_CHARGER=2 MAX_RECORDS=200 \
.venv/bin/python scripts/generate_telemetry_ev_multimodel.py
```

### Step 4: Query with Trino

```bash
docker exec -it trino trino
```

```sql
SHOW CATALOGS;
SHOW TABLES FROM iceberg.curated;
SELECT count(*) FROM iceberg.curated.temperature;

-- per-minute temperature trend, last hour
SELECT date_trunc('minute', timestamp) AS ts,
       round(avg(value_celsius), 2) AS avg_temp_c,
       count(*)                       AS n
FROM iceberg.curated.temperature
WHERE timestamp >= now() - INTERVAL '1' HOUR
GROUP BY 1 ORDER BY 1 DESC;
```

Full query examples (daily aggregates, humidity, EVSE electrical, session events) are in `ev_telemetry_lakehouse_runbook.md`.

### Step 5: dbt (optional)

```bash
source .venv/bin/activate
cd ev_dbt
dbt debug      # needs ev_dbt/profiles.yml -> trino://<user>@localhost:8080/iceberg, schema=curated
dbt run
dbt test
```

### Step 6: Access Superset

- **URL:** http://localhost:8088
- **Login:** `admin` / `admin`

**Add the Trino datasource:** Data → Databases → Add database:
```
trino://user@localhost:8080/iceberg/curated
```

## 🔧 Configuration

### Environment (`.env`)

```env
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
# Trino / Superset / database creds per .env.example
```

These MinIO creds must match everywhere: `.env`, MinIO, iceberg-rest, and the Trino catalog configs.

### Generator knobs (env vars)

| Var | Default |
|---|---|
| `ICEBERG_SINK_ENABLED` | `true` |
| `ICEBERG_REST_URI` | `http://localhost:8181` |
| `ICEBERG_WAREHOUSE` | `s3://warehouse/` |
| `ICEBERG_NAMESPACE` | `curated` |
| `TICK_SECONDS` | `2` |
| `FLUSH_EVERY_N_TICKS` | `300` |
| `MAX_RECORDS` / `MAX_RECORDS_PER_TYPE` | `0` (unlimited) |

## 🐛 Troubleshooting

```bash
docker compose -f docker-compose.phase3.yml logs trino
curl http://localhost:9000/minio/health/live
docker exec trino trino --execute "SHOW SCHEMAS FROM iceberg;"

# missing pyiceberg-core (NotInstalledError)
uv pip install --python .venv/bin/python "pyiceberg[pyiceberg-core]"

# reset curated tables (stop generator first)
docker exec trino trino --execute "DROP SCHEMA IF EXISTS iceberg.curated CASCADE;"
bash scripts/setup_iceberg.sh   # namespace only; generator recreates tables
```

## 📝 Common Commands

```bash
docker compose -f docker-compose.phase3.yml up -d
docker compose -f docker-compose.phase3.yml down
docker compose -f docker-compose.phase3.yml logs -f trino
.venv/bin/python scripts/generate_telemetry_ev_multimodel.py
docker exec minio mc ls -r local/warehouse/curated/ | head
```

## 💡 Iceberg maintenance

```bash
docker exec trino trino --execute "ALTER TABLE iceberg.curated.temperature EXECUTE OPTIMIZE;"
docker exec trino trino --execute "CALL iceberg.system.expire_snapshots('curated', 'evse_electrical', timestamp => now() - INTERVAL '7' DAY);"
```

## 📚 Documentation

- `ev_telemetry_lakehouse_runbook.md` - operational guide, time-series queries, troubleshooting
- `HIGH_PRIORITY_CHANGES.md` - optimization details
- `TEST_RESULTS.md` - test results