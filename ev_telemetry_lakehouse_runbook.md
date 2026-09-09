# EV Telemetry Lakehouse Runbook

## Purpose
Single reference for operating and troubleshooting the local EV telemetry data platform: MinIO (S3) storage, Iceberg REST catalog + curated tables, Trino query engine, the telemetry generator (with direct PyIceberg sink), dbt, and Superset.

> **Pipeline (Option A):** the generator writes Parquet (ZSTD) raw files to MinIO **and** appends the same batches straight into Iceberg curated tables via PyIceberg → REST catalog. Hive is no longer part of the pipeline; the `hive` catalog / metastore in the stack is legacy and can be ignored.

---

# System Overview

## Architecture Flow

```
Generator ──▶ MinIO (S3) ──raw──▶ s3://iot-telemetry/ev_v1/{stream}/year=../
    │
    └────────▶ Iceberg REST (s3://warehouse/curated/{stream}) ──▶ Trino ──▶ dbt ──▶ Superset
```

## Core Components

- **MinIO**: object storage (raw Parquet files + Iceberg warehouse). Ports 9000/9001.
- **iceberg-rest**: REST catalog (port 8181) with warehouse `s3://warehouse/`, backed by an **in-memory SQLite DB** (see note below).
- **Trino**: query engine (port 8080), catalogs: `iceberg`, `hive` (legacy), `system`.
- **PyIceberg sink**: inside the generator, appends rows to `iceberg.curated.<stream>` tables partitioned by `day(timestamp)`.
- **dbt**: transformations in `ev_dbt/`.
- **Superset**: BI/visualization (port 8088).

## Credentials

All come from `.env` — see `.env.example` for the template.

| Thing | Value |
|---|---|
| MinIO user | `MINIO_ROOT_USER=minioadmin` |
| MinIO password | `MINIO_ROOT_PASSWORD=change-this-password-in-production` |
| Superset | `admin` / `admin` (auto-created on first boot; override via `SUPERSET_ADMIN_USERNAME` / `SUPERSET_ADMIN_PASSWORD`) |

Superset metadata lives in Postgres (`superset-db`), not filesystem SQLite — wired via `superset/superset_config.py` mounted into `/app/pythonpath/`.

> **⚠️ Catalog is in-memory:** the REST catalog's SQLite DB is `mode=memory`, so **restarting** `iceberg-rest` (or `docker compose down`) wipes table registration even though the Iceberg metadata/files remain in S3. After any restart, `SHOW TABLES FROM iceberg.curated` may be empty — follow the **Reset / recreate** procedure in Section 5 (drop schema + rerun generator). If you rotate MinIO creds, they must also match in `iceberg-rest` env and `trino/etc/catalog/*.properties` (these are currently hardcoded).

---

# Section 1 — MinIO Operations

`mc` is not installed on the host; use the copy inside the MinIO container.

```bash
docker exec minio mc alias set local http://localhost:9000 minioadmin "change-this-password-in-production"
```

Inspect storage:

```bash
docker exec minio mc ls local/                     # buckets: iot-telemetry, warehouse
docker exec minio mc ls local/iot-telemetry/ev_v1/
docker exec minio mc ls -r local/iot-telemetry/ev_v1/temperature/ | head
docker exec minio mc ls -r local/warehouse/curated/temperature/ | head
```

Layout:

- Raw: `s3://iot-telemetry/ev_v1/{stream}/{year=...}/{month=...}/{day=...}/{hour=...}/...parquet`
- Iceberg tables: `s3://warehouse/curated/{stream}/{data|metadata}/...`
- Iceberg data files are physically partitioned `timestamp_day=YYYY-MM-DD`.

---

# Section 2 — Docker Operations

This repo only has `docker-compose.phase3.yml`; always pass `-f`:

```bash
docker compose -f docker-compose.phase3.yml up -d      # start
docker compose -f docker-compose.phase3.yml ps         # status
docker compose -f docker-compose.phase3.yml down       # stop
docker compose -f docker-compose.phase3.yml restart trino
docker compose -f docker-compose.phase3.yml up -d --build   # rebuild (e.g. hive-metastore config mounts are bind mounts; rarely needed)
```

Key ports: minio 9000/9001 · trino 8080 · iceberg-rest 8181 · superset 8088 · hive-metastore 9083 (legacy).

Logs / shell:

```bash
docker logs -f trino
docker exec -it trino trino      # Trino CLI
docker compose -f docker-compose.phase3.yml stats
```

---

# Section 3 — Trino Operations

```bash
docker exec -it trino trino
```

Inspection:

```sql
SHOW CATALOGS;                       -- iceberg, hive (legacy), system
SHOW SCHEMAS FROM iceberg;           -- curated
SHOW TABLES FROM iceberg.curated;    -- the 6 curated streams
DESCRIBE iceberg.curated.temperature;
```

One-liners:

```bash
docker exec trino trino --execute "SELECT count(*) FROM iceberg.curated.temperature;"
```

---

# Section 4 — Data Generation (Generator + Iceberg Sink)

## Python environment

The repo uses a uv-managed venv at `.venv`.

```bash
cd /Users/dev/Documents/iot_telemetry

# install dependencies once
uv pip install --python .venv/bin/python minio fastavro pyarrow trino python-dotenv "pyiceberg[pyiceberg-core]"
```

> The `pyiceberg-core` extra is required — the `day(timestamp)` partition transform is implemented in Rust (`pyiceberg_core`). Without it you get `NotInstalledError: pyiceberg_core needs to be installed`.

## Run the generator

```bash
# loads .env for MinIO creds; connects to MinIO + Iceberg REST
.venv/bin/python scripts/generate_telemetry_ev_multimodel.py
```

On startup it will print:

```
✅ Created Iceberg table curated.temperature      (…6 tables)
✅ Iceberg curated sink connected: REST=http://localhost:8181 namespace=curated
```

Behaviors:

- Runs until Ctrl-C; at stop it does a **final flush** of remaining buffers into both MinIO and Iceberg.
- Buffers flush to MinIO + Iceberg every `FLUSH_EVERY_N_TICKS` ticks (default 300 ticks → every ~10 min at `TICK_SECONDS=2`). To see data quickly during a test, lower it (e.g. `FLUSH_EVERY_N_TICKS=10`).

## Useful knobs (env vars)

| Var | Default | Meaning |
|---|---|---|
| `ICEBERG_SINK_ENABLED` | `true` | master switch for the Iceberg sink |
| `ICEBERG_REST_URI` | `http://localhost:8181` | REST catalog endpoint (host → container) |
| `ICEBERG_WAREHOUSE` | `s3://warehouse/` | Iceberg warehouse location |
| `ICEBERG_NAMESPACE` | `curated` | catalog namespace for curated tables |
| `TICK_SECONDS` | `2` | tick interval |
| `FLUSH_EVERY_N_TICKS` | `300` | flush cadence (ticks) |
| `MAX_BUFFER_RECORDS` | `50000` | force flush when any buffer hits this |
| `CHARGER_COUNT` | `8` | world size |
| `CONNECTORS_PER_CHARGER` | `2` | world size |
| `MAX_RECORDS` | `0` | stop when total across all streams reaches this (`0` = unlimited) |
| `MAX_RECORDS_PER_TYPE` | `0` | stop when each stream reaches this |
| `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` | via `.env` | MinIO creds (fallback `minioadmin`) |
| `MINIO_ENDPOINT` / `MINIO_BUCKET` | `localhost:9000` / `iot-telemetry` | MinIO target for the sink |

Small test run (stops itself, ~10s):

```bash
ICEBERG_SINK_ENABLED=true TICK_SECONDS=1 FLUSH_EVERY_N_TICKS=2 \
CHARGER_COUNT=2 CONNECTORS_PER_CHARGER=2 MAX_RECORDS=200 \
.venv/bin/python scripts/generate_telemetry_ev_multimodel.py
```

If the sink fails to initialize, the generator logs a warning and **continues MinIO-only** — curated tables will just sit empty.

---

# Section 5 — Iceberg Curated Layer

## How tables are created

The generator creates `iceberg.<namespace>.<stream>` on first run if missing (6 streams):

| Table | Notable columns |
|---|---|
| `curated.temperature` | device_id, timestamp, value_celsius, unit, accuracy_pct, status |
| `curated.humidity` | device_id, timestamp, relative_humidity_pct, dew_point_c, location |
| `curated.vibration` | device_id, timestamp, rms_acceleration, peak_frequency_hz, bandwidth_hz, sensor_model |
| `curated.evse_electrical` | site_id, asset_id, connector_id, device_id, timestamp, voltage_v, current_a, power_kw, energy_kwh_total, power_factor, grid_frequency_hz, phase, temperature_cabinet_c, derate_pct, status |
| `curated.evse_state` | site_id, asset_id, connector_id, device_id, timestamp, state, available, fault_active, fault_code, derate_pct, charger_temp_c, uptime_s, firmware_version |
| `curated.evse_session_event` | site_id, asset_id, connector_id, device_id, timestamp, session_id, event_type, reason_code, severity, meter_start_kwh, meter_end_kwh, energy_delivered_kwh, duration_s, user_id_hash |

- `timestamp` is a real `TIMESTAMP(6)` (epoch-ms converted to UTC); partition spec is `day(timestamp)`.
- All columns nullable.

## Reset / recreate (schema drift, wrong legacy schema, etc.)

Stop the generator first, then:

```bash
docker exec trino trino --execute "DROP SCHEMA IF EXISTS iceberg.curated CASCADE;"
bash scripts/setup_iceberg.sh      # recreates the namespace (tables are recreated by the generator on next run)
```

## Validate

```sql
SHOW TABLES FROM iceberg.curated;
SELECT count(*) FROM iceberg.curated.temperature;
SELECT min(timestamp), max(timestamp), count(*) FROM iceberg.curated.evse_electrical;
```

---

# Section 6 — Time-Series Queries

Schema note: query on `timestamp` (real timestamp); use `day(timestamp)` / `date_trunc` for bucketing. There is no separate `hour` column.

```sql
-- Temperature: per-minute trend for the last hour
SELECT date_trunc('minute', timestamp) AS ts,
       round(avg(value_celsius), 2) AS avg_temp_c,
       max(value_celsius)            AS max_temp_c,
       count(*)                      AS n
FROM iceberg.curated.temperature
WHERE timestamp >= now() - INTERVAL '1' HOUR
GROUP BY 1 ORDER BY 1 DESC;

-- Temperature: daily aggregates (prunes on timestamp_day partition)
SELECT CAST(timestamp AS DATE) AS day,
       round(min(value_celsius), 2), round(max(value_celsius), 2), round(avg(value_celsius), 2)
FROM iceberg.curated.temperature
WHERE timestamp >= current_date - INTERVAL '7' DAY
GROUP BY 1 ORDER BY 1 DESC;

-- Humidity
SELECT date_trunc('hour', timestamp) AS hour,
       round(avg(relative_humidity_pct), 1),
       round(avg(dew_point_c), 1)
FROM iceberg.curated.humidity
GROUP BY 1 ORDER BY 1 DESC LIMIT 24;

-- EVSE power/energy for the last 24h
SELECT CAST(timestamp AS DATE) AS day,
       count(DISTINCT connector_id) AS connectors,
       round(sum(power_kw), 2)       AS kwh_est,
       round(avg(voltage_v), 1)      AS avg_volts,
       round(avg(current_a), 2)      AS avg_amps
FROM iceberg.curated.evse_electrical
WHERE timestamp >= now() - INTERVAL '24' HOUR
GROUP BY 1 ORDER BY 1 DESC;

-- Faults / derated connectors (most recent activity)
SELECT timestamp, device_id, state, fault_code, derate_pct, charger_temp_c
FROM iceberg.curated.evse_state
WHERE fault_active = true
ORDER BY timestamp DESC LIMIT 100;

-- Session events bucketed by hour
SELECT date_trunc('hour', timestamp) AS hour, event_type, severity, count(*) AS n
FROM iceberg.curated.evse_session_event
WHERE timestamp >= current_date
GROUP BY 1, 2, 3 ORDER BY 1 DESC;
```

---

# Section 7 — Iceberg Maintenance

```sql
-- Full rewrite of a table's data files
ALTER TABLE iceberg.curated.temperature EXECUTE OPTIMIZE;

-- Rewrite recent data only (predicate on timestamp, the partition column)
ALTER TABLE iceberg.curated.temperature EXECUTE OPTIMIZE
WHERE timestamp >= now() - INTERVAL '7' DAY;

-- Expire snapshots / history
CALL iceberg.system.expire_snapshots('curated', 'evse_electrical', timestamp => now() - INTERVAL '7' DAY);

-- Snapshot history / time travel
SELECT * FROM iceberg.curated.temperature FOR VERSION AS OF <snapshot_id> LIMIT 10;
```

Schedule OPTIMIZE in off-peak hours (small test runs create many tiny files — OPTIMIZE is worth running after long sessions).

---

# Section 8 — dbt Operations

```bash
source .venv/bin/activate
dbt debug                # verify Trino connection
dbt compile
dbt run -s stg_temperature
dbt test
dbt docs serve --port 8088
```

> **`profiles.yml`** (`~/.dbt/profiles.yml`, not committed): `type: trino`, `method: none`, `host: localhost`, `port: 8080`, `user: admin`, `database: iceberg`, `schema: curated`. **Set `threads: 1`** — the Iceberg REST catalog is backed by a single SQLite file; parallel dbt threads cause `SQLITE_BUSY` commit failures (`ICEBERG_COMMIT_ERROR ... Service failed: 500`). If you hit this anyway, `docker compose -f docker-compose.phase3.yml restart iceberg-rest` clears stale catalog locks (table metadata persists in the volume).
>
> **Sources live in `ev_dbt/models/sources.yml`** — dbt only scans YAML under configured paths (`model-paths`), so a `sources.yml` at the project root is silently ignored (`Compilation Error: depends on a source named ... which was not found`).

---

# Section 9 — Daily Startup Checklist

```bash
docker compose -f docker-compose.phase3.yml up -d
docker compose -f docker-compose.phase3.yml ps                    # minio, iceberg-rest, trino, superset healthy

docker exec minio mc ls local/                                    # buckets present
docker exec trino trino --execute "SHOW TABLES FROM iceberg.curated;"

# generate (auto-creates tables if missing, appends on flush)
.venv/bin/python scripts/generate_telemetry_ev_multimodel.py

# sanity check
docker exec trino trino --execute "SELECT count(*) FROM iceberg.curated.temperature;"
```

---

# Section 10 — Troubleshooting

## Iceberg tables missing / `SHOW TABLES` empty

Sink didn't create them yet — either the generator hasn't run, sink init failed, or the catalog was restarted (in-memory DB reset):

```bash
# the sink confirms itself in the GENERATOR's terminal, not Trino logs
# (run the generator and look for "Iceberg curated sink connected")
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:8181/v1/config   # 200 = catalog alive
docker compose -f docker-compose.phase3.yml logs iceberg-rest | tail
# if the catalog was restarted, re-register the tables (Section 5 reset) and rerun the generator
```

## `NotInstalledError: pyiceberg_core needs to be installed`

```bash
uv pip install --python .venv/bin/python "pyiceberg[pyiceberg-core]"
```

## `SignatureDoesNotMatch` or access-denied writing to MinIO

Credentials mismatch. `.env` must set `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` to the same values MinIO runs with. These files also carry hardcoded copies — keep them in sync when rotating creds:
- `iceberg-rest` env in `docker-compose.phase3.yml` (`CATALOG_S3_ACCESS__KEY__ID` / `CATALOG_S3_SECRET__ACCESS__KEY`)
- `trino/etc/catalog/iceberg.properties` and `trino/etc/catalog/hive.properties` (`s3.aws-access-key` / `s3.aws-secret-key`)

## Existing curated table has an old/incorrect schema

Tables created before the Option A pivot (the old `event_ts/day/hour` layout) are empty and safe to drop:

```bash
docker exec trino trino --execute "DROP SCHEMA IF EXISTS iceberg.curated CASCADE;"
bash scripts/setup_iceberg.sh
# rerun the generator; it recreates the 6 tables with the data-driven schema
```

## Hive tables return zero rows or Hive errors

`hive` catalog is legacy and not part of the current pipeline — ignore it. (If ever needed again: `hive.recursive-directories=true` is already set, and creds in `trino/etc/catalog/hive.properties` were fixed to the MinIO root user.)

## Trino not reachable

```bash
docker ps | grep trino
docker restart trino
docker logs --tail 200 trino
```

## Superset can't find tables

Connect Trino database with uri `trino://<user>@localhost:8080/iceberg/curated` and refresh. Ensure the generator has created tables.

---

# Operational Strategy

- **RAW layer** (`s3://iot-telemetry/ev_v1/`): immutable Parquet (ZSTD) point-in-time snapshot of everything generated.
- **CURATED layer** (`iceberg.curated.*`): Iceberg tables mirroring the records, partitioned by `day(timestamp)` for cheap time-series pruning; written inline by the generator.
- **DBT layer**: staging → marts → business models (use the curated tables).
- **BI**: Superset over Trino (`localhost:8080`).