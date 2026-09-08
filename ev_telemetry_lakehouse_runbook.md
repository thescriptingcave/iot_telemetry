# EV Telemetry Lakehouse Runbook

## Purpose
This runbook provides a single reference for operating and troubleshooting the local EV telemetry data platform. It covers MinIO storage, Trino query engine, Hive raw ingestion, Iceberg curated tables, Docker orchestration, and dbt transformations.

Use this document as a daily operational guide and recovery reference.

---

# System Overview

## Architecture Flow

Avro Generator → MinIO (S3) → Hive External Tables → Iceberg Tables → dbt Models → Analytics

## Core Components

- MinIO: Object storage (Avro + Parquet)
- Hive Metastore: Table metadata store
- Trino: Query engine
- Iceberg: Table format for curated data
- dbt: Transformations + modeling
- Docker Compose: Orchestration

---

# Section 1 — MinIO Operations (mc)

## One-Time Setup
Create local alias:

mc alias set local http://127.0.0.1:9000 admin password

Verify alias:

mc alias list

## Inspect Storage
List buckets:

mc ls local

List project bucket:

mc ls local/iot-telemetry/

List generated telemetry data:

mc ls local/iot-telemetry/ev_v1/

Preview files:

mc ls -r local/iot-telemetry/ev_v1/temperature/ | head

## Schema Management
Create schemas folder:

mc mb local/iot-telemetry/schemas 2>/dev/null || true

Upload all schemas:

mc cp --recursive schemas/ local/iot-telemetry/schemas/

Verify upload:

mc ls local/iot-telemetry/schemas/

Inspect a schema file:

mc cat local/iot-telemetry/schemas/temperature.avsc | head

---

# Section 2 — Docker Operations

## Stack Control
Start entire stack:

docker compose up -d

Stop stack:

docker compose down

Restart stack:

docker compose restart

Rebuild containers:

docker compose up -d --build

## Monitoring
List containers:

docker ps

View logs:

docker logs -f trino

View last 200 lines:

docker logs --tail 200 trino

Shell into container:

docker exec -it trino sh

View resource usage:

docker stats

---

# Section 3 — Trino Operations

## Connect to CLI

docker exec -it trino trino

## System Inspection

SHOW CATALOGS;

SHOW SCHEMAS FROM hive;

SHOW SCHEMAS FROM iceberg;

SHOW TABLES FROM hive.raw;

SHOW TABLES FROM iceberg.curated;

---

# Section 4 — Hive RAW Layer (Avro External Tables)

## Create Raw Schema

CREATE SCHEMA IF NOT EXISTS hive.raw;

## Register Datasets

Example (Temperature):

CREATE TABLE hive.raw.temperature
WITH (
  external_location='s3://iot-telemetry/ev_v1/temperature/',
  format='AVRO',
  avro_schema_url='s3://iot-telemetry/schemas/temperature.avsc'
);

Repeat pattern for:
- humidity
- vibration
- evse_electrical
- evse_state
- evse_session_event
- device_metadata

## Validate

SELECT count(*) FROM hive.raw.temperature;
SELECT * FROM hive.raw.temperature LIMIT 5;

---

# Section 5 — Iceberg Curated Layer

## Create Schema

CREATE SCHEMA IF NOT EXISTS iceberg.curated;

## Convert Raw → Iceberg

CREATE TABLE iceberg.curated.temperature
WITH (format='PARQUET')
AS SELECT * FROM hive.raw.temperature;

Repeat for each dataset.

## Validate

SHOW TABLES FROM iceberg.curated;

SELECT count(*) FROM iceberg.curated.temperature;

---

# Section 6 — dbt Operations

## Environment

Activate venv:

source .venv-dbt/bin/activate

Check installation:

dbt --version

## Project Lifecycle

Validate connection:

dbt debug

Compile SQL:

dbt compile

Run models:

dbt run

Run single model:

dbt run -s stg_temperature

Run tests:

dbt test

Generate docs:

dbt docs generate

Serve docs:

dbt docs serve --port 8088

---

# Section 7 — Daily Startup Checklist

1) Start stack
2) Confirm containers running
3) Confirm MinIO accessible
4) Confirm Trino catalogs
5) Confirm Iceberg tables visible

Commands:

docker compose up -d

docker compose ps

mc ls local/iot-telemetry/

SHOW CATALOGS;

SHOW TABLES FROM iceberg.curated;

---

# Section 8 — Troubleshooting Guide

## Trino Not Reachable

Check container:

docker ps | grep trino

Restart:

docker restart trino

## Hive Tables Return Zero Rows

Likely missing recursive read.
Ensure hive.properties contains:

hive.recursive-directories=true

Restart Trino.

## Schemas Missing

mc ls local/iot-telemetry/schemas/

Re-upload if needed.

## Iceberg Tables Not Appearing

SHOW SCHEMAS FROM iceberg;

Check metastore container.

## dbt Connection Errors

Run:

dbt debug

Verify host, port, catalog, schema in profiles.yml

---

# Section 9 — Operational Strategy

RAW Layer:
- Immutable Avro ingestion

CURATED Layer:
- Iceberg optimized for analytics

DBT Layer:
- Staging → Marts → Business models

This design mirrors production-grade lakehouse architecture.

