-- DuckDB demo queries for the EV telemetry lakehouse
-- Run: duckdb < scripts/duckdb_demo.sql
-- (or duckdb --readonly scripts/duckdb_demo.sql from the repo root)

--------------------------------------------------------------------------------
-- 0. CONNECT TO MINIO (used by later sections)
--------------------------------------------------------------------------------

INSTALL httpfs;  LOAD httpfs;
INSTALL iceberg; LOAD iceberg;

-- MinIO S3 credentials + path-style access
SET s3_region            = 'us-east-1';
SET s3_endpoint          = 'localhost:9000';
SET s3_access_key_id     = 'minioadmin';
SET s3_secret_access_key = 'minioadmin';
SET s3_use_ssl           = false;
SET s3_url_style         = 'path';

-- allow reading Iceberg metadata from MinIO without a version-hint file
SET unsafe_enable_version_guessing = true;

--------------------------------------------------------------------------------
-- 1. LOCAL RAW PARQUET (same bytes as MinIO, no config needed)
--------------------------------------------------------------------------------

-- Peek at the raw temperature stream (epoch-ms timestamps)
SELECT device_id, timestamp, value_celsius, unit, status
FROM   'data/avro_incoming/ev_v1/temperature/**/*.parquet'
ORDER  BY timestamp DESC
LIMIT  10;

-- Per-minute temperature trend, last hour (raw stream)
SELECT date_trunc('minute', to_timestamp(timestamp / 1000.0)) AS ts,
       round(avg(value_celsius), 2)                           AS avg_temp_c,
       count(*)                                               AS n
FROM   'data/avro_incoming/ev_v1/temperature/**/*.parquet'
WHERE  to_timestamp(timestamp / 1000.0) >= now() - INTERVAL '1' HOUR
GROUP  BY 1
ORDER  BY 1 DESC;

-- Row counts per stream (explicit paths keep schemas distinct)
SELECT 'temperature'        AS stream, count(*) AS rows FROM 'data/avro_incoming/ev_v1/temperature/**/*.parquet'
UNION ALL SELECT 'humidity',            count(*)       FROM 'data/avro_incoming/ev_v1/humidity/**/*.parquet'
UNION ALL SELECT 'vibration',           count(*)       FROM 'data/avro_incoming/ev_v1/vibration/**/*.parquet'
UNION ALL SELECT 'evse_electrical',     count(*)       FROM 'data/avro_incoming/ev_v1/evse_electrical/**/*.parquet'
UNION ALL SELECT 'evse_state',          count(*)       FROM 'data/avro_incoming/ev_v1/evse_state/**/*.parquet'
UNION ALL SELECT 'evse_session_event',  count(*)       FROM 'data/avro_incoming/ev_v1/evse_session_event/**/*.parquet'
ORDER BY 2 DESC;

-- Faults (evse_state) with readable timestamps
SELECT to_timestamp(timestamp / 1000.0) AS ts,
       device_id, state, fault_code, derate_pct
FROM   'data/avro_incoming/ev_v1/evse_state/**/*.parquet'
WHERE  fault_active = true
ORDER  BY timestamp DESC
LIMIT  10;

-- Compare raw vs curated counts (same data, two representations)
WITH raw AS (
    SELECT 'raw (lake parquet)'  AS source, count(*) AS n
    FROM   'data/avro_incoming/ev_v1/temperature/**/*.parquet'
),
cur AS (
    SELECT 'curated (iceberg)' AS source, count(*) AS n
    FROM   iceberg_scan('s3://warehouse/curated/temperature')
)
SELECT * FROM raw
UNION ALL SELECT * FROM cur;

--------------------------------------------------------------------------------
-- 2. RAW FROM MINIO (S3 over HTTP)
--------------------------------------------------------------------------------

-- Live read straight from the lake (no local files involved)
SELECT count(*) AS raw_temperature_rows
FROM   read_parquet('s3://iot-telemetry/ev_v1/temperature/**/**.parquet');

-- Power statistics lazily over all EVSE electrical partitions
SELECT round(avg(voltage_v), 1) AS avg_volts,
       round(avg(current_a), 2) AS avg_amps,
       round(sum(power_kw), 2)  AS total_kw
FROM   read_parquet('s3://iot-telemetry/ev_v1/evse_electrical/**/**.parquet');

--------------------------------------------------------------------------------
-- 3. CURATED ICELBERG TABLES (managed table, real TIMESTAMP type)
--------------------------------------------------------------------------------

-- Read the curated temperature table exactly like the catalog would
SELECT count(*)                       AS rows,
       min(timestamp)                 AS first_ts,
       max(timestamp)                 AS last_ts,
       round(avg(value_celsius), 2)   AS avg_temp_c
FROM   iceberg_scan('s3://warehouse/curated/temperature');

-- Per-minute trend from the curated table (native timestamp column)
SELECT date_trunc('minute', timestamp) AS ts,
       round(avg(value_celsius), 2)    AS avg_temp_c,
       count(*)                        AS n
FROM   iceberg_scan('s3://warehouse/curated/temperature')
WHERE  timestamp >= now() - INTERVAL '1' HOUR
GROUP  BY 1
ORDER  BY 1 DESC;

-- Faults from the curated evse_state table
SELECT timestamp, device_id, state, fault_code, derate_pct, charger_temp_c
FROM   iceberg_scan('s3://warehouse/curated/evse_state')
WHERE  fault_active = true
ORDER  BY timestamp DESC
LIMIT  10;

-- Session events bucketed hourly
SELECT date_trunc('hour', timestamp) AS hour,
       event_type, severity, count(*) AS n
FROM   iceberg_scan('s3://warehouse/curated/evse_session_event')
GROUP  BY 1, 2, 3
ORDER  BY 1 DESC;