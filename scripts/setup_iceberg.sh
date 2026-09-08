#!/bin/bash
# Setup script to create Iceberg schema and tables in Trino

echo "Setting up Iceberg schema and tables..."

# Create curated schema
echo "Creating curated schema..."
docker exec trino trino --execute "CREATE SCHEMA IF NOT EXISTS iceberg.curated;"

# Create tables
echo "Creating evse_electrical table..."
docker exec trino trino --execute "CREATE TABLE IF NOT EXISTS iceberg.curated.evse_electrical (
    event_ts TIMESTAMP(6),
    device_id VARCHAR,
    charger_id VARCHAR,
    connector_id VARCHAR,
    power_kw DOUBLE,
    voltage_v DOUBLE,
    current_a DOUBLE,
    energy_kwh_total DOUBLE,
    state_of_charge DOUBLE,
    temperature_c DOUBLE,
    status VARCHAR,
    session_id VARCHAR,
    evse_status VARCHAR,
    day DATE,
    hour SMALLINT
);"

echo "Creating evse_state table..."
docker exec trino trino --execute "CREATE TABLE IF NOT EXISTS iceberg.curated.evse_state (
    event_ts TIMESTAMP(6),
    device_id VARCHAR,
    charger_id VARCHAR,
    connector_id VARCHAR,
    state VARCHAR,
    status VARCHAR,
    session_id VARCHAR,
    day DATE,
    hour SMALLINT
);"

echo "Creating evse_session_event table..."
docker exec trino trino --execute "CREATE TABLE IF NOT EXISTS iceberg.curated.evse_session_event (
    event_ts TIMESTAMP(6),
    device_id VARCHAR,
    charger_id VARCHAR,
    connector_id VARCHAR,
    event_type VARCHAR,
    session_id VARCHAR,
    energy_kwh DOUBLE,
    duration_minutes DOUBLE,
    power_kw DOUBLE,
    day DATE,
    hour SMALLINT
);"

echo "Creating humidity table..."
docker exec trino trino --execute "CREATE TABLE IF NOT EXISTS iceberg.curated.humidity (
    event_ts TIMESTAMP(6),
    device_id VARCHAR,
    humidity DOUBLE,
    temperature_c DOUBLE,
    battery_level DOUBLE,
    signal_strength DOUBLE,
    firmware_version VARCHAR,
    day DATE,
    hour SMALLINT
);"

echo "Creating vibration table..."
docker exec trino trino --execute "CREATE TABLE IF NOT EXISTS iceberg.curated.vibration (
    event_ts TIMESTAMP(6),
    device_id VARCHAR,
    x_axis DOUBLE,
    y_axis DOUBLE,
    z_axis DOUBLE,
    frequency_hz DOUBLE,
    amplitude_g DOUBLE,
    battery_level DOUBLE,
    firmware_version VARCHAR,
    day DATE,
    hour SMALLINT
);"

echo "Creating temperature table..."
docker exec trino trino --execute "CREATE TABLE IF NOT EXISTS iceberg.curated.temperature (
    event_ts TIMESTAMP(6),
    device_id VARCHAR,
    temperature_c DOUBLE,
    humidity DOUBLE,
    battery_level DOUBLE,
    signal_strength DOUBLE,
    firmware_version VARCHAR,
    day DATE,
    hour SMALLINT
);"

echo "Verifying tables..."
docker exec trino trino --execute "SHOW TABLES FROM iceberg.curated;"

echo "Done! All Iceberg tables are ready."