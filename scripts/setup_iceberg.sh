#!/bin/bash
# Setup script for the Iceberg REST catalog.
#
# As of the "Option A" pipeline, curated tables are created by the generator
# itself (PyIceberg sink in generate_telemetry_ev_multimodel.py) with a schema
# that mirrors the generated records, partitioned by day(timestamp).
# This script only (re)creates the curated namespace.
#
# To reset curated tables to a clean state while the generator is stopped:
#   docker exec trino trino --execute "DROP SCHEMA IF EXISTS iceberg.curated CASCADE;"
# then re-run this script (or just let the generator recreate the namespace).

echo "Setting up Iceberg schema..."
docker exec trino trino --execute "CREATE SCHEMA IF NOT EXISTS iceberg.curated;"

echo "Verifying namespace..."
docker exec trino trino --execute "SHOW SCHEMAS FROM iceberg;"

echo "Done! Curated tables will be created by the generator on first run."