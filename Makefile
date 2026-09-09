# EV Telemetry Lakehouse - Makefile
# Useful commands for managing the lakehouse platform

.PHONY: up down restart status logs clean prune help trino superset minio check reset build rebuild data dbt setup

# Start all services
up:
	@echo "Starting services..."
	docker compose -f docker-compose.phase3.yml up -d

# Stop all services
down:
	@echo "Stopping services..."
	docker compose -f docker-compose.phase3.yml down

# Restart services
restart: down up
	@echo "Services restarted."

# Show service status
status:
	@echo "Service status:"
	docker compose -f docker-compose.phase3.yml ps

# View logs
logs:
	@echo "Viewing logs (Ctrl+C to exit)..."
	docker compose -f docker-compose.phase3.yml logs -f

# Clean up volumes (data loss!)
clean:
	@echo "Cleaning up volumes (data will be lost)..."
	docker compose -f docker-compose.phase3.yml down -v

# Prune all Docker resources
prune:
	@echo "Pruning Docker resources..."
	docker system prune -a -f
	docker volume prune -f

# Help
help:
	@echo "EV Telemetry Lakehouse - Available commands:"
	@echo ""
	@echo "  make up          - Start all services (MinIO, Trino, Superset, etc.)"
	@echo "  make down        - Stop all services"
	@echo "  make restart     - Restart all services"
	@echo "  make status      - Show service status"
	@echo "  make logs        - View logs (Ctrl+C to exit)"
	@echo "  make clean       - Remove volumes (data loss!)"
	@echo "  make prune       - Prune all Docker resources"
	@echo "  make setup       - Setup Iceberg schema and tables"
	@echo "  make data        - Generate EV telemetry data"
	@echo "  make dbt         - Run dbt transformations"
	@echo "  make trino       - Open Trino CLI"
	@echo "  make superset    - Open Superset"
	@echo "  make minio       - Open MinIO console"
	@echo "  make check       - Check if services are healthy"
	@echo "  make reset       - Reset everything (down -v && up)"
	@echo ""

# Setup Iceberg schema and tables
setup:
	@echo "Setting up Iceberg schema and tables..."
	@docker exec trino trino --execute "CREATE SCHEMA IF NOT EXISTS iceberg.curated;"
	@docker exec trino trino --execute "CREATE TABLE IF NOT EXISTS iceberg.curated.evse_electrical (event_ts TIMESTAMP(6), device_id VARCHAR, charger_id VARCHAR, connector_id VARCHAR, power_kw DOUBLE, voltage_v DOUBLE, current_a DOUBLE, energy_kwh_total DOUBLE, state_of_charge DOUBLE, temperature_c DOUBLE, status VARCHAR, session_id VARCHAR, evse_status VARCHAR, day DATE, hour SMALLINT);"
	@docker exec trino trino --execute "CREATE TABLE IF NOT EXISTS iceberg.curated.evse_state (event_ts TIMESTAMP(6), device_id VARCHAR, charger_id VARCHAR, connector_id VARCHAR, state VARCHAR, status VARCHAR, session_id VARCHAR, day DATE, hour SMALLINT);"
	@docker exec trino trino --execute "CREATE TABLE IF NOT EXISTS iceberg.curated.evse_session_event (event_ts TIMESTAMP(6), device_id VARCHAR, charger_id VARCHAR, connector_id VARCHAR, event_type VARCHAR, session_id VARCHAR, energy_kwh DOUBLE, duration_minutes DOUBLE, power_kw DOUBLE, day DATE, hour SMALLINT);"
	@docker exec trino trino --execute "CREATE TABLE IF NOT EXISTS iceberg.curated.humidity (event_ts TIMESTAMP(6), device_id VARCHAR, humidity DOUBLE, temperature_c DOUBLE, battery_level DOUBLE, signal_strength DOUBLE, firmware_version VARCHAR, day DATE, hour SMALLINT);"
	@docker exec trino trino --execute "CREATE TABLE IF NOT EXISTS iceberg.curated.vibration (event_ts TIMESTAMP(6), device_id VARCHAR, x_axis DOUBLE, y_axis DOUBLE, z_axis DOUBLE, frequency_hz DOUBLE, amplitude_g DOUBLE, battery_level DOUBLE, firmware_version VARCHAR, day DATE, hour SMALLINT);"
	@docker exec trino trino --execute "CREATE TABLE IF NOT EXISTS iceberg.curated.temperature (event_ts TIMESTAMP(6), device_id VARCHAR, temperature_c DOUBLE, humidity DOUBLE, battery_level DOUBLE, signal_strength DOUBLE, firmware_version VARCHAR, day DATE, hour SMALLINT);"
	@echo "Iceberg tables ready:"
	@docker exec trino trino --execute "SHOW TABLES FROM iceberg.curated;"

# Generate EV telemetry data
data:
	@echo "Generating EV telemetry data..."
	python3 scripts/generate_telemetry_ev_multimodel.py

# Run dbt transformations
dbt:
	@echo "Running dbt transformations..."
	cd ev_dbt && source ../.venv/bin/activate && dbt run

# Open Trino CLI
trino:
	@echo "Connecting to Trino..."
	docker exec -it trino trino

# Open Superset (in browser)
superset:
	@echo "Opening Superset in browser..."
	@open http://localhost:8088

# Open MinIO console (in browser)
minio:
	@echo "Opening MinIO console in browser..."
	@open http://localhost:9001

# Check service health
check:
	@echo "Checking service health..."
	@docker compose -f docker-compose.phase3.yml ps
	@echo ""
	@echo "Testing MinIO:"
	@curl -s http://localhost:9000/minio/health/live | head -1 || echo "MinIO not reachable"
	@echo ""
	@echo "Testing Trino:"
	@docker exec trino trino --execute "SELECT 1 as test;" 2>&1 | grep -q "1" && echo "Trino OK" || echo "Trino not responding"
	@echo ""
	@echo "Testing Iceberg tables:"
	@docker exec trino trino --execute "SHOW TABLES FROM iceberg.curated;" 2>&1 | grep -q "evse_electrical" && echo "Iceberg tables OK" || echo "Iceberg tables not ready (run: make setup)"

# Reset everything
reset: clean up
	@echo "System reset complete."

# Build specific service
build-%:
	@echo "Building $*..."
	docker compose -f docker-compose.phase3.yml build $*

# Rebuild and restart service
rebuild-%: build-%
	@echo "Rebuilding and restarting $*..."
	docker compose -f docker-compose.phase3.yml up -d $*