# EV Telemetry Lakehouse - Quick Start Guide

This guide will help you get the EV Telemetry Lakehouse platform up and running in under 10 minutes.

## 🏗️ Architecture

```
IoT Devices → MinIO (S3) → Hive (Avro) → Iceberg (Parquet) → Trino → dbt → Superset
```

**Components:**
- **MinIO** - S3-compatible object storage
- **Hive Metastore** - Table metadata
- **Trino** - Query engine (Iceberg + Hive connectors)
- **Iceberg** - Table format for optimized analytics
- **dbt** - Data transformation tool
- **Superset** - BI/visualization

## 📦 Prerequisites

- Docker Desktop installed
- Docker Compose v2+
- Python 3.8+
- ~4GB RAM free

### Install Python Dependencies

```bash
pip install minio fastavro pyarrow trino dbt-core dbt-postgres
```

## 🚀 Quick Start

### Prerequisites

- Docker Desktop installed
- Docker Compose v2+
- Python 3.8+
- ~4GB RAM free

### Install `uv` Package Manager

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### Install Python Dependencies with `uv`

```bash
# Create and activate virtual environment
uv venv

# Install dependencies
uv pip install minio fastavro pyarrow trino dbt-core dbt-postgres
```

### Step 1: Clone and Start Services

```bash
# Clone the repository (or navigate to project directory)
cd /Users/dev/Documents/iot-telemetry

# Start all services
docker compose -f docker-compose.phase3.yml up -d
```

### Step 2: Verify Services Are Running

```bash
docker compose -f docker-compose.phase3.yml ps
```

All services should show **healthy** status:
- ✅ minio (port 9000)
- ✅ trino (port 8080)
- ✅ superset (port 8088)
- ✅ hive-metastore (port 9083)

### Step 3: Generate Telemetry Data

```bash
# Generate EV telemetry with Parquet output
python3 scripts/generate_telemetry_ev_multimodel.py
```

This will:
- Connect to MinIO
- Create partitioned Parquet files (ZSTD compressed)
- Generate ~1000+ records

### Step 4: Query with Trino CLI

```bash
# Connect to Trino
docker exec -it trino trino --catalog iceberg

# Check catalogs
SHOW CATALOGS;

# List tables
SHOW SCHEMAS FROM iceberg;
SHOW TABLES FROM iceberg.curated;

# Run a query
SELECT day, AVG(power_kw), SUM(energy_kwh_total) 
FROM iceberg.curated.evse_electrical 
GROUP BY day 
ORDER BY day DESC 
LIMIT 10;
```

### Step 5: Load Data into Iceberg

```bash
# Create curated tables from raw Avro data
docker exec -it trino trino --catalog hive

CREATE SCHEMA IF NOT EXISTS iceberg.curated;

-- Convert raw data to Iceberg with optimization
CREATE TABLE iceberg.curated.temperature
WITH (
  format='PARQUET',
  partitioning=ARRAY['day(event_ts)', 'device_id']
) AS SELECT * FROM hive.raw.temperature;

-- Repeat for other tables...
```

### Step 6: Run dbt Transformations

```bash
# Activate virtual environment (if using uv)
source .venv/bin/activate

# Run dbt models
cd ev_dbt

# Install dependencies (first time only)
dbt deps

# Run transformations
dbt run

# Run tests
dbt test
```

### Step 7: Access Superset

Open your browser to:
- **Superset:** http://localhost:8088
- **Username:** admin
- **Password:** admin (change in production!)

**Configure Data Source:**
1. Go to **Data** → **Databases**
2. Add new database with connection string:
   ```
   trino://user@localhost:8080/hive/default
   ```

## 📊 Performance Optimizations Applied

### Storage Efficiency
- **ZSTD compression** - 70-90% size reduction
- **Parquet format** - Columnar storage for fast queries
- **Partitioning** - year/month/day/hour structure

### Query Performance
- **8GB query memory** allocated to Trino
- **Column pruning** eliminates unnecessary data reads
- **Predicate pushdown** filters data at storage level

### Data Quality
- Automated tests on all models
- Value range validation
- Temporal consistency checks

## 🔧 Configuration

### Environment Variables (`.env`)

```env
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin

# Database credentials
POSTGRES_METASTORE_PASSWORD=your-password
POSTGRES_SUPERSET_PASSWORD=your-password

# Superset
SUPERSET_SECRET_KEY=generate-with-openssl-rand-base64-32
```

### Adjusting Resource Limits

Edit `docker-compose.phase3.yml`:
```yaml
trino:
  deploy:
    resources:
      limits:
        memory: 16G  # Increase for larger datasets
```

## 🐛 Troubleshooting

### Service Not Starting

```bash
# Check logs
docker compose -f docker-compose.phase3.yml logs trino

# Restart specific service
docker compose -f docker-compose.phase3.yml restart trino
```

### Connection Issues

```bash
# Check MinIO
curl http://localhost:9000/minio/health/live

# Test Trino
docker exec trino trino --execute "SELECT 1"
```

### Out of Memory

Increase memory in `trino/etc/config.properties`:
```properties
query.max-memory=16GB
query.max-memory-per-node=8GB
```

## 📝 Common Commands

```bash
# Start services
docker compose -f docker-compose.phase3.yml up -d

# Stop services
docker compose -f docker-compose.phase3.yml down

# View logs
docker compose -f docker-compose.phase3.yml logs -f

# Generate new data
python3 scripts/generate_telemetry_ev_multimodel.py

# Run dbt (activate venv first if using uv)
source .venv/bin/activate
dbt run
dbt test
dbt docs generate
```

## 📦 Using `uv` (Recommended)

```bash
# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create virtual environment
uv venv

# Install dependencies
uv pip install minio fastavro pyarrow trino dbt-core dbt-postgres

# Run scripts with uv
uv run python3 scripts/generate_telemetry_ev_multimodel.py
uv run dbt run
```

## 🎯 Next Steps

1. **Load Data**: Run Iceberg OPTIMIZE jobs
2. **Configure Alerts**: Set up monitoring
3. **Scale**: Add more workers to Trino
4. **Production**: Deploy with TLS and IAM

## 📚 Documentation

- `HIGH_PRIORITY_CHANGES.md` - Optimization details
- `TEST_RESULTS.md` - Test results
- `ev_telemetry_lakehouse_runbook.md` - Operational guide