# EV Telemetry Lakehouse - Quick Start Guide

This guide will help you get the EV Telemetry Lakehouse platform up and running in under 10 minutes.

## 🏗️ Architecture

```
IoT Devices → MinIO (S3) → Iceberg (Parquet) → Trino → dbt → Superset
```

**Components:**
- **MinIO** - S3-compatible object storage
- **Iceberg REST Catalog** - Modern table metadata (replaces Hive Metastore)
- **Trino** - Query engine (Iceberg + S3 connectors)
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
pip install minio fastavro pyarrow trino dbt-core dbt-postgres python-dotenv
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

# Install dependencies (includes python-dotenv for .env file support)
uv pip install minio fastavro pyarrow trino dbt-core dbt-postgres python-dotenv
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
- ✅ iceberg-rest (port 8181)

### Step 3: Generate Telemetry Data

```bash
# Generate EV telemetry with Parquet output
python3 scripts/generate_telemetry_ev_multimodel.py
```

This will:
- Connect to MinIO
- Create partitioned Parquet files (ZSTD compressed)
- Generate ~1000+ records

### Step 4: Query with Trino (Iceberg)

```bash
# Connect to Trino
docker exec -it trino trino

# Check catalogs
SHOW CATALOGS;

# List Iceberg schemas
SHOW SCHEMAS FROM iceberg;

# List Iceberg tables
SHOW TABLES FROM iceberg.curated;

# Run a query
SELECT day, AVG(power_kw), SUM(energy_kwh_total) 
FROM iceberg.curated.evse_electrical 
GROUP BY day 
ORDER BY day DESC 
LIMIT 10;
```

### Step 5: Run dbt Transformations

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

### Step 6: Access Superset

Open your browser to:
- **Superset:** http://localhost:8088
- **Username:** admin
- **Password:** admin (change in production!)

**Configure Data Source:**
1. Go to **Data** → **Databases**
2. Add new database with connection string:
   ```
   trino://user@localhost:8080/iceberg
   ```

## 📊 Performance Optimizations Applied

### Storage Efficiency
- **ZSTD compression** - 70-90% size reduction
- **Parquet format** - Columnar storage for fast queries
- **Partitioning** - year/month/day/hour structure

### Query Performance
- **Column pruning** eliminates unnecessary data reads
- **Predicate pushdown** filters data at storage level
- **Vectorized execution** for faster processing

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

# Verify Iceberg REST catalog
docker exec trino trino --execute "SHOW SCHEMAS FROM iceberg;"
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
uv pip install minio fastavro pyarrow trino dbt-core dbt-postgres python-dotenv

# Run scripts with uv
uv run python3 scripts/generate_telemetry_ev_multimodel.py
uv run dbt run
```

## 🎯 Next Steps

### 1. **Iceberg Table Operations**

Iceberg tables provide time travel, schema evolution, and ACID transactions:

```bash
# Show all Iceberg tables
docker exec -it trino trino --execute "SHOW TABLES FROM iceberg.curated;"

# Query with time travel
SELECT * FROM iceberg.curated.temperature FOR VERSION AS OF 'timestamp';

# Run table maintenance
CALL iceberg.system.optimize('curated', 'evse_electrical', action => 'rewrite_data_files');

# View table properties
DESCRIBE iceberg.curated.evse_electrical;
```

### 2. **Configure Alerts and Monitoring**

Set up monitoring for your lakehouse infrastructure using Prometheus and Grafana:

```yaml
# Add to docker-compose.phase3.yml services
prometheus:
  image: prom/prometheus:latest
  ports:
    - "9090:9090"
  volumes:
    - ./prometheus.yml:/etc/prometheus/prometheus.yml
  depends_on:
    - minio
    - trino
```

Set up alerts for:
- MinIO disk usage (>80%)
- Trino query failures
- Warehouse storage growth

### 3. **Scale Trino Cluster**

Add more workers for larger datasets:

```yaml
# In docker-compose.phase3.yml, add more trino workers
trino-worker-1:
  image: trinodb/trino:latest
  depends_on:
    - trino
  environment:
    - COORDINATOR=false
    - DISCOVERY_URI=http://trino:8080
  networks: [lake]

trino-worker-2:
  image: trinodb/trino:latest
  depends_on:
    - trino
  environment:
    - COORDINATOR=false
    - DISCOVERY_URI=http://trino:8080
  networks: [lake]
```

Increase query memory in `trino/etc/config.properties`:
```properties
worker-concurrency=8
query.max-memory-per-node=8GB
query.max-total-memory-per-node=10GB
```

### 4. **Production Deployment**

For production, implement security and reliability best practices:

**TLS/HTTPS:**
- Generate certificates for MinIO
- Configure MinIO with TLS: `--tls-cert-file /certs/public.crt --tls-key-file /certs/private.key`
- Update Trino to use HTTPS: `http-server.https.enabled=true`

**IAM and Authentication:**
- Configure S3 IAM roles for MinIO
- Set up Trino LDAP authentication
- Enable Superset OAuth (Google, GitHub, etc.)

**High Availability:**
- Run multiple MinIO instances with distributed mode
- Use external database for Hive Metastore (Postgres in HA mode)
- Configure Trino coordinator with backup

**Backup Strategy:**
```bash
# Backup MinIO data
docker exec minio mc cp -r local/warehouse s3://backup-bucket/

# Backup database
docker exec postgres-metastore pg_dump metastore_db > backups/metastore-$(date +%Y%m%d).sql
```

## 📚 Documentation

- `HIGH_PRIORITY_CHANGES.md` - Optimization details
- `TEST_RESULTS.md` - Test results
- `ev_telemetry_lakehouse_runbook.md` - Operational guide