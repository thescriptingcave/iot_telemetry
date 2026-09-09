# dbt Models Guide - EV Telemetry Lakehouse

## Overview

This document explains the dbt models included in this repository.

## Model Structure

```
ev_dbt/models/
├── marts/              # Final aggregated tables
│   └── mart_evse_power_daily.sql
├── staging/            # Cleaned intermediate tables
│   ├── stg_evse_electrical.sql
│   ├── stg_evse_state.sql
│   ├── stg_evse_session_event.sql
│   ├── stg_humidity.sql
│   ├── stg_temperature.sql
│   └── stg_vibration.sql
└── sources.yml         # Source definitions for Parquet files
```

## Model Types

### 1. Sources (`sources.yml`)

Defines where raw data comes from (Parquet files in MinIO):

```yaml
sources:
  - name: raw
    database: iceberg
    schema: ev_v1
    tables:
      - name: temperature
      - name: humidity
      # ... other tables
```

### 2. Staging Models (`stg_*.sql`)

Clean and standardize raw data:

**stg_temperature.sql**:
- Casts data types
- Normalizes column names
- Adds `day` and `hour` partition columns
- Filters invalid temperature values (-40°C to 85°C)

**stg_evse_electrical.sql**:
- Standardizes charger IDs
- Converts power units (kW)
- Validates voltage/current ranges

### 3. Mart Models (`mart_*.sql`)

Aggregated business tables for analytics:

**mart_evse_power_daily.sql**:
- Aggregates hourly EV charging data to daily
- Calculates: avg power, total energy, max/min power
- Groups by day, device_id, charger_id
- Excludes fault periods

## Sample Queries

### Query Daily EV Power Statistics
```sql
SELECT 
  day,
  device_id,
  AVG(power_kw) as avg_power,
  SUM(energy_kwh_total) as total_energy,
  MAX(power_kw) as max_power
FROM ev_dbt.mart_evse_power_daily
WHERE day >= current_date - interval '7' day
GROUP BY day, device_id
ORDER BY day DESC, total_energy DESC;
```

### Query Temperature Trends
```sql
SELECT 
  day,
  AVG(temperature_c) as avg_temp,
  MIN(temperature_c) as min_temp,
  MAX(temperature_c) as max_temp
FROM ev_dbt.stg_temperature
WHERE device_id = 'sensor-01'
GROUP BY day
ORDER BY day DESC;
```

## Running dbt

```bash
# From ev_dbt directory
cd ev_dbt
source ../.venv/bin/activate

# Compile and run models
dbt run

# Run with specific model
dbt run --models stg_temperature

# Run tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

## dbt Commands Reference

| Command | Purpose |
|---------|---------|
| `dbt debug` | Check connection and configuration |
| `dbt deps` | Install dependencies |
| `dbt run` | Compile and run models |
| `dbt test` | Run data quality tests |
| `dbt clean` | Remove compiled files |
| `dbt build` | Run models + tests |
| `dbt snapshot` | Create snapshots (if using) |
| `dbt seed` | Load seed data |
| `dbt docs generate` | Generate documentation |

## Best Practices

1. **Run models in order**: staging → marts
2. **Test before deploying**: `dbt test` should pass
3. **Usedbt run --full-refresh** for initial loads
4. **Partition wisely**: daily partitioning for analytics
5. **Version control**: commit model changes

## Troubleshooting

| Issue | Solution |
|-------|----------|
| "Table not found" | Run staging models first |
| "Column mismatch" | Check source Parquet schema |
| "Slow queries" | Add partition filters |
| "Test failures" | Check data quality rules |

## Data Flow

```
Raw Parquet (MinIO)
    ↓
Staging Models (cleaned)
    ↓
Mart Models (aggregated)
    ↓
Superset Dashboard
```