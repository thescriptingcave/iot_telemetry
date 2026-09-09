# ACID Guarantees in the EV Telemetry Lakehouse

## What is ACID?

ACID is a set of four properties that database transactions must guarantee:

| Property | Meaning | Example in Your Lakehouse |
|----------|---------|---------------------------|
| Atomicity | All operations succeed, or none do | If writing fails mid-stream, no partial data committed |
| Consistency | Database moves from one valid state to another | All Parquet files follow schema validation rules |
| Isolation | Concurrent transactions don't interfere | Multiple pipelines can write without conflicts |
| Durability | Committed changes survive failures | Data persists in MinIO even if Trino crashes |

## ACID in Your Iceberg Lakehouse

### 1. Atomicity

Iceberg provides atomic table operations:

```sql
INSERT INTO iceberg.curated.evse_electrical SELECT * FROM staging;
```

### 2. Consistency Validation

dbt models enforce data consistency:

```yaml
columns:
  - name: power_kw
    data_tests:
      - not_null
      - dbt_utils.expression_is_true:
          expression: "power_kw >= 0 AND power_kw <= 500"
```

### 3. Isolation

Iceberg uses snapshot isolation:

```sql
SELECT * FROM iceberg.curated.evse_electrical FOR VERSION AS OF '2024-09-08';
```

### 4. Durability via MinIO

- Multi-disk redundancy
- Checksums for bit rot detection
- Persistent storage through container restarts

## ACID in Practice

### Safe Write Pattern

```bash
# 1. Write to staging table
docker exec trino trino --execute "CREATE TABLE iceberg.staging.evse_electrical_new AS SELECT * FROM hive.raw.evse_electrical;"

# 2. Validate data
docker exec trino trino --execute "SELECT COUNT(*) FROM iceberg.staging.evse_electrical_new;"

# 3. Atomic swap
docker exec trino trino --execute "CALL system.swap_table('iceberg', 'curated', 'evse_electrical', 'staging', 'evse_electrical_new');"
```

### Point-in-Time Recovery

```sql
SELECT * FROM iceberg.curated.evse_electrical FOR TIMESTAMP AS OF CURRENT_TIMESTAMP - INTERVAL '1' HOUR;
```

## Non-ACID Operations

| Scenario | ACID? | Solution |
|----------|-------|----------|
| Writing Parquet directly to S3 | ❌ | Use Iceberg INSERT |
| ALTER TABLE ... RENAME | ⚠️ | Use swap_table procedure |

## Summary

Your lakehouse provides ACID guarantees through:
1. Iceberg tables - Atomic commits
2. MinIO - Durable storage
3. Trino - Snapshot isolation
4. dbt - Consistency validation