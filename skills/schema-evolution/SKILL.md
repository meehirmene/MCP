---
name: schema-evolution
description: Detect and automatically resolve schema drift between source databases and downstream Iceberg/Flink tables
---

# Schema Evolution Skill

Automatically detects when a source table schema changes (new columns, type changes) and evolves downstream Iceberg tables to match.

## When to Use
- Monitor Agent detects schema drift
- Healer Agent needs to resolve schema incompatibilities
- Migration Agent performing planned schema changes

## Detection Steps

1. **Get Current Source Schema**
   ```
   Tool: pg_inspect_schema
   Args: table_name="orders"
   ```

2. **Compare Against Baseline**
   The Monitor Agent stores baseline schemas in Redis. Compare:
   - Columns added
   - Columns removed
   - Column types changed

3. **Classify the Change**
   - **Safe (auto-heal):** New nullable column added
   - **Caution (needs review):** Column type widened (INT → BIGINT)
   - **Dangerous (human required):** Column removed, type narrowed

## Healing Steps

### For New Columns (Safe)
```sql
ALTER TABLE iceberg_catalog.ecommerce.orders
ADD COLUMN discount_pct DECIMAL(5,2);
```

### For Type Widening (Caution)
```sql
-- Verify data compatibility first
SELECT count(*) FROM orders WHERE column_value > MAX_VALUE;

-- Then alter
ALTER TABLE iceberg_catalog.ecommerce.orders
ALTER COLUMN amount TYPE DECIMAL(12,2);
```

### For Column Removal (Dangerous)
- Do NOT auto-remove from downstream tables
- Log a warning event
- Escalate to human review via the Orchestrator

## PostgreSQL to Iceberg Type Mapping

| PostgreSQL | Iceberg | Flink SQL |
|-----------|---------|-----------|
| INTEGER | int | INT |
| BIGINT | long | BIGINT |
| DECIMAL(p,s) | decimal(p,s) | DECIMAL(p,s) |
| VARCHAR(n) | string | STRING |
| TEXT | string | STRING |
| BOOLEAN | boolean | BOOLEAN |
| TIMESTAMP | timestamp | TIMESTAMP(3) |
| TIMESTAMP WITH TZ | timestamptz | TIMESTAMP_LTZ(3) |
| DATE | date | DATE |
| DOUBLE PRECISION | double | DOUBLE |
| REAL | float | FLOAT |

## Post-Healing Validation
1. Restart affected Flink jobs
2. Verify data flows through with new schema
3. Check for any null values in new columns
4. Update the baseline schema in Monitor Agent memory
