---
name: cdc-ingestion
description: Set up Change Data Capture from a relational database to Kafka using logical replication
---

# CDC Ingestion Skill

Captures row-level changes (INSERT, UPDATE, DELETE) from PostgreSQL and streams them to Kafka topics in real-time.

## Prerequisites
- PostgreSQL with `wal_level=logical` (configured in docker-compose)
- Publication created for target tables
- Kafka cluster accessible

## Steps

1. **Verify CDC Readiness**
   Use the PostgreSQL MCP server to check WAL level and publications:
   ```
   Tool: pg_check_cdc
   Expected: wal_level = "logical", publication exists
   ```

2. **Create Kafka Topic**
   Create a topic matching the source table naming convention `cdc.{database}.{table}`:
   ```
   Tool: kafka_create_topic
   Args: topic_name="cdc.ecommerce.orders", num_partitions=3
   ```

3. **Inspect Source Schema**
   Get the full schema to generate the correct Flink SQL source definition:
   ```
   Tool: pg_inspect_schema
   Args: table_name="orders"
   ```

4. **Generate Flink CDC Source Table**
   Create a Flink SQL table definition using the `postgres-cdc` connector:
   ```sql
   CREATE TABLE orders_cdc (
       order_id INT,
       customer_id INT,
       status STRING,
       total_amount DECIMAL(10,2),
       region STRING,
       created_at TIMESTAMP(3),
       updated_at TIMESTAMP(3),
       PRIMARY KEY (order_id) NOT ENFORCED
   ) WITH (
       'connector' = 'postgres-cdc',
       'hostname' = 'postgres',
       'port' = '5432',
       'username' = 'flowforge',
       'password' = 'flowforge123',
       'database-name' = 'ecommerce',
       'schema-name' = 'public',
       'table-name' = 'orders',
       'slot.name' = 'orders_slot',
       'decoding.plugin.name' = 'pgoutput'
   );
   ```

5. **Verify Data Flow**
   Insert test data and consume from the CDC topic to verify:
   ```
   Tool: kafka_consume
   Args: topic="cdc.ecommerce.orders", max_messages=5
   ```

## Type Mapping (PostgreSQL → Flink)

| PostgreSQL | Flink SQL |
|-----------|-----------|
| INTEGER | INT |
| BIGINT | BIGINT |
| VARCHAR(n) | STRING |
| TEXT | STRING |
| DECIMAL(p,s) | DECIMAL(p,s) |
| BOOLEAN | BOOLEAN |
| TIMESTAMP | TIMESTAMP(3) |
| DATE | DATE |
| JSONB | STRING |

## Error Handling
- If WAL level is not logical: requires PostgreSQL restart with config change
- If publication is missing: CREATE PUBLICATION automatically
- If replication slot exists: reuse or drop and recreate
