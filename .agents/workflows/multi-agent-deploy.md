---
description: Deploy an end-to-end streaming pipeline using multi-agent orchestration
---

# Multi-Agent Pipeline Deployment

Deploy a streaming data pipeline from source database to Iceberg lakehouse using the full agent team.

## Steps

1. **Orchestrator receives the NL request** and decomposes into sub-tasks using LLM reasoning.

2. **Builder Agent inspects source databases**
   - List PostgreSQL tables via `pg_list_tables`
   - Get schema details via `pg_inspect_schema`
   - Check CDC readiness via `pg_check_cdc`
   // turbo

3. **Builder Agent creates Kafka topics** matching `cdc.{db}.{table}` naming convention.
   // turbo

4. **Builder Agent generates Flink SQL** for:
   - CDC source table connectors
   - Stream processing logic (window aggregations)
   - Iceberg sink table definition

5. **Quality Agent validates source schema** ensures:
   - No null primary keys
   - Data types are compatible with downstream
   - Sample data passes integrity checks

6. **Builder Agent deploys the Flink job** to the cluster via `flink_submit_sql`.

7. **Monitor Agent configures health tracking**:
   - Baseline schema stored in Redis
   - Alert thresholds set for throughput
   - SLA freshness targets established
   // turbo

8. **Orchestrator synthesizes results** and reports status to the user in natural language.
