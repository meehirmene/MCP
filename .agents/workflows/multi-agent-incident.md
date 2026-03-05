---
description: Multi-agent collaborative incident response for pipeline failures
---

# Multi-Agent Incident Response

When a pipeline issue is detected, multiple agents collaborate to diagnose, heal, and verify recovery.

## Steps

1. **Monitor Agent detects anomaly** — schema drift, throughput drop, or job failure.
   - Publishes event to shared memory with severity level
   // turbo

2. **Orchestrator triages the incident** based on severity:
   - CRITICAL: Immediate Healer dispatch
   - HIGH: Healer + Optimizer dispatched
   - WARNING: Logged, monitored for escalation

3. **Healer Agent diagnoses root cause**:
   - Retrieve Flink job exceptions via `flink_job_exceptions`
   - Analyze logs with LLM for root cause
   - Classify as: schema_drift | resource_issue | data_issue | config_error

4. **Healer applies the fix**:
   - Schema drift → auto-evolve Iceberg table (invoke schema-evolution skill)
   - Job crash → restart with corrected config
   - Backpressure → request Optimizer to rescale
   - Data issue → trigger replay from Kafka offset

5. **Quality Agent validates recovery**:
   - Check data integrity post-fix
   - Verify no data loss during healing
   - Compare row counts pre/post incident
   // turbo

6. **Monitor Agent confirms recovery**:
   - Job back to RUNNING state
   - Throughput restored to baseline
   - Reset alert state
   // turbo

7. **Orchestrator generates incident report** in natural language:
   - What happened
   - Root cause
   - Actions taken
   - Time to recovery
   - Recommendations to prevent recurrence
