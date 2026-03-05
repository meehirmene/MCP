---
description: Diagnose and fix a failing streaming pipeline
---

# Pipeline Diagnosis

Step-by-step workflow for diagnosing and fixing pipeline failures.

## Steps

1. **Check Flink cluster health**
   ```
   flowforge health
   ```
   // turbo

2. **List all Flink jobs and identify failed ones**
   - Use Monitor Agent's health check capability
   - Focus on jobs with state = FAILED or RESTARTING

3. **Get job exception details**
   - Retrieve stack traces from failed jobs
   - Use LLM to translate errors into plain English

4. **Determine root cause category**:
   - `schema_drift`: Source schema changed, downstream can't parse
   - `resource_exhaustion`: OOM, too many task slots used
   - `network_issue`: Kafka/DB connectivity lost
   - `data_quality`: Malformed events, null keys, type mismatches

5. **Apply fix based on category**:
   - Schema drift → invoke `schema-evolution` skill
   - Resource issue → increase parallelism or task manager slots
   - Network issue → wait and retry with exponential backoff
   - Data quality → quarantine bad events, replay clean ones

6. **Verify fix**
   // turbo
   ```
   flowforge health
   ```

7. **Generate incident report** using Orchestrator's NL synthesis
