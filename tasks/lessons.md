# FlowForge Lessons

Patterns learned from development sessions. Update after corrections.

## Tool Calls

### MCP tool results are JSON strings
Always `json.loads(result)` before using the value. Never assume the return is already a dict.
```python
result = await self.call_tool("flink_list_jobs", {})
jobs = json.loads(result)  # required
```

## Agent Patterns

### LLM calls
Use `await self.reason(prompt, context)` — not direct OpenAI/Anthropic calls. This keeps the provider swappable.

### Redis Keys
Prefix all keys with `flowforge:` to avoid namespace collision with other applications.

## Infrastructure

### Docker vs Local Ports
When running Python outside Docker, use localhost ports (`localhost:9092`, `localhost:5432`).
When running inside Docker containers, use internal hostnames (`kafka:29092`, `postgres:5432`).
The `.env.example` has both sets — use the correct one for your context.

### Docker volumes
Never run `docker compose down -v` in development — it wipes all data volumes including
Kafka topics, Flink checkpoints, Iceberg tables, and database data.

## Schema Healing

### Column removal is dangerous
Never auto-remove columns from Iceberg tables. Only add new nullable columns or widen types.
Escalate column removal to human review via the Orchestrator.
