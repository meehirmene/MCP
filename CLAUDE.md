# FlowForge

Multi-agent data pipeline platform — orchestrates Kafka, Flink, Iceberg, PostgreSQL, MongoDB via LLM-powered agents.

## Architecture

```
flowforge/
  agents/          # orchestrator, builder, healer, monitor
    shared/        # base agent, llm_client, memory (Redis)
  servers/         # MCP servers: flink_mcp, kafka_mcp, postgres_mcp, mongodb_mcp
  api_server.py    # FastAPI + WebSocket, rate limiting (slowapi)
  cli.py           # CLI: health, request, events, agents
  config.py        # Pydantic settings, env_prefix per service
dashboard/         # React 19 + Vite, WebSocket client
skills/            # Reusable pattern docs (CDC, schema-evolution, window-aggregation)
.agents/workflows/ # Multi-agent workflow definitions
```

## Running the Stack

```bash
# Start all infrastructure
docker compose up -d

# Start API server (separate terminal)
uvicorn flowforge.api_server:app --reload --port 8000

# Start dashboard (separate terminal)
cd dashboard && npm run dev

# Seed demo data
python scripts/seed_demo.py

# CLI
flowforge health
flowforge request "create a CDC pipeline from orders table"
```

## Environment Setup

```bash
cp .env.example .env
# Set LLM_API_KEY (OpenAI by default)
# Set LLM_PROVIDER=openai|anthropic|gemini|ollama
```

## Key Conventions

- MCP tool calls return JSON strings — always `json.loads(result)` before using
- LLM calls in agents: `await self.reason(prompt, context)` — never call OpenAI directly
- Redis key prefix: `flowforge:` — never use bare keys
- Kafka CDC topic naming: `cdc.{database}.{table}` (e.g. `cdc.ecommerce.orders`)
- Store pipeline state: `self.memory.set_pipeline_state(pipeline_id, {...})`
- Log events: `self.memory.log_event({"agent_id": ..., "agent_role": ..., "event_type": ..., "details": {...}})`

## Docker vs Local Ports

| Service | Local (outside Docker) | Internal (inside Docker) |
|---------|----------------------|------------------------|
| Kafka | `localhost:9092` | `kafka:29092` |
| Flink | `http://localhost:8081` | `http://flink-jobmanager:8081` |
| PostgreSQL | `localhost:5432` | `postgres:5432` |
| MongoDB | `localhost:27017` | `mongodb:27017` |
| Redis | `localhost:6379` | `redis:6379` |

## DO NOT

- Commit `.env` — it contains secrets
- Run `docker compose down -v` in development — wipes all data volumes
- Use `asyncio.run()` inside agents — they run in an async event loop already
- Call LLM providers directly — use `await self.reason()` so provider is swappable
