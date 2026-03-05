"""
FlowForge API Server — FastAPI backend for the FlowForge Dashboard.

Provides REST + WebSocket endpoints for:
- Sending natural language requests
- Viewing agent status, pipeline health, and events
- Real-time streaming of agent activity via WebSocket
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from flowforge.agents.orchestrator import OrchestratorAgent
from flowforge.agents.shared.memory import AgentMemory
from flowforge.agents.shared.base import AgentTask, AgentRole
from flowforge.data_generator import DataProducer

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Globals
# ─────────────────────────────────────────────
orchestrator: OrchestratorAgent | None = None
memory: AgentMemory | None = None
ws_clients: list[WebSocket] = []


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize on startup, cleanup on shutdown."""
    global orchestrator, memory
    logger.info("🔥 FlowForge API Server starting...")
    orchestrator = OrchestratorAgent()
    memory = AgentMemory()
    yield
    logger.info("FlowForge API Server shutting down.")


app = FastAPI(
    title="FlowForge API",
    description="Multi-Agent AI Data Pipeline Platform",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─────────────────────────────────────────────
# Request / Response Models
# ─────────────────────────────────────────────
class PipelineRequest(BaseModel):
    request: str
    context: dict = {}


class HealthResponse(BaseModel):
    overall_status: str
    components: dict
    issues: list
    timestamp: str


class GenerateDataRequest(BaseModel):
    target: str = "kafka"  # kafka | postgres
    count: int = 50
    topic: str = "ecommerce.events"


# ─────────────────────────────────────────────
# WebSocket broadcast
# ─────────────────────────────────────────────
async def broadcast(event: dict):
    """Broadcast an event to all connected WebSocket clients."""
    message = json.dumps(event, default=str)
    disconnected = []
    for ws in ws_clients:
        try:
            await ws.send_text(message)
        except Exception:
            disconnected.append(ws)
    for ws in disconnected:
        ws_clients.remove(ws)


# ─────────────────────────────────────────────
# REST Endpoints
# ─────────────────────────────────────────────
@app.get("/")
async def root():
    return {
        "name": "FlowForge API",
        "version": "0.1.0",
        "status": "running",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.post("/api/request")
async def handle_request(req: PipelineRequest):
    """Send a natural language request to the Orchestrator agent."""
    await broadcast({
        "type": "request_received",
        "request": req.request,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })

    result = await orchestrator.handle_user_request(req.request)

    await broadcast({
        "type": "request_completed",
        "summary": result.get("summary", ""),
        "tasks_completed": result.get("tasks_completed", 0),
        "tasks_failed": result.get("tasks_failed", 0),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })

    return result


@app.get("/api/health")
async def get_health():
    """Run a health check across all pipeline components."""
    from flowforge.agents.monitor import MonitorAgent

    monitor = MonitorAgent()
    task = AgentTask(
        description="Full health check",
        assigned_to=AgentRole.MONITOR,
    )
    result = await monitor.execute_task(task)

    if result.result:
        return result.result.get("health", result.result)

    raise HTTPException(status_code=500, detail="Health check failed")


@app.get("/api/agents")
async def get_agents():
    """List all registered agents."""
    try:
        agents = memory.get_active_agents()
        return {"agents": agents, "count": len(agents)}
    except Exception as e:
        return {"agents": [], "count": 0, "error": str(e)}


@app.get("/api/events")
async def get_events(count: int = 50):
    """Get recent agent activity events."""
    try:
        events = memory.get_events(count=count)
        return {"events": events, "count": len(events)}
    except Exception as e:
        return {"events": [], "count": 0, "error": str(e)}


@app.get("/api/pipelines")
async def get_pipelines():
    """List all active pipelines."""
    try:
        pipelines = memory.list_pipelines()
        return {"pipelines": pipelines, "count": len(pipelines)}
    except Exception as e:
        return {"pipelines": [], "count": 0, "error": str(e)}


@app.post("/api/generate-data")
async def generate_data(req: GenerateDataRequest):
    """Generate sample e-commerce data for testing."""
    producer = DataProducer()

    if req.target == "kafka":
        count = producer.produce_to_kafka(topic=req.topic, count=req.count)
        return {"status": "produced", "target": "kafka", "topic": req.topic, "count": count}
    elif req.target == "postgres":
        count = producer.produce_to_postgres(count=req.count)
        return {"status": "inserted", "target": "postgres", "count": count}
    else:
        raise HTTPException(status_code=400, detail=f"Unknown target: {req.target}")


@app.post("/api/simulate/schema-drift")
async def simulate_schema_drift():
    """Simulate schema drift by adding a column to the orders table."""
    producer = DataProducer()
    result = producer.simulate_schema_drift()

    await broadcast({
        "type": "schema_drift_simulated",
        "details": result,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })

    return result


@app.get("/api/databases/postgres/tables")
async def get_postgres_tables():
    """List PostgreSQL tables."""
    from flowforge.servers.postgres_mcp import list_tables
    result = json.loads(list_tables())
    return result


@app.get("/api/databases/postgres/tables/{table_name}")
async def get_postgres_table_schema(table_name: str):
    """Get PostgreSQL table schema."""
    from flowforge.servers.postgres_mcp import inspect_schema
    result = json.loads(inspect_schema(table_name))
    return result


@app.get("/api/databases/mongodb/collections")
async def get_mongodb_collections():
    """List MongoDB collections."""
    from flowforge.servers.mongodb_mcp import list_collections
    result = json.loads(list_collections())
    return result


@app.get("/api/kafka/topics")
async def get_kafka_topics():
    """List Kafka topics."""
    from flowforge.servers.kafka_mcp import list_topics
    result = json.loads(list_topics())
    return result


@app.get("/api/flink/jobs")
async def get_flink_jobs():
    """List Flink jobs."""
    from flowforge.servers.flink_mcp import list_jobs
    result = json.loads(await list_jobs())
    return result


# ─────────────────────────────────────────────
# WebSocket for real-time updates
# ─────────────────────────────────────────────
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket for real-time agent activity streaming."""
    await websocket.accept()
    ws_clients.append(websocket)
    logger.info(f"WebSocket client connected. Total: {len(ws_clients)}")

    try:
        # Send initial state
        await websocket.send_text(json.dumps({
            "type": "connected",
            "message": "Connected to FlowForge real-time stream",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }))

        # Keep alive and listen for commands
        while True:
            data = await websocket.receive_text()
            msg = json.loads(data)

            if msg.get("type") == "request":
                # Handle NL request via WebSocket
                result = await orchestrator.handle_user_request(msg.get("text", ""))
                await websocket.send_text(json.dumps({
                    "type": "request_result",
                    "result": result,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }, default=str))

            elif msg.get("type") == "ping":
                await websocket.send_text(json.dumps({"type": "pong"}))

    except WebSocketDisconnect:
        ws_clients.remove(websocket)
        logger.info(f"WebSocket client disconnected. Total: {len(ws_clients)}")
