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
import uuid
from datetime import datetime, timezone
from contextlib import asynccontextmanager

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

from flowforge.agents.orchestrator import OrchestratorAgent
from flowforge.agents.shared.memory import AgentMemory
from flowforge.agents.shared.base import AgentTask, AgentRole
from flowforge.config import config
from flowforge.data_generator import DataProducer

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Rate limiter
# ─────────────────────────────────────────────
limiter = Limiter(key_func=get_remote_address)

# ─────────────────────────────────────────────
# Globals
# ─────────────────────────────────────────────
orchestrator: OrchestratorAgent | None = None
memory: AgentMemory | None = None
scheduler: AsyncIOScheduler | None = None
ws_clients: list[WebSocket] = []


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize on startup, cleanup on shutdown."""
    global orchestrator, memory, scheduler
    logger.info("🔥 FlowForge API Server starting...")
    orchestrator = OrchestratorAgent()
    memory = AgentMemory()
    scheduler = AsyncIOScheduler()
    # Restore persisted schedules from Redis
    for sched in memory.get_schedules():
        try:
            _register_cron_job(sched["schedule_id"], sched["request"], sched["cron_expression"])
        except Exception as e:
            logger.warning(f"Failed to restore schedule {sched['schedule_id']}: {e}")
    scheduler.start()
    yield
    scheduler.shutdown(wait=False)
    logger.info("FlowForge API Server shutting down.")


def _register_cron_job(schedule_id: str, request_text: str, cron_expr: str) -> None:
    """Register a cron job with APScheduler."""
    parts = cron_expr.strip().split()
    trigger = CronTrigger(
        minute=parts[0], hour=parts[1], day=parts[2], month=parts[3], day_of_week=parts[4]
    )
    scheduler.add_job(
        _run_scheduled_pipeline,
        trigger=trigger,
        id=schedule_id,
        replace_existing=True,
        args=[schedule_id, request_text],
    )


async def _run_scheduled_pipeline(schedule_id: str, request_text: str) -> None:
    """Execute a scheduled pipeline request and broadcast the result."""
    logger.info(f"Running scheduled pipeline {schedule_id}: {request_text}")
    try:
        result = await orchestrator.handle_user_request(request_text)
    except Exception as e:
        result = {"summary": f"Scheduled run failed: {e}", "tasks_completed": 0, "tasks_failed": 1}
    await broadcast({
        "type": "scheduled_run_completed",
        "schedule_id": schedule_id,
        "summary": result.get("summary", ""),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })
    memory.log_event({
        "agent_role": "scheduler",
        "event_type": "SCHEDULED_RUN",
        "details": {"schedule_id": schedule_id, "request": request_text, "summary": result.get("summary", "")},
    })


app = FastAPI(
    title="FlowForge API",
    description="Multi-Agent AI Data Pipeline Platform",
    version="0.1.0",
    lifespan=lifespan,
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

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


class ScheduleRequest(BaseModel):
    request: str
    cron_expression: str  # 5-field standard cron: "0 * * * *"
    name: str = ""


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
@limiter.limit("10/minute")
async def handle_request(request: Request, req: PipelineRequest):
    """Send a natural language request to the Orchestrator agent (rate limited: 10/min)."""
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
        "from_cache": result.get("from_cache", False),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })

    # Broadcast updated events so WebSocket clients refresh their feed
    try:
        recent_events = memory.get_events(count=5)
        for evt in reversed(recent_events):
            await broadcast({"type": "event", "data": evt})
    except Exception:
        pass

    return result


@app.get("/api/health")
async def get_health():
    """Structured health check across all pipeline infrastructure components."""
    checks: dict[str, dict] = {}
    issues: list[str] = []

    # Redis
    try:
        memory._redis.ping()
        checks["redis"] = {"status": "healthy", "host": config.redis.host, "port": config.redis.port}
    except Exception as e:
        checks["redis"] = {"status": "unhealthy", "error": str(e)}
        issues.append(f"Redis unreachable: {e}")

    # Flink JobManager
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.get(f"{config.flink.jobmanager_url}/overview")
            resp.raise_for_status()
            flink_data = resp.json()
        checks["flink"] = {
            "status": "healthy",
            "jobs_running": flink_data.get("jobs-running", 0),
            "jobs_finished": flink_data.get("jobs-finished", 0),
            "taskmanagers": flink_data.get("taskmanagers", 0),
            "slots_available": flink_data.get("slots-available", 0),
        }
    except Exception as e:
        checks["flink"] = {"status": "unhealthy", "error": str(e)}
        issues.append(f"Flink JobManager unreachable: {e}")

    # Kafka (via admin bootstrap)
    try:
        from confluent_kafka.admin import AdminClient
        admin = AdminClient({"bootstrap.servers": config.kafka.bootstrap_servers, "socket.timeout.ms": 3000})
        meta = admin.list_topics(timeout=3)
        checks["kafka"] = {"status": "healthy", "topics": len(meta.topics)}
    except Exception as e:
        checks["kafka"] = {"status": "degraded", "error": str(e)}
        issues.append(f"Kafka check failed: {e}")

    # PostgreSQL
    try:
        import psycopg2
        conn = psycopg2.connect(config.postgres.dsn, connect_timeout=3)
        conn.close()
        checks["postgres"] = {"status": "healthy", "host": config.postgres.host}
    except Exception as e:
        checks["postgres"] = {"status": "unhealthy", "error": str(e)}
        issues.append(f"PostgreSQL unreachable: {e}")

    # MongoDB
    try:
        import pymongo
        client = pymongo.MongoClient(config.mongo.uri, serverSelectionTimeoutMS=3000)
        client.server_info()
        client.close()
        checks["mongodb"] = {"status": "healthy", "host": config.mongo.host}
    except Exception as e:
        checks["mongodb"] = {"status": "degraded", "error": str(e)}
        issues.append(f"MongoDB check failed: {e}")

    unhealthy = [k for k, v in checks.items() if v["status"] == "unhealthy"]
    degraded = [k for k, v in checks.items() if v["status"] == "degraded"]

    if unhealthy:
        overall = "unhealthy"
    elif degraded:
        overall = "degraded"
    else:
        overall = "healthy"

    return {
        "overall_status": overall,
        "components": checks,
        "issues": issues,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


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

    # Also push the new event to WebSocket clients
    try:
        memory.log_event({
            "agent_role": "system",
            "event_type": "SCHEMA_DRIFT_SIMULATED",
            "details": result,
        })
        recent_events = memory.get_events(count=3)
        for evt in reversed(recent_events):
            await broadcast({"type": "event", "data": evt})
    except Exception:
        pass

    return result


@app.get("/api/learnings")
async def get_learnings():
    """Return all stored agent incident patterns and learned resolutions."""
    try:
        learnings = memory.get_all_learnings()
        return {"learnings": learnings, "count": len(learnings)}
    except Exception as e:
        return {"learnings": [], "count": 0, "error": str(e)}


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
# Scheduled Pipelines
# ─────────────────────────────────────────────
@app.post("/api/schedules")
async def create_schedule(req: ScheduleRequest):
    """Create a new scheduled pipeline (cron-based)."""
    if len(req.cron_expression.split()) != 5:
        raise HTTPException(status_code=400, detail="cron_expression must have 5 fields: min hr dom mon dow")
    schedule_id = str(uuid.uuid4())[:8]
    schedule = {
        "name": req.name or req.request[:50],
        "request": req.request,
        "cron_expression": req.cron_expression,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    memory.set_schedule(schedule_id, schedule)
    _register_cron_job(schedule_id, req.request, req.cron_expression)
    logger.info(f"Schedule created: {schedule_id} ({req.cron_expression})")
    return {"schedule_id": schedule_id, **schedule}


@app.get("/api/schedules")
async def list_schedules():
    """List all active scheduled pipelines."""
    schedules = memory.get_schedules()
    return {"schedules": schedules, "count": len(schedules)}


@app.delete("/api/schedules/{schedule_id}")
async def delete_schedule(schedule_id: str):
    """Delete a scheduled pipeline by ID."""
    existed = memory.delete_schedule(schedule_id)
    if not existed:
        raise HTTPException(status_code=404, detail=f"Schedule {schedule_id} not found")
    if scheduler.get_job(schedule_id):
        scheduler.remove_job(schedule_id)
    return {"status": "deleted", "schedule_id": schedule_id}


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
