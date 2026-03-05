"""
FlowForge Agent Memory — Shared state store for inter-agent coordination.

Uses Redis as the backend for fast, shared state across all agents.
Stores pipeline states, agent tasks, and coordination data.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any

import redis

from flowforge.config import config

logger = logging.getLogger(__name__)


class AgentMemory:
    """
    Shared memory store for the multi-agent system.

    Backed by Redis, this provides:
    - Pipeline state tracking (BUILDING → RUNNING → DEGRADED → HEALING)
    - Task queue for agent work items
    - Event log for agent activity history
    - Agent heartbeats for liveness detection
    """

    def __init__(self):
        self._redis = redis.Redis(
            host=config.redis.host,
            port=config.redis.port,
            db=config.redis.db,
            decode_responses=True,
        )
        self.KEY_PREFIX = "flowforge:"

    def _key(self, *parts: str) -> str:
        return self.KEY_PREFIX + ":".join(parts)

    # ─────────────────────────────────────────────
    # Pipeline State
    # ─────────────────────────────────────────────

    def set_pipeline_state(self, pipeline_id: str, state: dict) -> None:
        """Store or update the state of a pipeline."""
        state["updated_at"] = datetime.now(timezone.utc).isoformat()
        self._redis.hset(self._key("pipelines", pipeline_id), mapping={
            k: json.dumps(v) if isinstance(v, (dict, list)) else str(v)
            for k, v in state.items()
        })
        # Add to pipeline index
        self._redis.sadd(self._key("pipeline_ids"), pipeline_id)
        logger.info(f"Pipeline state updated: {pipeline_id} → {state.get('status', 'unknown')}")

    def get_pipeline_state(self, pipeline_id: str) -> dict | None:
        """Get the current state of a pipeline."""
        data = self._redis.hgetall(self._key("pipelines", pipeline_id))
        if not data:
            return None
        # Deserialize JSON fields
        result = {}
        for k, v in data.items():
            try:
                result[k] = json.loads(v)
            except (json.JSONDecodeError, TypeError):
                result[k] = v
        return result

    def list_pipelines(self) -> list[dict]:
        """List all pipeline states."""
        pipeline_ids = self._redis.smembers(self._key("pipeline_ids"))
        pipelines = []
        for pid in pipeline_ids:
            state = self.get_pipeline_state(pid)
            if state:
                state["pipeline_id"] = pid
                pipelines.append(state)
        return pipelines

    # ─────────────────────────────────────────────
    # Task Queue
    # ─────────────────────────────────────────────

    def enqueue_task(self, agent_role: str, task: dict) -> None:
        """Add a task to an agent's work queue."""
        task["enqueued_at"] = datetime.now(timezone.utc).isoformat()
        self._redis.rpush(
            self._key("tasks", agent_role),
            json.dumps(task),
        )

    def dequeue_task(self, agent_role: str) -> dict | None:
        """Pop the next task from an agent's work queue."""
        data = self._redis.lpop(self._key("tasks", agent_role))
        if data:
            return json.loads(data)
        return None

    def get_pending_tasks(self, agent_role: str) -> list[dict]:
        """Peek at all pending tasks for an agent."""
        items = self._redis.lrange(self._key("tasks", agent_role), 0, -1)
        return [json.loads(item) for item in items]

    # ─────────────────────────────────────────────
    # Event Log
    # ─────────────────────────────────────────────

    def log_event(self, event: dict) -> None:
        """Log an agent event to the activity stream."""
        event["logged_at"] = datetime.now(timezone.utc).isoformat()
        self._redis.lpush(
            self._key("events"),
            json.dumps(event),
        )
        # Trim to last 1000 events
        self._redis.ltrim(self._key("events"), 0, 999)

    def get_events(self, count: int = 50) -> list[dict]:
        """Get the most recent agent events."""
        items = self._redis.lrange(self._key("events"), 0, count - 1)
        return [json.loads(item) for item in items]

    def get_events_for_pipeline(self, pipeline_id: str, count: int = 50) -> list[dict]:
        """Get events related to a specific pipeline."""
        all_events = self.get_events(count=200)
        return [e for e in all_events if e.get("pipeline_id") == pipeline_id][:count]

    # ─────────────────────────────────────────────
    # Agent Registry / Heartbeats
    # ─────────────────────────────────────────────

    def register_agent(self, agent_id: str, agent_info: dict) -> None:
        """Register an agent in the shared registry."""
        agent_info["registered_at"] = datetime.now(timezone.utc).isoformat()
        self._redis.hset(
            self._key("agents", agent_id),
            mapping={k: json.dumps(v) if isinstance(v, (dict, list)) else str(v) for k, v in agent_info.items()},
        )
        self._redis.sadd(self._key("agent_ids"), agent_id)

    def heartbeat(self, agent_id: str) -> None:
        """Update agent heartbeat timestamp."""
        self._redis.hset(
            self._key("agents", agent_id),
            "last_heartbeat",
            datetime.now(timezone.utc).isoformat(),
        )

    def get_active_agents(self) -> list[dict]:
        """Get all registered agents."""
        agent_ids = self._redis.smembers(self._key("agent_ids"))
        agents = []
        for aid in agent_ids:
            data = self._redis.hgetall(self._key("agents", aid))
            if data:
                result = {"agent_id": aid}
                for k, v in data.items():
                    try:
                        result[k] = json.loads(v)
                    except (json.JSONDecodeError, TypeError):
                        result[k] = v
                agents.append(result)
        return agents

    # ─────────────────────────────────────────────
    # Shared Context (Key-Value)
    # ─────────────────────────────────────────────

    def set_context(self, key: str, value: Any) -> None:
        """Store a shared context value."""
        self._redis.set(self._key("context", key), json.dumps(value))

    def get_context(self, key: str) -> Any:
        """Retrieve a shared context value."""
        data = self._redis.get(self._key("context", key))
        if data:
            return json.loads(data)
        return None

    def clear_all(self) -> None:
        """Clear all FlowForge data from Redis. Use with caution!"""
        keys = self._redis.keys(f"{self.KEY_PREFIX}*")
        if keys:
            self._redis.delete(*keys)
        logger.warning("All FlowForge memory cleared!")
