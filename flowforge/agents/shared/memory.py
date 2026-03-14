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

    # ─────────────────────────────────────────────
    # Incident Memory & Pattern Learning
    # ─────────────────────────────────────────────

    def store_incident(self, incident_type: str, context: dict, resolution: dict, success: bool) -> str:
        """Store an incident and its resolution outcome for agent learning.

        Returns the resolution_key (hash) for future reference.
        """
        import hashlib
        resolution_key = hashlib.md5(json.dumps(resolution, sort_keys=True).encode()).hexdigest()[:8]

        incident = {
            "incident_type": incident_type,
            "context": context,
            "resolution": resolution,
            "resolution_key": resolution_key,
            "success": success,
            "occurred_at": datetime.now(timezone.utc).isoformat(),
        }

        # Store incident log — capped at 50 per type
        list_key = self._key("incidents", incident_type)
        self._redis.lpush(list_key, json.dumps(incident))
        self._redis.ltrim(list_key, 0, 49)

        # Update or create the pattern record
        pattern_key = self._key("patterns", incident_type, resolution_key)
        now = datetime.now(timezone.utc).isoformat()

        if self._redis.exists(pattern_key):
            if success:
                self._redis.hincrby(pattern_key, "success_count", 1)
            else:
                self._redis.hincrby(pattern_key, "failure_count", 1)
            self._redis.hset(pattern_key, "last_used", now)
        else:
            self._redis.hset(pattern_key, mapping={
                "incident_type": incident_type,
                "resolution": json.dumps(resolution),
                "resolution_key": resolution_key,
                "success_count": str(1 if success else 0),
                "failure_count": str(0 if success else 1),
                "last_used": now,
                "created_at": now,
            })

        # Update global learnings sorted set (score = success_count)
        success_count = int(self._redis.hget(pattern_key, "success_count") or 0)
        self._redis.zadd(
            self._key("agent_learnings"),
            {f"{incident_type}:{resolution_key}": success_count},
        )

        logger.info(f"Incident stored: {incident_type} (success={success}, key={resolution_key})")
        return resolution_key

    def get_best_resolution(self, incident_type: str, min_success_count: int = 3) -> dict | None:
        """Get the highest-confidence resolution for a given incident type.

        Returns None if no pattern meets the minimum success threshold.
        """
        pattern_keys = self._redis.keys(self._key("patterns", incident_type, "*"))
        best: dict | None = None
        best_score = -1.0

        for key in pattern_keys:
            data = self._redis.hgetall(key)
            if not data:
                continue
            success_count = int(data.get("success_count", 0))
            failure_count = int(data.get("failure_count", 0))
            if success_count < min_success_count:
                continue
            total = success_count + failure_count
            rate = success_count / total if total > 0 else 0.0
            score = success_count * rate  # weight by both volume and success rate
            if score > best_score:
                best_score = score
                try:
                    resolution = json.loads(data.get("resolution", "{}"))
                except json.JSONDecodeError:
                    resolution = {}
                best = {
                    "incident_type": data.get("incident_type", incident_type),
                    "resolution": resolution,
                    "resolution_key": data.get("resolution_key", ""),
                    "success_count": success_count,
                    "failure_count": failure_count,
                    "success_rate": round(rate, 2),
                    "last_used": data.get("last_used", ""),
                }

        return best

    def increment_resolution_success(self, incident_type: str, resolution_key: str) -> None:
        """Record an additional successful use of a known resolution pattern."""
        pattern_key = self._key("patterns", incident_type, resolution_key)
        if self._redis.exists(pattern_key):
            self._redis.hincrby(pattern_key, "success_count", 1)
            self._redis.hset(pattern_key, "last_used", datetime.now(timezone.utc).isoformat())
            success_count = int(self._redis.hget(pattern_key, "success_count") or 0)
            self._redis.zadd(
                self._key("agent_learnings"),
                {f"{incident_type}:{resolution_key}": success_count},
            )

    # ─────────────────────────────────────────────
    # Scheduled Pipelines
    # ─────────────────────────────────────────────

    def set_schedule(self, schedule_id: str, schedule: dict) -> None:
        """Store a scheduled pipeline entry."""
        schedule = {**schedule, "updated_at": datetime.now(timezone.utc).isoformat()}
        self._redis.hset(
            self._key("schedules", schedule_id),
            mapping={
                k: json.dumps(v) if isinstance(v, (dict, list)) else str(v)
                for k, v in schedule.items()
            },
        )
        self._redis.sadd(self._key("schedule_ids"), schedule_id)

    def get_schedules(self) -> list[dict]:
        """Return all stored schedules."""
        ids = self._redis.smembers(self._key("schedule_ids"))
        result = []
        for sid in ids:
            data = self._redis.hgetall(self._key("schedules", sid))
            if data:
                parsed: dict = {"schedule_id": sid}
                for k, v in data.items():
                    try:
                        parsed[k] = json.loads(v)
                    except (json.JSONDecodeError, TypeError):
                        parsed[k] = v
                result.append(parsed)
        return result

    def delete_schedule(self, schedule_id: str) -> bool:
        """Delete a schedule. Returns True if it existed."""
        existed = self._redis.exists(self._key("schedules", schedule_id))
        self._redis.delete(self._key("schedules", schedule_id))
        self._redis.srem(self._key("schedule_ids"), schedule_id)
        return bool(existed)

    def get_all_learnings(self) -> list[dict]:
        """Return all stored incident patterns sorted by success count descending."""
        entries = self._redis.zrevrangebyscore(
            self._key("agent_learnings"), "+inf", "-inf", withscores=True, start=0, num=100,
        )
        learnings = []
        for member, score in entries:
            parts = member.split(":", 1)
            if len(parts) != 2:
                continue
            incident_type, resolution_key = parts
            pattern_key = self._key("patterns", incident_type, resolution_key)
            data = self._redis.hgetall(pattern_key)
            if not data:
                continue
            try:
                resolution = json.loads(data.get("resolution", "{}"))
            except json.JSONDecodeError:
                resolution = {}
            success_count = int(data.get("success_count", 0))
            failure_count = int(data.get("failure_count", 0))
            total = success_count + failure_count
            # Build a human-readable summary of the resolution
            summary_parts = []
            for k, v in resolution.items():
                if isinstance(v, str) and len(v) < 80:
                    summary_parts.append(f"{k}: {v}")
            resolution_summary = "; ".join(summary_parts[:3]) or str(resolution)[:80]
            learnings.append({
                "incident_type": data.get("incident_type", incident_type),
                "resolution_summary": resolution_summary,
                "resolution": resolution,
                "success_count": success_count,
                "failure_count": failure_count,
                "success_rate": round(success_count / total, 2) if total > 0 else 0.0,
                "last_used": data.get("last_used", ""),
                "created_at": data.get("created_at", ""),
            })
        return learnings
