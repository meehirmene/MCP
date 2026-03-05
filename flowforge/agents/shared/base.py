"""
FlowForge Agent Base Class

Defines the contract for all specialist agents in the multi-agent system.
Each agent has a role, a set of capabilities, access to MCP tools, and shared memory.
"""

import json
import logging
import uuid
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field

from flowforge.agents.shared.llm_client import LLMClient

logger = logging.getLogger(__name__)


class AgentRole(str, Enum):
    ORCHESTRATOR = "orchestrator"
    BUILDER = "builder"
    MONITOR = "monitor"
    HEALER = "healer"
    OPTIMIZER = "optimizer"
    QUALITY = "quality"
    MIGRATION = "migration"


class TaskStatus(str, Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    ESCALATED = "escalated"


class AgentTask(BaseModel):
    """A task assigned to an agent."""
    task_id: str = Field(default_factory=lambda: uuid.uuid4().hex[:12])
    description: str
    assigned_to: AgentRole
    assigned_by: AgentRole | None = None
    status: TaskStatus = TaskStatus.PENDING
    priority: int = 5  # 1 (highest) to 10 (lowest)
    context: dict = Field(default_factory=dict)
    result: dict | None = None
    error: str | None = None
    created_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    completed_at: str | None = None
    parent_task_id: str | None = None  # For sub-task tracking
    depends_on: list[str] = Field(default_factory=list)


class AgentEvent(BaseModel):
    """An event emitted by an agent for inter-agent communication."""
    event_id: str = Field(default_factory=lambda: uuid.uuid4().hex[:12])
    agent_id: str
    agent_role: AgentRole
    event_type: str  # TASK_COMPLETED, ANOMALY_DETECTED, SCHEMA_DRIFT, etc.
    severity: str = "INFO"  # INFO, WARNING, HIGH, CRITICAL
    pipeline_id: str | None = None
    details: dict = Field(default_factory=dict)
    recommended_action: str | None = None
    target_agent: AgentRole | None = None
    timestamp: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class AgentCapability(BaseModel):
    """Describes what an agent can do."""
    name: str
    description: str
    tools_required: list[str] = Field(default_factory=list)


class BaseAgent(ABC):
    """
    Base class for all FlowForge agents.

    Every agent has:
    - A role (orchestrator, builder, monitor, etc.)
    - A set of capabilities
    - Access to an LLM for reasoning
    - Access to MCP tools for executing actions
    - Shared memory for coordination
    """

    def __init__(
        self,
        role: AgentRole,
        agent_id: str | None = None,
        llm_client: LLMClient | None = None,
    ):
        self.role = role
        self.agent_id = agent_id or f"{role.value}-{uuid.uuid4().hex[:6]}"
        self.llm = llm_client or LLMClient()
        self.tools: dict[str, callable] = {}
        self.capabilities: list[AgentCapability] = []
        self.task_history: list[AgentTask] = []
        self._setup()

    @abstractmethod
    def _setup(self):
        """Register tools and capabilities for this agent."""
        pass

    @abstractmethod
    async def execute_task(self, task: AgentTask) -> AgentTask:
        """Execute a task and return the updated task with results."""
        pass

    def get_system_prompt(self) -> str:
        """Generate the system prompt for this agent's LLM calls."""
        capabilities_str = "\n".join(
            f"- {cap.name}: {cap.description}" for cap in self.capabilities
        )
        tools_str = "\n".join(
            f"- {name}: available" for name in self.tools.keys()
        )
        return f"""You are the {self.role.value.title()} Agent in the FlowForge multi-agent data pipeline platform.

Your role: {self._get_role_description()}

Your capabilities:
{capabilities_str}

Available tools:
{tools_str}

Guidelines:
- Be precise and actionable in your responses
- Always explain what you're doing and why
- If you encounter an error, diagnose it before attempting a fix
- Report your findings in structured JSON format
- If a task is outside your capabilities, recommend which agent should handle it
"""

    def _get_role_description(self) -> str:
        descriptions = {
            AgentRole.ORCHESTRATOR: "Decompose user requests into sub-tasks, delegate to specialist agents, and synthesize results.",
            AgentRole.BUILDER: "Create and deploy data pipelines — Kafka topics, Flink jobs, Iceberg tables, CDC connectors.",
            AgentRole.MONITOR: "Watch pipeline health — throughput, latency, errors, SLAs. Detect anomalies.",
            AgentRole.HEALER: "Fix pipeline failures — schema drift, job crashes, data skew, backpressure.",
            AgentRole.OPTIMIZER: "Tune pipeline performance — parallelism, partitioning, compaction, watermarks.",
            AgentRole.QUALITY: "Validate data quality — completeness, freshness, accuracy, consistency.",
            AgentRole.MIGRATION: "Handle schema changes — evolve tables, migrate data, backfill.",
        }
        return descriptions.get(self.role, "Unknown role")

    def register_tool(self, name: str, func: callable, description: str = ""):
        """Register a tool that this agent can use."""
        self.tools[name] = func
        logger.info(f"[{self.agent_id}] Registered tool: {name}")

    async def call_tool(self, tool_name: str, **kwargs) -> Any:
        """Execute a registered tool."""
        if tool_name not in self.tools:
            raise ValueError(f"Tool '{tool_name}' not registered for agent {self.agent_id}")

        logger.info(f"[{self.agent_id}] Calling tool: {tool_name} with args: {kwargs}")
        tool_func = self.tools[tool_name]

        import asyncio
        if asyncio.iscoroutinefunction(tool_func):
            result = await tool_func(**kwargs)
        else:
            result = tool_func(**kwargs)

        logger.info(f"[{self.agent_id}] Tool {tool_name} completed")
        return result

    async def reason(self, prompt: str, context: dict | None = None) -> str:
        """Use the LLM to reason about a problem."""
        messages = [
            {"role": "system", "content": self.get_system_prompt()},
        ]
        if context:
            messages.append({
                "role": "user",
                "content": f"Context:\n```json\n{json.dumps(context, indent=2)}\n```\n\n{prompt}",
            })
        else:
            messages.append({"role": "user", "content": prompt})

        response = self.llm.chat(messages)
        return response.get("content", "")

    def emit_event(self, event_type: str, details: dict, severity: str = "INFO",
                   pipeline_id: str | None = None, target_agent: AgentRole | None = None) -> AgentEvent:
        """Create an agent event for inter-agent communication."""
        event = AgentEvent(
            agent_id=self.agent_id,
            agent_role=self.role,
            event_type=event_type,
            severity=severity,
            pipeline_id=pipeline_id,
            details=details,
            target_agent=target_agent,
        )
        logger.info(f"[{self.agent_id}] Emitting event: {event_type} (severity={severity})")
        return event

    def to_dict(self) -> dict:
        """Serialize agent info for the registry."""
        return {
            "agent_id": self.agent_id,
            "role": self.role.value,
            "capabilities": [cap.model_dump() for cap in self.capabilities],
            "tools": list(self.tools.keys()),
            "tasks_completed": len([t for t in self.task_history if t.status == TaskStatus.COMPLETED]),
            "tasks_failed": len([t for t in self.task_history if t.status == TaskStatus.FAILED]),
        }
