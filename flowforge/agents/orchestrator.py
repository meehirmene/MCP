"""
FlowForge Orchestrator Agent

The central coordinator that:
1. Receives natural language requests from users
2. Decomposes them into sub-tasks
3. Delegates to specialist agents (Builder, Monitor, Healer, etc.)
4. Synthesizes results into a coherent response
5. Handles conflict resolution between agents
"""

import json
import logging
from datetime import datetime, timezone

from flowforge.agents.shared.base import (
    BaseAgent, AgentRole, AgentTask, AgentCapability, TaskStatus,
)
from flowforge.agents.shared.memory import AgentMemory
from flowforge.agents.builder import BuilderAgent
from flowforge.agents.monitor import MonitorAgent
from flowforge.agents.healer import HealerAgent

logger = logging.getLogger(__name__)


class OrchestratorAgent(BaseAgent):
    """
    The Orchestrator Agent is the brain of the multi-agent system.

    It follows the Supervisory Hierarchy pattern:
    - Receives user requests in natural language
    - Decomposes them into ordered sub-tasks with dependencies
    - Routes sub-tasks to the best specialist agent
    - Tracks progress and synthesizes final results
    - Handles inter-agent conflicts and escalations
    """

    def __init__(self, **kwargs):
        super().__init__(role=AgentRole.ORCHESTRATOR, **kwargs)
        self.memory = AgentMemory()

        # Initialize specialist agents
        self.specialists: dict[AgentRole, BaseAgent] = {}
        self._init_specialists()

    def _init_specialists(self):
        """Initialize all specialist agents."""
        try:
            self.specialists[AgentRole.BUILDER] = BuilderAgent()
            self.specialists[AgentRole.MONITOR] = MonitorAgent()
            self.specialists[AgentRole.HEALER] = HealerAgent()

            # Register agents in shared memory
            for role, agent in self.specialists.items():
                self.memory.register_agent(agent.agent_id, agent.to_dict())
                logger.info(f"[{self.agent_id}] Registered specialist: {role.value} ({agent.agent_id})")

        except Exception as e:
            logger.warning(f"[{self.agent_id}] Could not initialize all specialists: {e}")

    def _setup(self):
        self.capabilities = [
            AgentCapability(
                name="decompose_request",
                description="Break down a natural language request into ordered sub-tasks",
            ),
            AgentCapability(
                name="delegate_task",
                description="Assign a sub-task to the best specialist agent",
            ),
            AgentCapability(
                name="synthesize_results",
                description="Combine results from multiple agents into a coherent response",
            ),
            AgentCapability(
                name="conflict_resolution",
                description="Resolve conflicts between agent recommendations",
            ),
        ]

    async def handle_user_request(self, request: str) -> dict:
        """
        Main entry point — handle a natural language user request.

        This is the "wow demo" flow:
        1. User says something like "Stream orders from Postgres to Iceberg, aggregate by region"
        2. Orchestrator decomposes into tasks
        3. Delegates to Builder, Monitor, Quality
        4. Returns a synthesized response
        """
        logger.info(f"[{self.agent_id}] Received user request: {request}")

        # Log the request event
        self.memory.log_event({
            "agent_id": self.agent_id,
            "agent_role": self.role.value,
            "event_type": "USER_REQUEST_RECEIVED",
            "details": {"request": request},
        })

        # Step 1: Decompose the request into sub-tasks
        tasks = await self._decompose_request(request)

        # Step 2: Execute tasks in dependency order
        results = {}
        for task in tasks:
            # Check dependencies
            deps_met = all(
                results.get(dep, {}).get("status") == "completed"
                for dep in task.depends_on
            )

            if not deps_met:
                task.status = TaskStatus.FAILED
                task.error = "Dependencies not met"
                results[task.task_id] = {"status": "failed", "error": task.error}
                continue

            # Pass relevant context from completed dependencies
            for dep_id in task.depends_on:
                dep_result = results.get(dep_id, {})
                task.context.update(dep_result.get("context_forward", {}))

            # Delegate to specialist
            completed_task = await self._delegate_task(task)
            results[task.task_id] = {
                "status": completed_task.status.value,
                "result": completed_task.result,
                "error": completed_task.error,
                "agent": task.assigned_to.value,
                "description": task.description,
            }

            # Log event
            self.memory.log_event({
                "agent_id": self.agent_id,
                "agent_role": self.role.value,
                "event_type": f"TASK_{completed_task.status.value.upper()}",
                "details": {
                    "task_id": task.task_id,
                    "task": task.description,
                    "assigned_to": task.assigned_to.value,
                },
            })

        # Step 3: Synthesize response
        response = await self._synthesize_results(request, tasks, results)

        return {
            "request": request,
            "tasks_executed": len(tasks),
            "tasks_completed": len([t for t in tasks if t.status == TaskStatus.COMPLETED]),
            "tasks_failed": len([t for t in tasks if t.status == TaskStatus.FAILED]),
            "results": results,
            "summary": response,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    async def _decompose_request(self, request: str) -> list[AgentTask]:
        """Use LLM to decompose a user request into ordered sub-tasks."""
        prompt = f"""You are the Orchestrator of a multi-agent data pipeline platform called FlowForge.

Decompose this user request into specific, actionable sub-tasks:

REQUEST: "{request}"

Available specialist agents:
- BUILDER: Creates pipelines — Kafka topics, Flink jobs, CDC setup, Iceberg tables
- MONITOR: Watches pipeline health — throughput, errors, schema drift, SLAs
- HEALER: Fixes failures — schema drift resolution, job restarts, replays

For each sub-task, specify:
- description: What needs to be done
- agent: Which specialist handles it (builder, monitor, or healer)
- depends_on: List of task indices (0-based) this task depends on
- context: Any parameters needed (table names, topic names, etc.)
- priority: 1 (highest) to 10 (lowest)

Return a JSON array of tasks in execution order. Example:
[
  {{"description": "Inspect PostgreSQL orders table schema", "agent": "builder", "depends_on": [], "context": {{"table_name": "orders"}}, "priority": 1}},
  {{"description": "Create Kafka topic for CDC events", "agent": "builder", "depends_on": [0], "context": {{"topic_name": "cdc.ecommerce.orders"}}, "priority": 2}},
  {{"description": "Start monitoring pipeline health", "agent": "monitor", "depends_on": [1], "context": {{}}, "priority": 3}}
]

Return ONLY the JSON array, no additional text."""

        response = await self.reason(prompt)

        # Parse the LLM response
        try:
            # Try to extract JSON from the response
            response_clean = response.strip()
            if response_clean.startswith("```"):
                # Strip markdown code fences
                lines = response_clean.split("\n")
                response_clean = "\n".join(lines[1:-1])

            task_defs = json.loads(response_clean)
        except json.JSONDecodeError:
            # Fallback: create a default pipeline build task
            logger.warning(f"[{self.agent_id}] Could not parse LLM task decomposition, using defaults")
            task_defs = [
                {"description": "Inspect source databases", "agent": "builder", "depends_on": [], "context": {}, "priority": 1},
                {"description": request, "agent": "builder", "depends_on": [0], "context": {}, "priority": 2},
                {"description": "Run health check on pipeline components", "agent": "monitor", "depends_on": [1], "context": {}, "priority": 3},
            ]

        # Convert to AgentTask objects
        tasks = []
        task_id_map = {}  # index → task_id

        for i, td in enumerate(task_defs):
            agent_role = {
                "builder": AgentRole.BUILDER,
                "monitor": AgentRole.MONITOR,
                "healer": AgentRole.HEALER,
            }.get(td.get("agent", "builder"), AgentRole.BUILDER)

            task = AgentTask(
                description=td["description"],
                assigned_to=agent_role,
                assigned_by=AgentRole.ORCHESTRATOR,
                priority=td.get("priority", 5),
                context=td.get("context", {}),
                depends_on=[task_id_map[dep] for dep in td.get("depends_on", []) if dep in task_id_map],
            )
            task_id_map[i] = task.task_id
            tasks.append(task)

        logger.info(f"[{self.agent_id}] Decomposed request into {len(tasks)} tasks")
        return tasks

    async def _delegate_task(self, task: AgentTask) -> AgentTask:
        """Delegate a task to the appropriate specialist agent."""
        agent = self.specialists.get(task.assigned_to)

        if not agent:
            task.status = TaskStatus.FAILED
            task.error = f"No specialist agent for role: {task.assigned_to.value}"
            return task

        logger.info(f"[{self.agent_id}] Delegating to {task.assigned_to.value}: {task.description}")
        return await agent.execute_task(task)

    async def _synthesize_results(self, request: str, tasks: list[AgentTask], results: dict) -> str:
        """Use LLM to synthesize task results into a coherent user response."""
        # Build a summary of what happened
        task_summaries = []
        for task in tasks:
            result = results.get(task.task_id, {})
            task_summaries.append({
                "task": task.description,
                "agent": task.assigned_to.value,
                "status": result.get("status", "unknown"),
                "has_result": result.get("result") is not None,
            })

        prompt = f"""Summarize the results of this multi-agent pipeline operation for the user.

Original Request: "{request}"

Tasks Executed:
{json.dumps(task_summaries, indent=2)}

Write a clear, concise summary (2-4 sentences) explaining:
1. What was accomplished
2. Any issues encountered
3. Current status of the pipeline
4. Next steps if needed

Be conversational and helpful. Use technical terms where appropriate but explain them."""

        summary = await self.reason(prompt, {"tasks": task_summaries})
        return summary

    async def execute_task(self, task: AgentTask) -> AgentTask:
        """Execute a task assigned to the orchestrator itself."""
        task.status = TaskStatus.IN_PROGRESS

        try:
            result = await self.handle_user_request(task.description)
            task.result = result
            task.status = TaskStatus.COMPLETED
        except Exception as e:
            task.error = str(e)
            task.status = TaskStatus.FAILED

        self.task_history.append(task)
        return task

    async def handle_agent_event(self, event: dict) -> dict:
        """Handle an event from a specialist agent (e.g., anomaly detected)."""
        event_type = event.get("event_type", "")
        severity = event.get("severity", "INFO")

        logger.info(f"[{self.agent_id}] Received agent event: {event_type} (severity={severity})")

        self.memory.log_event(event)

        # Auto-respond to critical events
        if severity in ("HIGH", "CRITICAL"):
            if "SCHEMA_DRIFT" in event_type:
                # Route to Healer
                heal_task = AgentTask(
                    description=f"Heal schema drift: {event.get('details', {})}",
                    assigned_to=AgentRole.HEALER,
                    assigned_by=AgentRole.ORCHESTRATOR,
                    priority=1,
                    context=event.get("details", {}),
                )
                result = await self._delegate_task(heal_task)
                return {"action": "auto_healed", "task": result.result}

            elif "ANOMALY" in event_type:
                # Diagnose with Healer
                diag_task = AgentTask(
                    description=f"Diagnose anomaly: {event.get('details', {})}",
                    assigned_to=AgentRole.HEALER,
                    assigned_by=AgentRole.ORCHESTRATOR,
                    priority=2,
                    context=event.get("details", {}),
                )
                result = await self._delegate_task(diag_task)
                return {"action": "diagnosed", "task": result.result}

        return {"action": "logged", "event": event}
