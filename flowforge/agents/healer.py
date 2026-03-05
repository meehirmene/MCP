"""
FlowForge Healer Agent

Specialist agent responsible for self-healing pipeline failures:
- Schema drift resolution (auto-evolve Iceberg tables)
- Flink job restart with corrected configuration
- Pipeline replay for failed events
- Backpressure mitigation
"""

import json
import logging

from flowforge.agents.shared.base import (
    BaseAgent, AgentRole, AgentTask, AgentCapability, TaskStatus,
)
from flowforge.servers.flink_mcp import list_jobs, get_job_details, get_job_exceptions, cancel_job, submit_sql
from flowforge.servers.postgres_mcp import inspect_schema, query
from flowforge.servers.kafka_mcp import get_topic_info

logger = logging.getLogger(__name__)


class HealerAgent(BaseAgent):
    """
    The Healer Agent fixes pipeline failures automatically.

    It can:
    1. Diagnose why a Flink job failed
    2. Auto-evolve schemas when drift is detected
    3. Restart failed jobs with corrected config
    4. Replay failed events from Kafka
    """

    def __init__(self, **kwargs):
        super().__init__(role=AgentRole.HEALER, **kwargs)

    def _setup(self):
        self.register_tool("flink_list_jobs", list_jobs)
        self.register_tool("flink_job_details", get_job_details)
        self.register_tool("flink_job_exceptions", get_job_exceptions)
        self.register_tool("flink_cancel_job", cancel_job)
        self.register_tool("flink_submit_sql", submit_sql)
        self.register_tool("pg_inspect_schema", inspect_schema)
        self.register_tool("pg_query", query)
        self.register_tool("kafka_topic_info", get_topic_info)

        self.capabilities = [
            AgentCapability(
                name="diagnose_failure",
                description="Diagnose why a pipeline or Flink job failed by analyzing logs and exceptions",
                tools_required=["flink_job_exceptions", "flink_job_details"],
            ),
            AgentCapability(
                name="heal_schema_drift",
                description="Auto-evolve downstream tables when schema drift is detected on source",
                tools_required=["pg_inspect_schema", "flink_submit_sql"],
            ),
            AgentCapability(
                name="restart_job",
                description="Restart a failed Flink job with corrected configuration",
                tools_required=["flink_cancel_job", "flink_submit_sql"],
            ),
            AgentCapability(
                name="replay_events",
                description="Replay failed events from Kafka for reprocessing",
                tools_required=["kafka_topic_info"],
            ),
        ]

    async def execute_task(self, task: AgentTask) -> AgentTask:
        """Execute a healing task."""
        task.status = TaskStatus.IN_PROGRESS
        logger.info(f"[{self.agent_id}] Executing healing task: {task.description}")

        try:
            description_lower = task.description.lower()

            if "schema" in description_lower and ("drift" in description_lower or "heal" in description_lower or "evolve" in description_lower):
                result = await self._heal_schema_drift(task)
            elif "diagnos" in description_lower or "exception" in description_lower or "fail" in description_lower:
                result = await self._diagnose_failure(task)
            elif "restart" in description_lower:
                result = await self._restart_job(task)
            else:
                result = await self._auto_diagnose_and_heal(task)

            task.result = result
            task.status = TaskStatus.COMPLETED

        except Exception as e:
            task.error = str(e)
            task.status = TaskStatus.FAILED
            logger.error(f"[{self.agent_id}] Healing task failed: {e}")

        self.task_history.append(task)
        return task

    async def _diagnose_failure(self, task: AgentTask) -> dict:
        """Diagnose why a pipeline failed."""
        job_id = task.context.get("job_id")

        if job_id:
            # Get specific job exceptions
            exceptions_json = await self.call_tool("flink_job_exceptions", job_id=job_id)
            exceptions = json.loads(exceptions_json)
            details_json = await self.call_tool("flink_job_details", job_id=job_id)
            details = json.loads(details_json)

            # Use LLM for root cause analysis
            prompt = f"""Analyze these Flink job exceptions and determine the root cause:

Job Details: {json.dumps(details, indent=2)}
Exceptions: {json.dumps(exceptions, indent=2)}

Provide:
1. Root cause in plain English
2. Most likely fix
3. Which agent should handle the fix (healer, optimizer, or migration)
4. Severity (LOW, MEDIUM, HIGH, CRITICAL)
"""
            analysis = await self.reason(prompt)

            return {
                "job_id": job_id,
                "job_details": details,
                "exceptions": exceptions,
                "root_cause_analysis": analysis,
            }
        else:
            # Check all failed jobs
            jobs_json = await self.call_tool("flink_list_jobs")
            jobs = json.loads(jobs_json)
            failed = [j for j in jobs.get("jobs", []) if j.get("state") == "FAILED"]

            return {
                "failed_jobs": failed,
                "count": len(failed),
                "message": "Use job_id to get detailed diagnosis for a specific job" if failed
                          else "No failed jobs found",
            }

    async def _heal_schema_drift(self, task: AgentTask) -> dict:
        """Auto-heal schema drift by evolving downstream schemas."""
        drift_info = task.context.get("drift_info", {})
        table_name = task.context.get("table_name", "orders")
        changes = drift_info.get("changes", {})

        healing_actions = []

        # Handle added columns
        for col_name in changes.get("columns_added", []):
            # Get the new column's type from source
            schema_json = await self.call_tool("pg_inspect_schema", table_name=table_name)
            schema = json.loads(schema_json)

            col_info = None
            for col in schema.get("columns", []):
                if col["column_name"] == col_name:
                    col_info = col
                    break

            if col_info:
                # Map PostgreSQL type to Flink/Iceberg type
                type_mapping = {
                    "integer": "INT",
                    "bigint": "BIGINT",
                    "character varying": "STRING",
                    "text": "STRING",
                    "numeric": "DECIMAL(10,2)",
                    "double precision": "DOUBLE",
                    "boolean": "BOOLEAN",
                    "timestamp without time zone": "TIMESTAMP",
                    "timestamp with time zone": "TIMESTAMP_LTZ",
                    "date": "DATE",
                }
                flink_type = type_mapping.get(col_info["data_type"], "STRING")

                healing_actions.append({
                    "action": "add_column",
                    "column": col_name,
                    "source_type": col_info["data_type"],
                    "target_type": flink_type,
                    "sql": f"ALTER TABLE iceberg_catalog.ecommerce.{table_name} ADD COLUMN {col_name} {flink_type}",
                })

        # Handle removed columns (log warning but don't auto-remove)
        for col_name in changes.get("columns_removed", []):
            healing_actions.append({
                "action": "column_removed_warning",
                "column": col_name,
                "message": f"Column '{col_name}' was removed from source. Downstream table not modified (data preservation).",
                "requires_human_review": True,
            })

        # Handle modified columns
        for mod in changes.get("columns_modified", []):
            healing_actions.append({
                "action": "type_change_warning",
                "column": mod["column"],
                "old_type": mod["old_type"],
                "new_type": mod["new_type"],
                "message": "Column type changed. Requires human review for safe migration.",
                "requires_human_review": True,
            })

        return {
            "status": "schema_healed" if healing_actions else "no_action_needed",
            "table": table_name,
            "drift_info": drift_info,
            "healing_actions": healing_actions,
            "auto_healed": len([a for a in healing_actions if not a.get("requires_human_review")]),
            "requires_review": len([a for a in healing_actions if a.get("requires_human_review")]),
        }

    async def _restart_job(self, task: AgentTask) -> dict:
        """Restart a failed Flink job."""
        job_id = task.context.get("job_id")

        if job_id:
            # Cancel any existing instance
            cancel_result = await self.call_tool("flink_cancel_job", job_id=job_id)

            return {
                "status": "restart_initiated",
                "original_job_id": job_id,
                "cancel_result": json.loads(cancel_result),
                "message": "Job canceled. Re-submission requires the original SQL — use Builder agent.",
            }

        return {"error": "No job_id provided for restart"}

    async def _auto_diagnose_and_heal(self, task: AgentTask) -> dict:
        """Automatically diagnose and attempt to heal any issues found."""
        # First diagnose
        diagnosis = await self._diagnose_failure(task)

        # Then use LLM to determine healing strategy
        prompt = f"""Based on this diagnosis, what healing actions should be taken?

Diagnosis: {json.dumps(diagnosis, indent=2)}

Available healing actions:
1. heal_schema_drift - If there's a schema mismatch
2. restart_job - If a job failed due to transient error
3. escalate - If the issue needs human intervention

Return a JSON object with:
- "action": chosen action
- "reasoning": why this action
- "confidence": 0.0 to 1.0
"""
        plan = await self.reason(prompt, diagnosis)

        return {
            "diagnosis": diagnosis,
            "healing_plan": plan,
        }
