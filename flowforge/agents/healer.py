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
from flowforge.servers.flink_mcp import list_jobs, get_job_details, get_job_exceptions, cancel_job, submit_sql, create_savepoint
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
        self.register_tool("flink_create_savepoint", create_savepoint)
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

    # Type mapping reused across heal methods
    _TYPE_MAP = {
        "integer": "INT",
        "bigint": "BIGINT",
        "smallint": "SMALLINT",
        "character varying": "STRING",
        "varchar": "STRING",
        "text": "STRING",
        "numeric": "DECIMAL(10,2)",
        "decimal": "DECIMAL(10,2)",
        "double precision": "DOUBLE",
        "real": "FLOAT",
        "boolean": "BOOLEAN",
        "timestamp without time zone": "TIMESTAMP(3)",
        "timestamp with time zone": "TIMESTAMP_LTZ(3)",
        "date": "DATE",
        "json": "STRING",
        "jsonb": "STRING",
    }

    async def _check_known_resolution(self, incident_type: str, context: dict) -> dict | None:
        """Look up a known high-confidence resolution before calling the LLM."""
        return self.memory.get_best_resolution(incident_type, min_success_count=3)

    async def _heal_schema_drift(self, task: AgentTask) -> dict:
        """Auto-heal schema drift by executing ALTER TABLE statements and restarting jobs."""
        drift_info = task.context.get("drift_info", {})
        table_name = task.context.get("table_name", "orders")
        changes = drift_info.get("changes", {})
        pipeline_id = task.context.get("pipeline_id", f"pipeline-{table_name}")

        # Check memory for a known resolution pattern first
        known = await self._check_known_resolution("schema_drift", {"table": table_name})
        if known:
            logger.info(f"[{self.agent_id}] Using cached schema drift resolution (success_rate={known['success_rate']})")

        healing_actions = []
        executed_sqls = []

        # Fetch current source schema to get accurate type info
        schema_json = await self.call_tool("pg_inspect_schema", table_name=table_name)
        schema = json.loads(schema_json)
        col_map = {col["column_name"]: col for col in schema.get("columns", [])}

        # Handle added columns — safe to auto-execute
        for col_name in changes.get("columns_added", []):
            col_info = col_map.get(col_name)
            flink_type = self._TYPE_MAP.get(
                (col_info or {}).get("data_type", "text"), "STRING"
            )
            iceberg_alter_sql = (
                f"ALTER TABLE iceberg_{table_name} ADD COLUMN {col_name} {flink_type}"
            )

            # Execute via Flink SQL (Iceberg table evolution)
            try:
                alter_result_json = await self.call_tool("flink_submit_sql", sql_statement=iceberg_alter_sql)
                alter_result = json.loads(alter_result_json)
                executed = True
                executed_sqls.append(iceberg_alter_sql)
            except Exception as e:
                alter_result = {"error": str(e)}
                executed = False

            healing_actions.append({
                "action": "add_column",
                "column": col_name,
                "source_type": (col_info or {}).get("data_type", "unknown"),
                "target_type": flink_type,
                "sql": iceberg_alter_sql,
                "executed": executed,
                "result": alter_result,
            })

        # Handle removed columns — flag for human review, do not auto-drop
        for col_name in changes.get("columns_removed", []):
            healing_actions.append({
                "action": "column_removed_warning",
                "column": col_name,
                "message": f"Column '{col_name}' removed from source. Downstream table preserved.",
                "requires_human_review": True,
                "executed": False,
            })

        # Handle type changes — require human review
        for mod in changes.get("columns_modified", []):
            healing_actions.append({
                "action": "type_change_warning",
                "column": mod.get("column", ""),
                "old_type": mod.get("old_type", ""),
                "new_type": mod.get("new_type", ""),
                "message": "Column type changed. Requires human review for safe migration.",
                "requires_human_review": True,
                "executed": False,
            })

        auto_healed = [a for a in healing_actions if not a.get("requires_human_review")]
        needs_review = [a for a in healing_actions if a.get("requires_human_review")]
        success = len(auto_healed) > 0

        # Log event
        self.memory.log_event({
            "agent_id": self.agent_id,
            "agent_role": self.role.value,
            "event_type": "SCHEMA_HEALED",
            "pipeline_id": pipeline_id,
            "details": {
                "table": table_name,
                "columns_healed": [a["column"] for a in auto_healed],
                "columns_flagged": [a["column"] for a in needs_review],
            },
        })

        # Store incident pattern for future learning
        self.memory.store_incident(
            incident_type="schema_drift",
            context={"table": table_name, "changes": changes},
            resolution={"actions": [a["action"] for a in auto_healed], "sqls": executed_sqls},
            success=success,
        )

        return {
            "status": "schema_healed" if auto_healed else "no_safe_actions",
            "table": table_name,
            "drift_info": drift_info,
            "healing_actions": healing_actions,
            "auto_healed": len(auto_healed),
            "requires_review": len(needs_review),
        }

    async def _restart_job(self, task: AgentTask) -> dict:
        """Restart a failed Flink job with savepoint, then resubmit stored SQL."""
        job_id = task.context.get("job_id")
        pipeline_id = task.context.get("pipeline_id")

        if not job_id:
            return {"error": "No job_id provided for restart"}

        # Check memory for a known restart pattern
        known = await self._check_known_resolution("job_failure", {"job_id": job_id})
        if known:
            logger.info(f"[{self.agent_id}] Using cached job restart resolution")

        # Step 1: Trigger savepoint before canceling
        savepoint_path = None
        try:
            sp_result = json.loads(await self.call_tool("flink_create_savepoint", job_id=job_id))
            savepoint_path = sp_result.get("savepoint_path")
            logger.info(f"[{self.agent_id}] Savepoint triggered at: {savepoint_path}")
        except Exception as e:
            logger.warning(f"[{self.agent_id}] Savepoint failed (will restart without it): {e}")

        # Step 2: Cancel the job
        cancel_result = json.loads(await self.call_tool("flink_cancel_job", job_id=job_id))

        # Step 3: Retrieve stored SQL from Redis pipeline state
        insert_sql = None
        if pipeline_id:
            pipeline_state = self.memory.get_pipeline_state(pipeline_id)
            if pipeline_state:
                insert_sql = pipeline_state.get("insert_sql")

        # Step 4: Resubmit the streaming job
        new_job_ref = None
        resubmit_result = None
        if insert_sql:
            resubmit_result = json.loads(await self.call_tool("flink_submit_sql", sql_statement=insert_sql))
            new_job_ref = resubmit_result.get("operation_handle") or resubmit_result.get("session_id")

            # Update pipeline state with new job reference
            if pipeline_id and new_job_ref:
                self.memory.set_pipeline_state(pipeline_id, {
                    "status": "running",
                    "flink_job_ref": new_job_ref,
                    "previous_job_id": job_id,
                    "restarted_from_savepoint": savepoint_path is not None,
                })

        # Log event
        self.memory.log_event({
            "agent_id": self.agent_id,
            "agent_role": self.role.value,
            "event_type": "JOB_RESTARTED",
            "pipeline_id": pipeline_id,
            "details": {
                "original_job_id": job_id,
                "new_job_ref": new_job_ref,
                "savepoint_used": savepoint_path is not None,
                "savepoint_path": savepoint_path,
            },
        })

        # Store pattern
        self.memory.store_incident(
            incident_type="job_failure",
            context={"job_id": job_id, "pipeline_id": pipeline_id},
            resolution={"action": "savepoint_restart", "savepoint": savepoint_path is not None},
            success=new_job_ref is not None,
        )

        return {
            "status": "restarted" if new_job_ref else "canceled_only",
            "original_job_id": job_id,
            "new_job_ref": new_job_ref,
            "savepoint_path": savepoint_path,
            "cancel_result": cancel_result,
            "resubmit_result": resubmit_result,
            "message": (
                f"Job restarted successfully (new ref: {new_job_ref})"
                if new_job_ref else
                "Job canceled. No stored SQL found — use Builder agent to redeploy."
            ),
        }

    async def _auto_diagnose_and_heal(self, task: AgentTask) -> dict:
        """Diagnose issues and attempt autonomous healing using memory + LLM."""
        diagnosis = await self._diagnose_failure(task)

        # Check if we have a known resolution before calling LLM
        for incident_type in ("job_failure", "schema_drift"):
            known = await self._check_known_resolution(incident_type, task.context)
            if known:
                logger.info(f"[{self.agent_id}] Applying known resolution for {incident_type}")
                if incident_type == "schema_drift":
                    return await self._heal_schema_drift(task)
                elif incident_type == "job_failure":
                    return await self._restart_job(task)

        # Fall back to LLM for novel situations
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

        # Try to act on the LLM plan
        try:
            plan_dict = json.loads(plan) if isinstance(plan, str) else plan
            action = plan_dict.get("action", "escalate")
            if action == "heal_schema_drift":
                return await self._heal_schema_drift(task)
            elif action == "restart_job":
                return await self._restart_job(task)
        except (json.JSONDecodeError, TypeError):
            pass

        return {
            "diagnosis": diagnosis,
            "healing_plan": plan,
            "status": "escalated",
        }
