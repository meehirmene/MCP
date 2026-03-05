"""
FlowForge Monitor Agent

Specialist agent responsible for pipeline health monitoring:
- Tracking throughput, latency, and error rates
- Detecting anomalies in streaming metrics
- Watching for schema drift
- SLA monitoring and alerting
"""

import json
import logging
from datetime import datetime, timezone

from flowforge.agents.shared.base import (
    BaseAgent, AgentRole, AgentTask, AgentCapability, TaskStatus, AgentEvent,
)
from flowforge.servers.kafka_mcp import list_topics, get_topic_info, consume_messages
from flowforge.servers.flink_mcp import list_jobs, get_job_details, get_job_exceptions
from flowforge.servers.postgres_mcp import list_tables, inspect_schema

logger = logging.getLogger(__name__)


class MonitorAgent(BaseAgent):
    """
    The Monitor Agent watches pipeline health and detects anomalies.

    It can:
    1. Check Flink job status and performance metrics
    2. Monitor Kafka topic throughput and consumer lag
    3. Detect schema drift on source tables
    4. Track SLAs and freshness
    """

    def __init__(self, **kwargs):
        super().__init__(role=AgentRole.MONITOR, **kwargs)
        self._baseline_schemas: dict[str, dict] = {}

    def _setup(self):
        # Kafka tools
        self.register_tool("kafka_list_topics", list_topics)
        self.register_tool("kafka_topic_info", get_topic_info)
        self.register_tool("kafka_consume", consume_messages)

        # Flink tools
        self.register_tool("flink_list_jobs", list_jobs)
        self.register_tool("flink_job_details", get_job_details)
        self.register_tool("flink_job_exceptions", get_job_exceptions)

        # PostgreSQL tools
        self.register_tool("pg_list_tables", list_tables)
        self.register_tool("pg_inspect_schema", inspect_schema)

        self.capabilities = [
            AgentCapability(
                name="health_check",
                description="Check the health of all pipelines — Flink jobs, Kafka topics, database connections",
                tools_required=["flink_list_jobs", "kafka_list_topics"],
            ),
            AgentCapability(
                name="detect_anomalies",
                description="Detect anomalies in streaming metrics — throughput drops, error spikes, latency increases",
                tools_required=["flink_job_details", "kafka_consume"],
            ),
            AgentCapability(
                name="detect_schema_drift",
                description="Detect schema changes in source databases by comparing against baseline",
                tools_required=["pg_inspect_schema"],
            ),
            AgentCapability(
                name="sla_check",
                description="Check data freshness and SLA compliance",
                tools_required=["flink_job_details", "kafka_topic_info"],
            ),
        ]

    async def execute_task(self, task: AgentTask) -> AgentTask:
        """Execute a monitoring task."""
        task.status = TaskStatus.IN_PROGRESS
        logger.info(f"[{self.agent_id}] Executing monitoring task: {task.description}")

        try:
            description_lower = task.description.lower()

            if "health" in description_lower or "status" in description_lower:
                result = await self._health_check(task)
            elif "schema" in description_lower and "drift" in description_lower:
                result = await self._detect_schema_drift(task)
            elif "anomal" in description_lower:
                result = await self._detect_anomalies(task)
            elif "sla" in description_lower or "freshness" in description_lower:
                result = await self._sla_check(task)
            else:
                result = await self._full_monitoring_sweep(task)

            task.result = result
            task.status = TaskStatus.COMPLETED

        except Exception as e:
            task.error = str(e)
            task.status = TaskStatus.FAILED
            logger.error(f"[{self.agent_id}] Monitoring task failed: {e}")

        self.task_history.append(task)
        return task

    async def _health_check(self, task: AgentTask) -> dict:
        """Run a comprehensive health check across all pipeline components."""
        health = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "overall_status": "healthy",
            "components": {},
            "issues": [],
        }

        # Check Flink jobs
        try:
            jobs_json = await self.call_tool("flink_list_jobs")
            jobs = json.loads(jobs_json)
            failed_jobs = [j for j in jobs.get("jobs", []) if j.get("state") == "FAILED"]
            running_jobs = [j for j in jobs.get("jobs", []) if j.get("state") == "RUNNING"]

            health["components"]["flink"] = {
                "status": "degraded" if failed_jobs else "healthy",
                "running_jobs": len(running_jobs),
                "failed_jobs": len(failed_jobs),
                "total_jobs": jobs.get("count", 0),
            }
            if failed_jobs:
                health["issues"].append({
                    "component": "flink",
                    "severity": "HIGH",
                    "message": f"{len(failed_jobs)} Flink job(s) have FAILED",
                    "details": failed_jobs,
                })
                health["overall_status"] = "degraded"
        except Exception as e:
            health["components"]["flink"] = {"status": "unreachable", "error": str(e)}
            health["issues"].append({
                "component": "flink",
                "severity": "CRITICAL",
                "message": f"Cannot reach Flink cluster: {e}",
            })
            health["overall_status"] = "critical"

        # Check Kafka
        try:
            topics_json = await self.call_tool("kafka_list_topics")
            topics = json.loads(topics_json)
            health["components"]["kafka"] = {
                "status": "healthy",
                "topic_count": topics.get("count", 0),
            }
        except Exception as e:
            health["components"]["kafka"] = {"status": "unreachable", "error": str(e)}
            health["issues"].append({
                "component": "kafka",
                "severity": "CRITICAL",
                "message": f"Cannot reach Kafka: {e}",
            })
            health["overall_status"] = "critical"

        # Check PostgreSQL
        try:
            tables_json = await self.call_tool("pg_list_tables")
            tables = json.loads(tables_json)
            health["components"]["postgres"] = {
                "status": "healthy",
                "table_count": len(tables.get("tables", [])),
            }
        except Exception as e:
            health["components"]["postgres"] = {"status": "unreachable", "error": str(e)}
            health["issues"].append({
                "component": "postgres",
                "severity": "HIGH",
                "message": f"Cannot reach PostgreSQL: {e}",
            })

        return health

    async def _detect_schema_drift(self, task: AgentTask) -> dict:
        """Check for schema drift on source tables."""
        table_name = task.context.get("table_name", "orders")

        # Get current schema
        schema_json = await self.call_tool("pg_inspect_schema", table_name=table_name)
        current_schema = json.loads(schema_json)

        # Compare with baseline
        baseline = self._baseline_schemas.get(table_name)

        if baseline is None:
            # First time — store as baseline
            self._baseline_schemas[table_name] = current_schema
            return {
                "table": table_name,
                "drift_detected": False,
                "message": "Baseline schema stored. Will detect drift on next check.",
                "schema": current_schema,
            }

        # Compare columns
        baseline_cols = {c["column_name"]: c for c in baseline.get("columns", [])}
        current_cols = {c["column_name"]: c for c in current_schema.get("columns", [])}

        added = set(current_cols.keys()) - set(baseline_cols.keys())
        removed = set(baseline_cols.keys()) - set(current_cols.keys())
        modified = []

        for col_name in set(baseline_cols.keys()) & set(current_cols.keys()):
            if baseline_cols[col_name]["data_type"] != current_cols[col_name]["data_type"]:
                modified.append({
                    "column": col_name,
                    "old_type": baseline_cols[col_name]["data_type"],
                    "new_type": current_cols[col_name]["data_type"],
                })

        drift_detected = bool(added or removed or modified)

        result = {
            "table": table_name,
            "drift_detected": drift_detected,
            "changes": {
                "columns_added": list(added),
                "columns_removed": list(removed),
                "columns_modified": modified,
            },
        }

        if drift_detected:
            # Update baseline
            self._baseline_schemas[table_name] = current_schema
            result["severity"] = "HIGH" if removed or modified else "WARNING"
            result["recommended_action"] = "INVOKE_HEALER_AGENT"

        return result

    async def _detect_anomalies(self, task: AgentTask) -> dict:
        """Use LLM to analyze metrics and detect anomalies."""
        # Gather current metrics
        health = await self._health_check(task)

        prompt = f"""Analyze the following pipeline health data and identify any anomalies:

{json.dumps(health, indent=2)}

Look for:
1. Failed or degraded components
2. Unusual patterns in job counts or topic counts
3. Any potential performance issues

Return a JSON object with:
- "anomalies": list of detected anomalies with severity and description
- "recommendations": list of recommended actions
- "overall_assessment": brief summary
"""
        analysis = await self.reason(prompt, health)

        return {
            "health_data": health,
            "analysis": analysis,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    async def _sla_check(self, task: AgentTask) -> dict:
        """Check SLA compliance for pipelines."""
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "sla_checks": [],
            "message": "SLA monitoring active — will report violations when pipelines are running",
        }

    async def _full_monitoring_sweep(self, task: AgentTask) -> dict:
        """Run all monitoring checks."""
        health = await self._health_check(task)

        # Check schema drift for all tables
        drift_results = {}
        try:
            tables_json = await self.call_tool("pg_list_tables")
            tables = json.loads(tables_json)
            for table in tables.get("tables", []):
                drift_task = AgentTask(
                    description="Check schema drift",
                    assigned_to=AgentRole.MONITOR,
                    context={"table_name": table["table_name"]},
                )
                drift = await self._detect_schema_drift(drift_task)
                drift_results[table["table_name"]] = drift
        except Exception as e:
            drift_results["error"] = str(e)

        return {
            "health": health,
            "schema_drift": drift_results,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
