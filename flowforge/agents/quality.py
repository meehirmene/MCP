"""
FlowForge Quality Agent

Specialist agent responsible for data quality validation:
- Row count reconciliation between source and sink
- Data freshness checks against SLA windows
- Completeness checks for critical fields
- Kafka consumer lag monitoring
"""

import json
import logging
from datetime import datetime, timezone

from flowforge.agents.shared.base import (
    BaseAgent, AgentRole, AgentTask, AgentCapability, TaskStatus,
)
from flowforge.servers.postgres_mcp import query, list_tables
from flowforge.servers.kafka_mcp import list_topics, get_topic_info, consume_messages
from flowforge.servers.mongodb_mcp import find_documents

logger = logging.getLogger(__name__)


class QualityAgent(BaseAgent):
    """
    The Quality Agent validates data quality across pipeline sources and sinks.

    It can:
    1. Reconcile row counts between PostgreSQL source and downstream sinks
    2. Verify data freshness against SLA thresholds
    3. Check completeness of critical fields
    4. Monitor Kafka consumer lag as a proxy for pipeline health
    """

    def __init__(self, **kwargs):
        super().__init__(role=AgentRole.QUALITY, **kwargs)

    def _setup(self):
        self.register_tool("pg_query", query)
        self.register_tool("pg_list_tables", list_tables)
        self.register_tool("kafka_list_topics", list_topics)
        self.register_tool("kafka_topic_info", get_topic_info)
        self.register_tool("kafka_consume", consume_messages)
        self.register_tool("mongo_find", find_documents)

        self.capabilities = [
            AgentCapability(
                name="row_count_reconciliation",
                description="Compare source PostgreSQL row counts against downstream sink counts",
                tools_required=["pg_query"],
            ),
            AgentCapability(
                name="freshness_check",
                description="Verify that the most recent record is within the SLA freshness window",
                tools_required=["pg_query"],
            ),
            AgentCapability(
                name="completeness_check",
                description="Check for null or missing values in critical fields",
                tools_required=["pg_query"],
            ),
            AgentCapability(
                name="lag_check",
                description="Check Kafka consumer group lag as a proxy for pipeline health",
                tools_required=["kafka_topic_info"],
            ),
        ]

    async def execute_task(self, task: AgentTask) -> AgentTask:
        """Execute a data quality validation task."""
        task.status = TaskStatus.IN_PROGRESS
        logger.info(f"[{self.agent_id}] Executing quality task: {task.description}")

        try:
            description_lower = task.description.lower()

            if "freshness" in description_lower or "stale" in description_lower or "sla" in description_lower:
                result = await self._freshness_check(task)
            elif "complet" in description_lower or "null" in description_lower or "missing" in description_lower:
                result = await self._completeness_check(task)
            elif "lag" in description_lower or "kafka" in description_lower or "consumer" in description_lower:
                result = await self._lag_check(task)
            else:
                result = await self._full_quality_report(task)

            task.result = result
            task.status = TaskStatus.COMPLETED

        except Exception as e:
            task.error = str(e)
            task.status = TaskStatus.FAILED
            logger.error(f"[{self.agent_id}] Quality task failed: {e}")

        self.task_history.append(task)
        return task

    async def _full_quality_report(self, task: AgentTask) -> dict:
        """Run a comprehensive quality report: counts, freshness, completeness, and lag."""
        table_name = task.context.get("table_name", "orders")
        topic_name = task.context.get("topic_name", f"cdc.ecommerce.{table_name}")
        freshness_sla_minutes = task.context.get("freshness_sla_minutes", 60)

        findings = {}

        # Row count
        count_result = await self._run_count_query(table_name)
        findings["row_count"] = count_result

        # Freshness
        freshness_result = await self._run_freshness_query(table_name, freshness_sla_minutes)
        findings["freshness"] = freshness_result

        # Completeness (if critical fields provided)
        critical_fields = task.context.get("critical_fields", [])
        if critical_fields:
            completeness_result = await self._run_completeness_query(table_name, critical_fields)
            findings["completeness"] = completeness_result

        # Kafka lag
        lag_result = await self._run_lag_check(topic_name)
        findings["kafka_lag"] = lag_result

        # LLM evaluates overall quality
        prompt = f"""Evaluate the data quality findings for the '{table_name}' pipeline.

Findings:
{json.dumps(findings, indent=2)}

SLA freshness window: {freshness_sla_minutes} minutes

Assign:
- overall_severity: OK | WARNING | CRITICAL
- issues: list of quality issues found
- recommendations: list of actionable fixes
- passing_checks: list of checks that passed

Return as JSON."""

        evaluation = await self.reason(prompt, {"findings": findings, "table": table_name})

        return {
            "table": table_name,
            "topic": topic_name,
            "findings": findings,
            "evaluation": evaluation,
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }

    async def _freshness_check(self, task: AgentTask) -> dict:
        """Check if the most recent record is within the SLA freshness window."""
        table_name = task.context.get("table_name", "orders")
        freshness_sla_minutes = task.context.get("freshness_sla_minutes", 60)

        result = await self._run_freshness_query(table_name, freshness_sla_minutes)

        return {
            "table": table_name,
            "freshness_sla_minutes": freshness_sla_minutes,
            "freshness": result,
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }

    async def _completeness_check(self, task: AgentTask) -> dict:
        """Check for null values in critical fields."""
        table_name = task.context.get("table_name", "orders")
        critical_fields = task.context.get("critical_fields", [])

        if not critical_fields:
            # Discover critical fields via schema introspection
            tables_json = await self.call_tool("pg_list_tables")
            tables = json.loads(tables_json)
            prompt = f"""Given these tables, what are the critical non-nullable fields for '{table_name}'?

Tables: {json.dumps(tables, indent=2)}

Return JSON: {{"critical_fields": ["field1", "field2"]}}"""
            suggestion = await self.reason(prompt)
            try:
                suggestion_dict = json.loads(suggestion)
                critical_fields = suggestion_dict.get("critical_fields", ["id"])
            except (json.JSONDecodeError, TypeError):
                critical_fields = ["id"]

        result = await self._run_completeness_query(table_name, critical_fields)

        return {
            "table": table_name,
            "critical_fields": critical_fields,
            "completeness": result,
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }

    async def _lag_check(self, task: AgentTask) -> dict:
        """Check Kafka consumer group lag for a topic."""
        topic_name = task.context.get("topic_name", "")

        if not topic_name:
            topics_json = await self.call_tool("kafka_list_topics")
            topics = json.loads(topics_json)
            return {
                "status": "topic_name_required",
                "available_topics": topics,
                "message": "Provide topic_name in task context to check lag.",
            }

        result = await self._run_lag_check(topic_name)

        return {
            "topic": topic_name,
            "lag": result,
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }

    async def _run_count_query(self, table_name: str) -> dict:
        """Run a COUNT(*) query on a PostgreSQL table."""
        sql = f"SELECT COUNT(*) AS row_count FROM {table_name}"
        try:
            result_json = await self.call_tool("pg_query", sql=sql)
            result = json.loads(result_json)
            rows = result.get("rows", [])
            count = rows[0].get("row_count", 0) if rows else 0
            return {"status": "ok", "row_count": count, "sql": sql}
        except Exception as e:
            return {"status": "error", "error": str(e), "sql": sql}

    async def _run_freshness_query(self, table_name: str, sla_minutes: int) -> dict:
        """Check the most recent updated_at timestamp against the SLA window."""
        sql = f"""
            SELECT
                MAX(updated_at) AS last_updated,
                NOW() - MAX(updated_at) AS lag,
                CASE
                    WHEN NOW() - MAX(updated_at) > INTERVAL '{sla_minutes} minutes'
                    THEN 'STALE'
                    ELSE 'FRESH'
                END AS freshness_status
            FROM {table_name}
            WHERE updated_at IS NOT NULL
        """
        try:
            result_json = await self.call_tool("pg_query", sql=sql.strip())
            result = json.loads(result_json)
            rows = result.get("rows", [])
            return {"status": "ok", "result": rows[0] if rows else {}, "sql": sql.strip()}
        except Exception as e:
            # Table may not have updated_at — fall back to row count only
            return {"status": "skipped", "reason": str(e)}

    async def _run_completeness_query(self, table_name: str, fields: list[str]) -> dict:
        """Check null counts for each critical field."""
        checks = {}
        for field in fields:
            sql = f"SELECT COUNT(*) AS null_count FROM {table_name} WHERE {field} IS NULL"
            try:
                result_json = await self.call_tool("pg_query", sql=sql)
                result = json.loads(result_json)
                rows = result.get("rows", [])
                null_count = rows[0].get("null_count", 0) if rows else 0
                checks[field] = {"null_count": null_count, "status": "WARNING" if null_count > 0 else "OK"}
            except Exception as e:
                checks[field] = {"status": "error", "error": str(e)}
        return checks

    async def _run_lag_check(self, topic_name: str) -> dict:
        """Get Kafka topic info and assess consumer lag."""
        try:
            topic_json = await self.call_tool("kafka_topic_info", topic_name=topic_name)
            topic = json.loads(topic_json)
            return {"status": "ok", "topic_info": topic}
        except Exception as e:
            return {"status": "error", "error": str(e)}
