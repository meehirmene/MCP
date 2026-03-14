"""
FlowForge Migration Agent

Specialist agent responsible for schema evolution and data migration:
- Executing ALTER TABLE on Iceberg tables via Flink SQL
- Creating Kafka replay topics for historical data backfill
- Coordinating blue/green cutover for breaking schema changes
"""

import json
import logging
from datetime import datetime, timezone

from flowforge.agents.shared.base import (
    BaseAgent, AgentRole, AgentTask, AgentCapability, TaskStatus,
)
from flowforge.agents.shared.memory import AgentMemory
from flowforge.servers.flink_mcp import submit_sql
from flowforge.servers.postgres_mcp import inspect_schema, query
from flowforge.servers.kafka_mcp import create_topic

logger = logging.getLogger(__name__)

# PostgreSQL → Flink/Iceberg type mapping (reused from healer pattern)
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


class MigrationAgent(BaseAgent):
    """
    The Migration Agent handles schema evolution and data backfill operations.

    It can:
    1. Evolve Iceberg table schemas (ALTER TABLE ADD/DROP/MODIFY COLUMN)
    2. Create Kafka replay topics for backfilling historical data
    3. Coordinate blue/green cutovers for breaking schema changes
    4. Generate migration SQL from schema diffs
    """

    def __init__(self, **kwargs):
        super().__init__(role=AgentRole.MIGRATION, **kwargs)
        self.memory = AgentMemory()

    def _setup(self):
        self.register_tool("flink_submit_sql", submit_sql)
        self.register_tool("pg_inspect_schema", inspect_schema)
        self.register_tool("pg_query", query)
        self.register_tool("kafka_create_topic", create_topic)

        self.capabilities = [
            AgentCapability(
                name="schema_evolution",
                description="Execute ALTER TABLE statements to evolve Iceberg table schemas via Flink SQL",
                tools_required=["pg_inspect_schema", "flink_submit_sql"],
            ),
            AgentCapability(
                name="backfill_historical",
                description="Create a Kafka replay topic and coordinate re-streaming of historical data",
                tools_required=["kafka_create_topic"],
            ),
            AgentCapability(
                name="coordinate_cutover",
                description="Coordinate blue/green cutover to swap old schema pipeline for new one",
                tools_required=["flink_submit_sql"],
            ),
        ]

    async def execute_task(self, task: AgentTask) -> AgentTask:
        """Execute a migration task."""
        task.status = TaskStatus.IN_PROGRESS
        logger.info(f"[{self.agent_id}] Executing migration task: {task.description}")

        try:
            description_lower = task.description.lower()

            if "backfill" in description_lower or "replay" in description_lower or "historical" in description_lower:
                result = await self._backfill_historical(task)
            elif "cutover" in description_lower or "blue" in description_lower or "green" in description_lower:
                result = await self._coordinate_cutover(task)
            else:
                result = await self._evolve_schema(task)

            task.result = result
            task.status = TaskStatus.COMPLETED

        except Exception as e:
            task.error = str(e)
            task.status = TaskStatus.FAILED
            logger.error(f"[{self.agent_id}] Migration task failed: {e}")

        self.task_history.append(task)
        return task

    async def _evolve_schema(self, task: AgentTask) -> dict:
        """
        Evolve an Iceberg table schema to match source PostgreSQL schema changes.

        Expects task.context to contain:
        - table_name: the source table (e.g. "orders")
        - columns_added: list of new column names
        - columns_removed: list of removed column names (flagged, not auto-dropped)
        - columns_modified: list of {column, old_type, new_type} dicts
        """
        table_name = task.context.get("table_name", "orders")
        columns_added = task.context.get("columns_added", [])
        columns_removed = task.context.get("columns_removed", [])
        columns_modified = task.context.get("columns_modified", [])

        # Inspect current source schema for accurate type mapping
        schema_json = await self.call_tool("pg_inspect_schema", table_name=table_name)
        schema = json.loads(schema_json)
        col_map = {col["column_name"]: col for col in schema.get("columns", [])}

        # If no explicit columns provided, use LLM to infer what changed
        if not any([columns_added, columns_removed, columns_modified]):
            prompt = f"""Analyze this PostgreSQL table schema and identify what migrations might be needed.

Table: {table_name}
Schema: {json.dumps(schema, indent=2)}
Task description: {task.description}

Return JSON: {{
  "columns_added": ["col1"],
  "columns_removed": [],
  "columns_modified": [],
  "reasoning": "<why>"
}}"""
            inference = await self.reason(prompt)
            try:
                inferred = json.loads(inference)
                columns_added = inferred.get("columns_added", [])
                columns_removed = inferred.get("columns_removed", [])
                columns_modified = inferred.get("columns_modified", [])
            except (json.JSONDecodeError, TypeError):
                pass

        migration_actions = []
        executed_sqls = []

        # Add new columns — safe to auto-execute
        for col_name in columns_added:
            col_info = col_map.get(col_name, {})
            flink_type = _TYPE_MAP.get(col_info.get("data_type", "text"), "STRING")
            alter_sql = f"ALTER TABLE iceberg_{table_name} ADD COLUMN {col_name} {flink_type}"

            try:
                result_json = await self.call_tool("flink_submit_sql", sql_statement=alter_sql)
                result = json.loads(result_json)
                executed = True
                executed_sqls.append(alter_sql)
            except Exception as e:
                result = {"error": str(e)}
                executed = False

            migration_actions.append({
                "action": "add_column",
                "column": col_name,
                "flink_type": flink_type,
                "sql": alter_sql,
                "executed": executed,
                "result": result,
            })

        # Drop columns — require human review, never auto-drop
        for col_name in columns_removed:
            migration_actions.append({
                "action": "remove_column_flagged",
                "column": col_name,
                "message": f"Column '{col_name}' removed from source. Downstream Iceberg table preserved to avoid data loss.",
                "requires_human_review": True,
                "executed": False,
            })

        # Type modifications — require human review
        for mod in columns_modified:
            col = mod.get("column", "")
            old_type = mod.get("old_type", "")
            new_type = mod.get("new_type", "")
            migration_actions.append({
                "action": "type_change_flagged",
                "column": col,
                "old_type": old_type,
                "new_type": new_type,
                "message": f"Type change {old_type} → {new_type} on '{col}' requires human validation.",
                "requires_human_review": True,
                "executed": False,
            })

        auto_migrated = [a for a in migration_actions if not a.get("requires_human_review")]
        needs_review = [a for a in migration_actions if a.get("requires_human_review")]
        success = len(auto_migrated) > 0 or len(migration_actions) == 0

        # Log migration event
        self.memory.log_event({
            "agent_id": self.agent_id,
            "agent_role": self.role.value,
            "event_type": "SCHEMA_MIGRATED",
            "details": {
                "table": table_name,
                "columns_migrated": [a["column"] for a in auto_migrated],
                "columns_flagged": [a["column"] for a in needs_review],
                "sqls_executed": executed_sqls,
            },
        })

        # Store migration pattern for future learning
        self.memory.store_incident(
            incident_type="schema_migration",
            context={"table": table_name},
            resolution={
                "actions": [a["action"] for a in auto_migrated],
                "sqls": executed_sqls,
            },
            success=success,
        )

        return {
            "status": "migrated" if auto_migrated else "no_safe_migrations",
            "table": table_name,
            "migration_actions": migration_actions,
            "auto_migrated": len(auto_migrated),
            "requires_review": len(needs_review),
            "executed_sqls": executed_sqls,
            "migrated_at": datetime.now(timezone.utc).isoformat(),
        }

    async def _backfill_historical(self, task: AgentTask) -> dict:
        """Create a Kafka replay topic and prepare backfill configuration."""
        table_name = task.context.get("table_name", "orders")
        start_date = task.context.get("start_date", "")
        end_date = task.context.get("end_date", "")

        replay_topic = f"replay.{table_name}.backfill"

        # Create the replay topic
        try:
            create_result_json = await self.call_tool(
                "kafka_create_topic",
                topic_name=replay_topic,
                num_partitions=task.context.get("partitions", 3),
                replication_factor=1,
            )
            create_result = json.loads(create_result_json)
            topic_created = True
        except Exception as e:
            create_result = {"error": str(e)}
            topic_created = False

        # Generate the backfill SQL for Flink to read from PostgreSQL
        backfill_sql = f"""INSERT INTO {replay_topic}_sink
SELECT * FROM pg_{table_name}
WHERE TRUE"""
        if start_date:
            backfill_sql += f"\n  AND updated_at >= '{start_date}'"
        if end_date:
            backfill_sql += f"\n  AND updated_at <= '{end_date}'"

        backfill_submitted = False
        backfill_result = {}
        if topic_created:
            try:
                backfill_result_json = await self.call_tool(
                    "flink_submit_sql", sql_statement=backfill_sql
                )
                backfill_result = json.loads(backfill_result_json)
                backfill_submitted = True
            except Exception as e:
                backfill_result = {"error": str(e)}

        self.memory.log_event({
            "agent_id": self.agent_id,
            "agent_role": self.role.value,
            "event_type": "BACKFILL_INITIATED",
            "details": {
                "table": table_name,
                "replay_topic": replay_topic,
                "start_date": start_date,
                "end_date": end_date,
                "topic_created": topic_created,
                "backfill_submitted": backfill_submitted,
            },
        })

        return {
            "status": "backfill_initiated" if backfill_submitted else "topic_only",
            "table": table_name,
            "replay_topic": replay_topic,
            "topic_created": topic_created,
            "create_result": create_result,
            "backfill_sql": backfill_sql,
            "backfill_submitted": backfill_submitted,
            "backfill_result": backfill_result,
            "initiated_at": datetime.now(timezone.utc).isoformat(),
        }

    async def _coordinate_cutover(self, task: AgentTask) -> dict:
        """
        Coordinate a blue/green cutover for a breaking schema change.

        Generates the SQL to swap the active pipeline from old to new schema.
        Does NOT auto-execute — returns the cutover plan for human approval.
        """
        table_name = task.context.get("table_name", "orders")
        old_pipeline_id = task.context.get("old_pipeline_id", f"pipeline_{table_name}_v1")
        new_pipeline_id = task.context.get("new_pipeline_id", f"pipeline_{table_name}_v2")

        schema_json = await self.call_tool("pg_inspect_schema", table_name=table_name)
        schema = json.loads(schema_json)

        prompt = f"""Design a blue/green cutover plan for this schema migration.

Table: {table_name}
Current Schema: {json.dumps(schema, indent=2)}
Old Pipeline: {old_pipeline_id}
New Pipeline: {new_pipeline_id}
Context: {json.dumps(task.context, indent=2)}

Produce a step-by-step cutover plan as JSON:
{{
  "steps": [
    {{"step": 1, "action": "<description>", "sql": "<optional SQL>", "requires_human": true/false}},
    ...
  ],
  "rollback_plan": "<what to do if cutover fails>",
  "estimated_downtime_seconds": <int>,
  "risk_level": "LOW|MEDIUM|HIGH"
}}"""

        cutover_plan = await self.reason(prompt, {"schema": schema, "context": task.context})

        self.memory.log_event({
            "agent_id": self.agent_id,
            "agent_role": self.role.value,
            "event_type": "CUTOVER_PLANNED",
            "details": {
                "table": table_name,
                "old_pipeline": old_pipeline_id,
                "new_pipeline": new_pipeline_id,
            },
        })

        return {
            "status": "cutover_plan_ready",
            "table": table_name,
            "old_pipeline_id": old_pipeline_id,
            "new_pipeline_id": new_pipeline_id,
            "cutover_plan": cutover_plan,
            "note": "Cutover plan requires human approval before execution.",
            "planned_at": datetime.now(timezone.utc).isoformat(),
        }
