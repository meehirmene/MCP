"""
FlowForge Builder Agent

Specialist agent responsible for creating and deploying data pipelines:
- Creating Kafka topics
- Setting up CDC from PostgreSQL
- Generating and submitting Flink SQL jobs
- Creating Iceberg sink tables
"""

import json
import logging

from flowforge.agents.shared.base import (
    BaseAgent, AgentRole, AgentTask, AgentCapability, TaskStatus,
)
from flowforge.servers.postgres_mcp import list_tables, inspect_schema, check_cdc_status, query
from flowforge.servers.kafka_mcp import list_topics, create_topic, produce_message, produce_batch
from flowforge.servers.flink_mcp import cluster_overview, list_jobs, submit_sql
from flowforge.servers.mongodb_mcp import list_collections, find_documents

logger = logging.getLogger(__name__)


class BuilderAgent(BaseAgent):
    """
    The Builder Agent creates and deploys streaming data pipelines.

    It can:
    1. Inspect source database schemas
    2. Create Kafka topics for data streaming
    3. Generate Flink SQL for stream processing
    4. Deploy pipeline configurations
    """

    def __init__(self, **kwargs):
        super().__init__(role=AgentRole.BUILDER, **kwargs)

    def _setup(self):
        """Register tools and capabilities."""
        # PostgreSQL tools
        self.register_tool("pg_list_tables", list_tables, "List PostgreSQL tables")
        self.register_tool("pg_inspect_schema", inspect_schema, "Inspect table schema")
        self.register_tool("pg_check_cdc", check_cdc_status, "Check CDC readiness")
        self.register_tool("pg_query", query, "Execute SQL query")

        # Kafka tools
        self.register_tool("kafka_list_topics", list_topics, "List Kafka topics")
        self.register_tool("kafka_create_topic", create_topic, "Create Kafka topic")
        self.register_tool("kafka_produce", produce_message, "Produce message")
        self.register_tool("kafka_produce_batch", produce_batch, "Produce batch")

        # Flink tools
        self.register_tool("flink_overview", cluster_overview, "Flink cluster overview")
        self.register_tool("flink_list_jobs", list_jobs, "List Flink jobs")
        self.register_tool("flink_submit_sql", submit_sql, "Submit Flink SQL")

        # MongoDB tools
        self.register_tool("mongo_list_collections", list_collections, "List MongoDB collections")
        self.register_tool("mongo_find", find_documents, "Find MongoDB documents")

        # Capabilities
        self.capabilities = [
            AgentCapability(
                name="create_streaming_pipeline",
                description="Create an end-to-end streaming pipeline from source to sink",
                tools_required=["pg_inspect_schema", "kafka_create_topic", "flink_submit_sql"],
            ),
            AgentCapability(
                name="setup_cdc",
                description="Set up Change Data Capture from PostgreSQL to Kafka",
                tools_required=["pg_check_cdc", "kafka_create_topic"],
            ),
            AgentCapability(
                name="create_flink_job",
                description="Generate and submit Flink SQL jobs for stream processing",
                tools_required=["flink_submit_sql", "flink_overview"],
            ),
            AgentCapability(
                name="inspect_sources",
                description="Inspect source database schemas and sample data",
                tools_required=["pg_list_tables", "pg_inspect_schema", "mongo_list_collections"],
            ),
        ]

    async def execute_task(self, task: AgentTask) -> AgentTask:
        """Execute a builder task."""
        task.status = TaskStatus.IN_PROGRESS
        logger.info(f"[{self.agent_id}] Executing task: {task.description}")

        try:
            # Determine what kind of build task this is
            description_lower = task.description.lower()

            if "inspect" in description_lower or "schema" in description_lower:
                result = await self._inspect_sources(task)
            elif "cdc" in description_lower or "capture" in description_lower:
                result = await self._setup_cdc_pipeline(task)
            elif "topic" in description_lower or "kafka" in description_lower:
                result = await self._create_kafka_topic(task)
            elif "flink" in description_lower or "aggregat" in description_lower or "stream" in description_lower:
                result = await self._create_flink_job(task)
            elif "pipeline" in description_lower:
                result = await self._build_full_pipeline(task)
            else:
                # Use LLM to figure out what to do
                result = await self._llm_guided_build(task)

            task.result = result
            task.status = TaskStatus.COMPLETED
            logger.info(f"[{self.agent_id}] Task completed: {task.task_id}")

        except Exception as e:
            task.error = str(e)
            task.status = TaskStatus.FAILED
            logger.error(f"[{self.agent_id}] Task failed: {e}")

        self.task_history.append(task)
        return task

    async def _inspect_sources(self, task: AgentTask) -> dict:
        """Inspect source databases and return schemas."""
        results = {}

        # Inspect PostgreSQL
        tables_json = await self.call_tool("pg_list_tables")
        tables = json.loads(tables_json)
        results["postgres_tables"] = tables

        # Get schema for each table
        schemas = {}
        for table in tables.get("tables", []):
            schema_json = await self.call_tool("pg_inspect_schema", table_name=table["table_name"])
            schemas[table["table_name"]] = json.loads(schema_json)
        results["postgres_schemas"] = schemas

        # Check CDC status
        cdc_json = await self.call_tool("pg_check_cdc")
        results["cdc_status"] = json.loads(cdc_json)

        # Inspect MongoDB
        collections_json = await self.call_tool("mongo_list_collections")
        results["mongodb_collections"] = json.loads(collections_json)

        return results

    async def _setup_cdc_pipeline(self, task: AgentTask) -> dict:
        """Set up CDC streaming from PostgreSQL to Kafka."""
        table_name = task.context.get("table_name", "orders")

        # 1. Check CDC readiness
        cdc_json = await self.call_tool("pg_check_cdc")
        cdc_status = json.loads(cdc_json)

        if not cdc_status.get("cdc_ready"):
            return {"error": "PostgreSQL WAL level is not 'logical'. CDC cannot be set up."}

        # 2. Create Kafka topic for CDC events
        topic_name = f"cdc.ecommerce.{table_name}"
        topic_result = await self.call_tool(
            "kafka_create_topic",
            topic_name=topic_name,
            num_partitions=3,
        )

        # 3. Get the schema for the table
        schema_json = await self.call_tool("pg_inspect_schema", table_name=table_name)

        return {
            "status": "cdc_pipeline_configured",
            "source_table": table_name,
            "kafka_topic": topic_name,
            "topic_creation": json.loads(topic_result),
            "source_schema": json.loads(schema_json),
            "cdc_status": cdc_status,
        }

    async def _create_kafka_topic(self, task: AgentTask) -> dict:
        """Create a Kafka topic."""
        topic_name = task.context.get("topic_name", "flowforge-default")
        partitions = task.context.get("partitions", 3)

        result = await self.call_tool(
            "kafka_create_topic",
            topic_name=topic_name,
            num_partitions=partitions,
        )
        return json.loads(result)

    async def _create_flink_job(self, task: AgentTask) -> dict:
        """Generate and submit a Flink SQL job."""
        # Use LLM to generate Flink SQL based on the task description
        context = task.context.copy()

        # Get source schema if not already provided
        if "source_schema" not in context:
            table_name = context.get("source_table", "orders")
            schema_json = await self.call_tool("pg_inspect_schema", table_name=table_name)
            context["source_schema"] = json.loads(schema_json)

        # Generate Flink SQL using LLM
        prompt = f"""Generate Flink SQL statements for the following pipeline task:

Task: {task.description}

Source Schema:
{json.dumps(context.get('source_schema', {}), indent=2)}

Requirements:
1. Create a Kafka source table that reads CDC events
2. Create an Iceberg sink table
3. If aggregation is needed, create the appropriate windowed query
4. Use the correct Flink SQL syntax for streaming

Return ONLY the SQL statements, separated by semicolons. No explanations."""

        flink_sql = await self.reason(prompt, context)

        return {
            "status": "flink_sql_generated",
            "task": task.description,
            "generated_sql": flink_sql,
            "source_schema": context.get("source_schema"),
        }

    async def _build_full_pipeline(self, task: AgentTask) -> dict:
        """Build a complete end-to-end pipeline."""
        results = {
            "pipeline_id": task.context.get("pipeline_id", f"pipeline-{task.task_id}"),
            "steps": [],
        }

        # Step 1: Inspect source
        inspect_task = AgentTask(
            description="Inspect source databases",
            assigned_to=AgentRole.BUILDER,
            context=task.context,
        )
        inspect_result = await self._inspect_sources(inspect_task)
        results["steps"].append({"step": "inspect_sources", "result": "completed"})
        results["source_info"] = inspect_result

        # Step 2: Set up CDC
        source_table = task.context.get("source_table", "orders")
        cdc_task = AgentTask(
            description=f"Set up CDC for {source_table}",
            assigned_to=AgentRole.BUILDER,
            context={"table_name": source_table},
        )
        cdc_result = await self._setup_cdc_pipeline(cdc_task)
        results["steps"].append({"step": "setup_cdc", "result": "completed"})
        results["cdc_info"] = cdc_result

        # Step 3: Generate Flink job
        flink_task = AgentTask(
            description=task.description,
            assigned_to=AgentRole.BUILDER,
            context={
                "source_table": source_table,
                "source_schema": cdc_result.get("source_schema"),
                **task.context,
            },
        )
        flink_result = await self._create_flink_job(flink_task)
        results["steps"].append({"step": "create_flink_job", "result": "completed"})
        results["flink_info"] = flink_result

        results["status"] = "pipeline_built"
        return results

    async def _llm_guided_build(self, task: AgentTask) -> dict:
        """Use LLM reasoning to determine and execute build steps."""
        prompt = f"""Analyze this build task and determine what actions to take:

Task: {task.description}
Context: {json.dumps(task.context, indent=2)}

Available actions:
1. inspect_sources - Inspect database schemas
2. setup_cdc - Set up CDC streaming from PostgreSQL
3. create_kafka_topic - Create a Kafka topic
4. create_flink_job - Generate and submit a Flink SQL job
5. build_full_pipeline - Build a complete end-to-end pipeline

Respond with a JSON object containing:
- "action": the action to take (one of the above)
- "params": any parameters needed
- "reasoning": why you chose this action"""

        response = await self.reason(prompt, task.context)

        try:
            plan = json.loads(response)
            action = plan.get("action", "inspect_sources")

            if action == "build_full_pipeline":
                return await self._build_full_pipeline(task)
            elif action == "setup_cdc":
                task.context.update(plan.get("params", {}))
                return await self._setup_cdc_pipeline(task)
            elif action == "create_flink_job":
                task.context.update(plan.get("params", {}))
                return await self._create_flink_job(task)
            else:
                return await self._inspect_sources(task)

        except json.JSONDecodeError:
            # If LLM response isn't valid JSON, default to inspection
            return await self._inspect_sources(task)
