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
from flowforge.config import config
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
            elif ("flink" in description_lower or "aggregat" in description_lower or "stream" in description_lower
                  or "hourly" in description_lower or "daily" in description_lower or "weekly" in description_lower
                  or "revenue" in description_lower or "calculat" in description_lower or "window" in description_lower
                  or "tumble" in description_lower or "total" in description_lower):
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

    # ─────────────────────────────────────────────
    # Flink SQL Template Helpers
    # ─────────────────────────────────────────────

    def _pg_to_flink_type(self, pg_type: str) -> str:
        """Map a PostgreSQL column type to its Flink SQL equivalent."""
        mapping = {
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
        return mapping.get(pg_type.lower().strip(), "STRING")

    def _build_cdc_source_sql(self, table_name: str, columns: list[dict]) -> str:
        """Generate a Flink PostgreSQL CDC source table definition."""
        col_defs = []
        pk_col = None
        for col in columns:
            name = col["column_name"]
            flink_type = self._pg_to_flink_type(col.get("data_type", "text"))
            col_defs.append(f"    {name} {flink_type}")
            if name in ("id", f"{table_name}_id", "order_id"):
                pk_col = name
        col_defs.append("    proc_time AS PROCTIME()")
        pk_clause = f",\n    PRIMARY KEY ({pk_col}) NOT ENFORCED" if pk_col else ""
        cols_sql = ",\n".join(col_defs)
        pg = config.postgres
        return (
            f"CREATE TABLE IF NOT EXISTS {table_name}_cdc (\n"
            f"{cols_sql}{pk_clause}\n"
            f") WITH (\n"
            f"    'connector' = 'postgres-cdc',\n"
            f"    'hostname' = '{pg.host}',\n"
            f"    'port' = '{pg.port}',\n"
            f"    'username' = '{pg.user}',\n"
            f"    'password' = '{pg.password}',\n"
            f"    'database-name' = '{pg.database}',\n"
            f"    'schema-name' = 'public',\n"
            f"    'table-name' = '{table_name}'\n"
            f")"
        )

    def _build_iceberg_sink_sql(self, table_name: str, columns: list[dict]) -> str:
        """Generate a Flink Iceberg REST catalog sink table definition."""
        col_defs = [f"    {col['column_name']} {self._pg_to_flink_type(col.get('data_type', 'text'))}"
                    for col in columns]
        cols_sql = ",\n".join(col_defs)
        ic = config.iceberg
        return (
            f"CREATE TABLE IF NOT EXISTS iceberg_{table_name} (\n"
            f"{cols_sql}\n"
            f") WITH (\n"
            f"    'connector' = 'iceberg',\n"
            f"    'catalog-name' = 'iceberg_catalog',\n"
            f"    'catalog-type' = 'rest',\n"
            f"    'uri' = '{ic.rest_url}',\n"
            f"    'warehouse' = '{ic.warehouse}',\n"
            f"    's3.endpoint' = '{ic.s3_endpoint}',\n"
            f"    's3.access-key' = '{ic.s3_access_key}',\n"
            f"    's3.secret-key' = '{ic.s3_secret_key}'\n"
            f")"
        )

    def _build_window_aggregation_sql(
        self,
        source_table: str,
        sink_table: str,
        group_by: str,
        window_seconds: int = 300,
    ) -> str:
        """Generate a Flink TUMBLE window aggregation INSERT statement."""
        return (
            f"INSERT INTO {sink_table}\n"
            f"SELECT\n"
            f"    {group_by},\n"
            f"    COUNT(*) AS event_count,\n"
            f"    SUM(CAST(total_amount AS DOUBLE)) AS total_revenue,\n"
            f"    TUMBLE_START(proc_time, INTERVAL '{window_seconds}' SECOND) AS window_start,\n"
            f"    TUMBLE_END(proc_time, INTERVAL '{window_seconds}' SECOND) AS window_end\n"
            f"FROM {source_table}\n"
            f"GROUP BY {group_by}, TUMBLE(proc_time, INTERVAL '{window_seconds}' SECOND)"
        )

    # ─────────────────────────────────────────────
    # Flink Job Creation
    # ─────────────────────────────────────────────

    async def _create_flink_job(self, task: AgentTask) -> dict:
        """Generate Flink SQL using templates and submit it to the cluster."""
        context = task.context.copy()
        table_name = context.get("source_table", "orders")

        # Get source schema
        if "source_schema" not in context:
            schema_json = await self.call_tool("pg_inspect_schema", table_name=table_name)
            context["source_schema"] = json.loads(schema_json)

        schema = context["source_schema"]
        columns = schema.get("columns", [])
        description_lower = task.description.lower()

        # Build SQL from templates
        cdc_sql = self._build_cdc_source_sql(table_name, columns)
        iceberg_sql = self._build_iceberg_sink_sql(table_name, columns)

        submitted_jobs = []

        # Submit CDC source table definition
        cdc_result = json.loads(await self.call_tool("flink_submit_sql", sql_statement=cdc_sql))
        if cdc_result.get("error"):
            raise RuntimeError(f"Flink SQL (CDC source) failed: {cdc_result['error']}")
        submitted_jobs.append({"type": "cdc_source", "result": cdc_result})

        # Submit Iceberg sink table definition
        iceberg_result = json.loads(await self.call_tool("flink_submit_sql", sql_statement=iceberg_sql))
        if iceberg_result.get("error"):
            raise RuntimeError(f"Flink SQL (Iceberg sink) failed: {iceberg_result['error']}")
        submitted_jobs.append({"type": "iceberg_sink", "result": iceberg_result})

        # Determine if windowed aggregation is needed
        group_by = context.get("group_by", "region")
        if "hourly" in description_lower or ("hour" in description_lower and "hourly" not in description_lower):
            window_seconds = int(context.get("window_seconds", 3600))
        elif "daily" in description_lower or "day" in description_lower:
            window_seconds = int(context.get("window_seconds", 86400))
        else:
            window_seconds = int(context.get("window_seconds", 300))
        needs_agg = any(kw in description_lower for kw in (
            "aggregat", "window", "region", "tumble", "minute",
            "hourly", "daily", "weekly", "revenue", "calculat", "total", "sum", "count", "average", "avg",
        ))

        if needs_agg:
            agg_cols = [
                {"column_name": group_by, "data_type": "character varying"},
                {"column_name": "event_count", "data_type": "bigint"},
                {"column_name": "total_revenue", "data_type": "double precision"},
                {"column_name": "window_start", "data_type": "timestamp without time zone"},
                {"column_name": "window_end", "data_type": "timestamp without time zone"},
            ]
            agg_sink_sql = self._build_iceberg_sink_sql(f"{table_name}_agg", agg_cols)
            await self.call_tool("flink_submit_sql", sql_statement=agg_sink_sql)
            insert_sql = self._build_window_aggregation_sql(
                f"{table_name}_cdc", f"iceberg_{table_name}_agg", group_by, window_seconds,
            )
        else:
            col_names = ", ".join(col["column_name"] for col in columns)
            insert_sql = f"INSERT INTO iceberg_{table_name} SELECT {col_names} FROM {table_name}_cdc"

        # Submit the streaming INSERT — this creates the actual Flink job
        insert_result = json.loads(await self.call_tool("flink_submit_sql", sql_statement=insert_sql))
        if insert_result.get("error"):
            raise RuntimeError(f"Flink SQL (streaming insert) failed: {insert_result['error']}")
        submitted_jobs.append({"type": "streaming_insert", "result": insert_result})

        session_id = insert_result.get("session_id", "")
        operation_handle = insert_result.get("operation_handle", "")
        job_ref = operation_handle or session_id

        # Persist pipeline state for Healer to reference on restart
        pipeline_id = context.get("pipeline_id", f"pipeline-{table_name}")
        self.memory.set_pipeline_state(pipeline_id, {
            "status": "running",
            "source_table": table_name,
            "flink_session_id": session_id,
            "flink_operation_handle": operation_handle,
            "flink_job_ref": job_ref,
            "cdc_sql": cdc_sql,
            "iceberg_sql": iceberg_sql,
            "insert_sql": insert_sql,
            "source": f"PostgreSQL → CDC → Flink → Iceberg",
        })

        self.memory.log_event({
            "agent_id": self.agent_id,
            "agent_role": self.role.value,
            "event_type": "FLINK_JOB_SUBMITTED",
            "pipeline_id": pipeline_id,
            "details": {
                "table": table_name,
                "job_ref": job_ref,
                "task": task.description,
                "aggregation": needs_agg,
            },
        })

        logger.info(f"[{self.agent_id}] Flink job submitted: pipeline={pipeline_id}, ref={job_ref}")
        return {
            "status": "flink_job_submitted",
            "pipeline_id": pipeline_id,
            "table": table_name,
            "job_ref": job_ref,
            "aggregation_enabled": needs_agg,
            "submitted_jobs": submitted_jobs,
            "sql_statements": {
                "cdc_source": cdc_sql,
                "iceberg_sink": iceberg_sql,
                "streaming_insert": insert_sql,
            },
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
