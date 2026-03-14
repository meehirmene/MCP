"""
FlowForge Optimizer Agent

Specialist agent responsible for performance tuning of data pipelines:
- Adjusting Flink job parallelism based on backpressure metrics
- Triggering Iceberg table compaction for fragmented files
- Optimizing watermark delays for late-arriving events
- Rebalancing Kafka partition throughput
"""

import json
import logging

from flowforge.agents.shared.base import (
    BaseAgent, AgentRole, AgentTask, AgentCapability, TaskStatus,
)
from flowforge.servers.flink_mcp import list_jobs, get_job_details, submit_sql, cluster_overview
from flowforge.servers.kafka_mcp import get_topic_info

logger = logging.getLogger(__name__)


class OptimizerAgent(BaseAgent):
    """
    The Optimizer Agent tunes streaming pipeline performance.

    It can:
    1. Detect backpressure and recommend parallelism changes
    2. Trigger Iceberg compaction for fragmented tables
    3. Optimize watermark delays for late event handling
    4. Identify Kafka partition imbalances
    """

    def __init__(self, **kwargs):
        super().__init__(role=AgentRole.OPTIMIZER, **kwargs)

    def _setup(self):
        self.register_tool("flink_list_jobs", list_jobs)
        self.register_tool("flink_job_details", get_job_details)
        self.register_tool("flink_submit_sql", submit_sql)
        self.register_tool("flink_overview", cluster_overview)
        self.register_tool("kafka_topic_info", get_topic_info)

        self.capabilities = [
            AgentCapability(
                name="tune_parallelism",
                description="Increase or decrease Flink job parallelism based on backpressure metrics",
                tools_required=["flink_list_jobs", "flink_job_details", "flink_submit_sql"],
            ),
            AgentCapability(
                name="trigger_compaction",
                description="Submit Iceberg compaction SQL to reduce small file fragmentation",
                tools_required=["flink_submit_sql"],
            ),
            AgentCapability(
                name="optimize_watermarks",
                description="Tune watermark delay settings for late-arriving event streams",
                tools_required=["flink_job_details", "flink_submit_sql"],
            ),
            AgentCapability(
                name="balance_partitions",
                description="Detect and report Kafka partition throughput imbalances",
                tools_required=["kafka_topic_info"],
            ),
        ]

    async def execute_task(self, task: AgentTask) -> AgentTask:
        """Execute a performance optimization task."""
        task.status = TaskStatus.IN_PROGRESS
        logger.info(f"[{self.agent_id}] Executing optimization task: {task.description}")

        try:
            description_lower = task.description.lower()

            if "compact" in description_lower or "compaction" in description_lower:
                result = await self._trigger_compaction(task)
            elif "partition" in description_lower or "kafka" in description_lower:
                result = await self._check_partition_balance(task)
            elif "watermark" in description_lower:
                result = await self._optimize_watermarks(task)
            else:
                result = await self._tune_parallelism(task)

            task.result = result
            task.status = TaskStatus.COMPLETED

        except Exception as e:
            task.error = str(e)
            task.status = TaskStatus.FAILED
            logger.error(f"[{self.agent_id}] Optimization task failed: {e}")

        self.task_history.append(task)
        return task

    async def _tune_parallelism(self, task: AgentTask) -> dict:
        """Analyze Flink job metrics and recommend or apply parallelism changes."""
        jobs_json = await self.call_tool("flink_list_jobs")
        jobs = json.loads(jobs_json)
        running_jobs = [j for j in jobs.get("jobs", []) if j.get("state") == "RUNNING"]

        if not running_jobs:
            return {"status": "no_running_jobs", "message": "No running Flink jobs found to optimize."}

        job_metrics = []
        for job in running_jobs[:5]:  # cap to 5 jobs to avoid excessive calls
            job_id = job.get("id")
            if job_id:
                details_json = await self.call_tool("flink_job_details", job_id=job_id)
                details = json.loads(details_json)
                job_metrics.append({"job_id": job_id, "details": details})

        prompt = """Analyze these Flink job metrics and recommend parallelism optimizations.

For each job, identify:
1. Signs of backpressure (high input/output buffer usage)
2. Recommended parallelism adjustment (increase/decrease/no change)
3. Expected throughput improvement
4. Priority (HIGH/MEDIUM/LOW)

Return a JSON object with:
- "recommendations": list of {job_id, current_parallelism, recommended_parallelism, reason, priority}
- "summary": brief plain-English summary
- "compaction_needed": true/false (if you detect Iceberg sink fragmentation)"""

        analysis = await self.reason(prompt, {"jobs": job_metrics})

        # Try to parse recommendations and apply the highest-priority one
        applied = []
        try:
            analysis_dict = json.loads(analysis) if isinstance(analysis, str) else {}
            recommendations = analysis_dict.get("recommendations", [])
            high_priority = [r for r in recommendations if r.get("priority") == "HIGH"]

            for rec in high_priority[:1]:  # apply at most 1 change autonomously
                job_id = rec.get("job_id")
                new_parallelism = rec.get("recommended_parallelism")
                if job_id and new_parallelism:
                    set_sql = f"SET 'parallelism.default' = '{new_parallelism}'"
                    submit_result = json.loads(
                        await self.call_tool("flink_submit_sql", sql_statement=set_sql)
                    )
                    applied.append({
                        "job_id": job_id,
                        "new_parallelism": new_parallelism,
                        "result": submit_result,
                    })
        except (json.JSONDecodeError, TypeError):
            pass

        return {
            "status": "analyzed",
            "jobs_checked": len(job_metrics),
            "analysis": analysis,
            "applied_changes": applied,
        }

    async def _trigger_compaction(self, task: AgentTask) -> dict:
        """Submit Iceberg compaction SQL for a specified table."""
        table_name = task.context.get("table_name", "")

        if not table_name:
            # Ask LLM to identify which table needs compaction
            jobs_json = await self.call_tool("flink_list_jobs")
            jobs = json.loads(jobs_json)
            prompt = f"""Given these running Flink jobs, which Iceberg table most likely needs compaction?

Jobs: {json.dumps(jobs, indent=2)}

Return JSON: {{"table_name": "<name>", "reason": "<reason>"}}"""
            suggestion = await self.reason(prompt)
            try:
                suggestion_dict = json.loads(suggestion)
                table_name = suggestion_dict.get("table_name", "")
            except (json.JSONDecodeError, TypeError):
                return {"status": "error", "message": "Could not determine table name for compaction."}

        compaction_sql = f"CALL iceberg.system.rewrite_data_files(table => '{table_name}')"

        try:
            result_json = await self.call_tool("flink_submit_sql", sql_statement=compaction_sql)
            result = json.loads(result_json)
            status = "compaction_submitted"
        except Exception as e:
            result = {"error": str(e)}
            status = "compaction_failed"

        return {
            "status": status,
            "table_name": table_name,
            "sql": compaction_sql,
            "result": result,
        }

    async def _optimize_watermarks(self, task: AgentTask) -> dict:
        """Recommend watermark delay adjustments for running Flink jobs."""
        jobs_json = await self.call_tool("flink_list_jobs")
        jobs = json.loads(jobs_json)
        running_jobs = [j for j in jobs.get("jobs", []) if j.get("state") == "RUNNING"]

        if not running_jobs:
            return {"status": "no_running_jobs", "message": "No running jobs to optimize watermarks for."}

        job_id = task.context.get("job_id") or running_jobs[0].get("id")
        details_json = await self.call_tool("flink_job_details", job_id=job_id)
        details = json.loads(details_json)

        prompt = f"""Analyze this Flink job and recommend watermark delay settings.

Job Details: {json.dumps(details, indent=2)}

Consider:
- Current event time vs processing time lag
- Late data arrival patterns
- SLA requirements from context: {json.dumps(task.context)}

Return JSON: {{
  "current_watermark_delay_ms": <int or null>,
  "recommended_watermark_delay_ms": <int>,
  "reason": "<explanation>",
  "sql_hint": "<SET statement or watermark DDL>"
}}"""

        recommendation = await self.reason(prompt, {"job": details, "context": task.context})

        return {
            "status": "recommendation_ready",
            "job_id": job_id,
            "recommendation": recommendation,
        }

    async def _check_partition_balance(self, task: AgentTask) -> dict:
        """Check Kafka partition throughput balance for a topic."""
        topic_name = task.context.get("topic_name", "")

        if not topic_name:
            return {"status": "error", "message": "Provide topic_name in task context to check partition balance."}

        topic_json = await self.call_tool("kafka_topic_info", topic_name=topic_name)
        topic = json.loads(topic_json)

        prompt = f"""Analyze this Kafka topic for partition imbalance.

Topic Info: {json.dumps(topic, indent=2)}

Identify:
1. Partitions with significantly higher/lower throughput
2. Recommended rebalancing strategy (increase partitions, change key, etc.)
3. Severity (LOW/MEDIUM/HIGH)

Return JSON: {{
  "imbalance_detected": true/false,
  "severity": "LOW|MEDIUM|HIGH",
  "recommendations": [<list of actions>],
  "summary": "<plain English>"
}}"""

        analysis = await self.reason(prompt, {"topic": topic})

        return {
            "status": "partition_analysis_complete",
            "topic": topic_name,
            "analysis": analysis,
        }
