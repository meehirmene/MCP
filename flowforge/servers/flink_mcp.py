"""
FlowForge Flink MCP Server
Exposes tools for managing Apache Flink jobs via the REST API.
"""

import json
import logging

import httpx
from mcp.server.fastmcp import FastMCP

from flowforge.config import config

logger = logging.getLogger(__name__)

flink_mcp = FastMCP(
    "FlowForge Flink MCP",
    instructions="MCP server for Apache Flink — submit SQL, monitor jobs, manage cluster",
)


def _flink_url(path: str) -> str:
    return f"{config.flink.jobmanager_url}{path}"


async def _flink_get(path: str) -> dict:
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(_flink_url(path))
        resp.raise_for_status()
        return resp.json()


async def _flink_post(path: str, data: dict | None = None) -> dict:
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(_flink_url(path), json=data or {})
        resp.raise_for_status()
        return resp.json()


async def _flink_patch(path: str, data: dict | None = None) -> dict:
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.patch(_flink_url(path), json=data or {})
        resp.raise_for_status()
        return resp.json()


@flink_mcp.tool()
async def cluster_overview() -> str:
    """Get the Flink cluster overview including slots, jobs, and task managers."""
    try:
        overview = await _flink_get("/overview")
        return json.dumps(overview, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})


@flink_mcp.tool()
async def list_jobs() -> str:
    """List all Flink jobs with their status (RUNNING, FINISHED, FAILED, CANCELED)."""
    try:
        result = await _flink_get("/jobs/overview")
        jobs = []
        for job in result.get("jobs", []):
            jobs.append({
                "id": job.get("jid"),
                "name": job.get("name"),
                "state": job.get("state"),
                "start_time": job.get("start-time"),
                "duration": job.get("duration"),
            })
        return json.dumps({"jobs": jobs, "count": len(jobs)}, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})


@flink_mcp.tool()
async def get_job_details(job_id: str) -> str:
    """Get detailed information about a specific Flink job.

    Args:
        job_id: The Flink job ID
    """
    try:
        job = await _flink_get(f"/jobs/{job_id}")
        return json.dumps({
            "id": job.get("jid"),
            "name": job.get("name"),
            "state": job.get("state"),
            "start_time": job.get("start-time"),
            "duration": job.get("duration"),
            "vertices": [
                {
                    "name": v.get("name"),
                    "status": v.get("status"),
                    "parallelism": v.get("parallelism"),
                    "metrics": v.get("metrics", {}),
                }
                for v in job.get("vertices", [])
            ],
        }, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})


@flink_mcp.tool()
async def get_job_exceptions(job_id: str) -> str:
    """Get exceptions/errors from a Flink job for debugging.

    Args:
        job_id: The Flink job ID
    """
    try:
        result = await _flink_get(f"/jobs/{job_id}/exceptions")
        exceptions = []
        for exc in result.get("all-exceptions", []):
            exceptions.append({
                "exception": exc.get("exception"),
                "task": exc.get("task"),
                "location": exc.get("location"),
                "timestamp": exc.get("timestamp"),
            })
        return json.dumps({
            "job_id": job_id,
            "root_exception": result.get("root-exception"),
            "exceptions": exceptions,
        }, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})


@flink_mcp.tool()
async def cancel_job(job_id: str) -> str:
    """Cancel a running Flink job.

    Args:
        job_id: The Flink job ID to cancel
    """
    try:
        await _flink_patch(f"/jobs/{job_id}?mode=cancel")
        return json.dumps({"status": "canceling", "job_id": job_id})
    except Exception as e:
        return json.dumps({"error": str(e)})


@flink_mcp.tool()
async def submit_sql(sql_statement: str) -> str:
    """Submit a Flink SQL statement for execution.

    This creates a SQL session and executes the statement via the SQL Gateway.
    Supports CREATE TABLE, INSERT INTO, SELECT, and other Flink SQL statements.

    Args:
        sql_statement: The Flink SQL statement to execute
    """
    try:
        # Use Flink SQL Gateway REST API
        # Create session
        session_resp = await _flink_post("/v1/sessions", {
            "properties": {
                "execution.runtime-mode": "streaming",
            }
        })
        session_id = session_resp.get("sessionHandle")

        # Execute statement
        exec_resp = await _flink_post(
            f"/v1/sessions/{session_id}/statements",
            {"statement": sql_statement}
        )
        operation_handle = exec_resp.get("operationHandle")

        return json.dumps({
            "status": "submitted",
            "session_id": session_id,
            "operation_handle": operation_handle,
            "sql": sql_statement,
        }, indent=2)
    except httpx.HTTPStatusError as e:
        # If SQL Gateway is not available, provide guidance
        return json.dumps({
            "error": f"Flink SQL Gateway may not be available: {e.response.status_code}",
            "hint": "SQL Gateway needs to be enabled in Flink configuration. "
                    "The statement can still be submitted via Flink CLI.",
            "sql": sql_statement,
        })
    except Exception as e:
        return json.dumps({"error": str(e), "sql": sql_statement})


@flink_mcp.tool()
async def get_task_managers() -> str:
    """List all Flink task managers with their resource information."""
    try:
        result = await _flink_get("/taskmanagers")
        tms = []
        for tm in result.get("taskmanagers", []):
            tms.append({
                "id": tm.get("id"),
                "path": tm.get("path"),
                "slots_total": tm.get("slotsNumber"),
                "slots_free": tm.get("freeSlots"),
                "hardware": {
                    "cpus": tm.get("hardware", {}).get("cpuCores"),
                    "memory_mb": tm.get("hardware", {}).get("physicalMemory", 0) // (1024 * 1024),
                },
            })
        return json.dumps({"task_managers": tms, "count": len(tms)}, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})


def create_server() -> FastMCP:
    return flink_mcp


if __name__ == "__main__":
    flink_mcp.run()
