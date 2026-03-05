"""
FlowForge CLI — Main entry point for the multi-agent data pipeline platform.

Usage:
    flowforge start          # Start all agents and services
    flowforge request "..."  # Send a natural language request
    flowforge health         # Check system health
    flowforge agents         # List active agents
    flowforge pipelines      # List active pipelines
    flowforge events         # Show recent agent events
"""

import asyncio
import json
import logging
import sys

import typer
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.syntax import Syntax
from rich.logging import RichHandler

from flowforge.agents.orchestrator import OrchestratorAgent
from flowforge.agents.shared.memory import AgentMemory

# Setup rich logging
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[RichHandler(rich_tracebacks=True)],
)
logger = logging.getLogger("flowforge")

app = typer.Typer(
    name="flowforge",
    help="🔥 FlowForge — Multi-Agent AI Data Pipeline Platform",
    no_args_is_help=True,
)
console = Console()

# Global orchestrator instance
_orchestrator: OrchestratorAgent | None = None


def get_orchestrator() -> OrchestratorAgent:
    global _orchestrator
    if _orchestrator is None:
        _orchestrator = OrchestratorAgent()
    return _orchestrator


@app.command()
def request(
    text: str = typer.Argument(..., help="Natural language request for the pipeline platform"),
):
    """Send a natural language request to FlowForge.

    Example: flowforge request "Stream orders from Postgres to Iceberg, aggregate by region"
    """
    console.print(Panel(
        f"[bold cyan]📨 Request:[/bold cyan] {text}",
        title="🔥 FlowForge",
        border_style="cyan",
    ))

    orchestrator = get_orchestrator()

    console.print("[dim]Orchestrator decomposing request into tasks...[/dim]\n")

    result = asyncio.run(orchestrator.handle_user_request(text))

    # Display results
    console.print(Panel(
        result.get("summary", "No summary available"),
        title="📋 Summary",
        border_style="green",
    ))

    # Tasks table
    table = Table(title="📋 Task Execution Log")
    table.add_column("Task", style="cyan")
    table.add_column("Agent", style="magenta")
    table.add_column("Status", style="green")

    for task_id, task_result in result.get("results", {}).items():
        status_color = "green" if task_result["status"] == "completed" else "red"
        table.add_row(
            task_result.get("description", task_id)[:60],
            task_result.get("agent", "?"),
            f"[{status_color}]{task_result['status']}[/{status_color}]",
        )

    console.print(table)
    console.print(f"\n[dim]Tasks: {result['tasks_executed']} total, "
                  f"{result['tasks_completed']} completed, "
                  f"{result['tasks_failed']} failed[/dim]")


@app.command()
def health():
    """Check the health of all pipeline components."""
    console.print("[bold]🏥 Running health check...[/bold]\n")

    from flowforge.agents.monitor import MonitorAgent
    from flowforge.agents.shared.base import AgentTask, AgentRole

    monitor = MonitorAgent()
    task = AgentTask(
        description="Full health check",
        assigned_to=AgentRole.MONITOR,
    )
    result = asyncio.run(monitor.execute_task(task))

    if result.result:
        health = result.result.get("health", result.result)
        overall = health.get("overall_status", "unknown")
        color = {"healthy": "green", "degraded": "yellow", "critical": "red"}.get(overall, "white")

        console.print(Panel(
            f"[bold {color}]Overall: {overall.upper()}[/bold {color}]",
            title="🏥 System Health",
            border_style=color,
        ))

        # Components table
        table = Table(title="Component Status")
        table.add_column("Component", style="cyan")
        table.add_column("Status", style="bold")
        table.add_column("Details")

        for name, info in health.get("components", {}).items():
            status = info.get("status", "unknown")
            s_color = {"healthy": "green", "degraded": "yellow", "unreachable": "red"}.get(status, "white")
            details_str = ", ".join(f"{k}: {v}" for k, v in info.items() if k != "status")
            table.add_row(name, f"[{s_color}]{status}[/{s_color}]", details_str[:80])

        console.print(table)

        # Issues
        issues = health.get("issues", [])
        if issues:
            console.print(f"\n[bold red]⚠️  {len(issues)} issue(s) found:[/bold red]")
            for issue in issues:
                console.print(f"  [{issue.get('severity', 'INFO')}] {issue.get('message', '')}")
    else:
        console.print("[red]Health check failed[/red]")


@app.command()
def agents():
    """List all active agents in the system."""
    console.print("[bold]🤖 Active Agents[/bold]\n")

    try:
        memory = AgentMemory()
        active = memory.get_active_agents()

        if active:
            table = Table(title="Agent Registry")
            table.add_column("Agent ID", style="cyan")
            table.add_column("Role", style="magenta")
            table.add_column("Capabilities", style="green")
            table.add_column("Tools", style="yellow")

            for agent in active:
                caps = agent.get("capabilities", [])
                tools = agent.get("tools", [])
                cap_count = len(caps) if isinstance(caps, list) else 0
                tool_count = len(tools) if isinstance(tools, list) else 0

                table.add_row(
                    str(agent.get("agent_id", "?")),
                    str(agent.get("role", "?")),
                    str(cap_count),
                    str(tool_count),
                )

            console.print(table)
        else:
            console.print("[dim]No agents registered. Run 'flowforge request' to initialize.[/dim]")
    except Exception as e:
        console.print(f"[red]Cannot connect to Redis: {e}[/red]")
        console.print("[dim]Make sure Docker services are running: docker compose up -d[/dim]")


@app.command()
def events(count: int = typer.Option(20, help="Number of events to show")):
    """Show recent agent activity events."""
    console.print(f"[bold]📜 Recent Events (last {count})[/bold]\n")

    try:
        memory = AgentMemory()
        recent = memory.get_events(count=count)

        if recent:
            for event in recent:
                agent = event.get("agent_role", "?")
                event_type = event.get("event_type", "?")
                details = event.get("details", {})
                ts = event.get("logged_at", "?")

                emoji = {
                    "USER_REQUEST_RECEIVED": "📨",
                    "TASK_COMPLETED": "✅",
                    "TASK_FAILED": "❌",
                    "ANOMALY_DETECTED": "⚠️",
                    "SCHEMA_DRIFT": "🔄",
                }.get(event_type, "📌")

                console.print(
                    f"  {emoji} [{agent}] {event_type}"
                    f" [dim]{json.dumps(details)[:80]}[/dim]"
                    f" [dim italic]{ts}[/dim italic]"
                )
        else:
            console.print("[dim]No events recorded yet.[/dim]")
    except Exception as e:
        console.print(f"[red]Cannot connect to Redis: {e}[/red]")


@app.command()
def pipelines():
    """List all active pipelines."""
    console.print("[bold]🔄 Active Pipelines[/bold]\n")

    try:
        memory = AgentMemory()
        pipeline_list = memory.list_pipelines()

        if pipeline_list:
            table = Table(title="Pipelines")
            table.add_column("Pipeline ID", style="cyan")
            table.add_column("Status", style="bold")
            table.add_column("Updated")

            for p in pipeline_list:
                status = p.get("status", "unknown")
                s_color = {
                    "running": "green", "building": "yellow",
                    "degraded": "red", "healing": "magenta",
                }.get(status, "white")

                table.add_row(
                    p.get("pipeline_id", "?"),
                    f"[{s_color}]{status}[/{s_color}]",
                    str(p.get("updated_at", "?"))[:19],
                )

            console.print(table)
        else:
            console.print("[dim]No pipelines deployed yet.[/dim]")
    except Exception as e:
        console.print(f"[red]Cannot connect to Redis: {e}[/red]")


@app.command()
def info():
    """Show FlowForge system information."""
    console.print(Panel(
        "[bold cyan]FlowForge[/bold cyan] — Multi-Agent AI Data Pipeline Platform\n\n"
        "[bold]Architecture:[/bold] Supervisory Hierarchy (Orchestrator → Specialists)\n"
        "[bold]Agents:[/bold] Orchestrator, Builder, Monitor, Healer\n"
        "[bold]MCP Servers:[/bold] PostgreSQL, Kafka, Flink, MongoDB\n"
        "[bold]Infrastructure:[/bold] Kafka, Flink, Iceberg, PostgreSQL, MongoDB, Redis\n"
        "[bold]Use Case:[/bold] E-commerce streaming data pipeline\n\n"
        "[dim]Run 'flowforge request \"<your request>\"' to get started[/dim]",
        title="🔥 FlowForge v0.1.0",
        border_style="cyan",
    ))


if __name__ == "__main__":
    app()
