"""
FlowForge PostgreSQL MCP Server
Exposes tools for querying, schema inspection, and CDC setup on PostgreSQL.
"""

import json
import logging
from typing import Any

import psycopg2
import psycopg2.extras
from mcp.server.fastmcp import FastMCP

from flowforge.config import config

logger = logging.getLogger(__name__)

# Create MCP server instance
postgres_mcp = FastMCP(
    "FlowForge PostgreSQL MCP",
    instructions="MCP server for PostgreSQL operations — query, schema inspect, CDC setup",
)

def _get_conn():
    """Get a PostgreSQL connection."""
    return psycopg2.connect(
        host=config.postgres.host,
        port=config.postgres.port,
        database=config.postgres.database,
        user=config.postgres.user,
        password=config.postgres.password,
    )


@postgres_mcp.tool()
def query(sql: str, params: list[Any] | None = None) -> str:
    """Execute a SQL query on the PostgreSQL database and return results as JSON.

    Args:
        sql: SQL query to execute (SELECT, INSERT, UPDATE, DELETE)
        params: Optional list of parameters for parameterized queries
    """
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            if cur.description:
                rows = cur.fetchall()
                # Convert to serializable format
                result = []
                for row in rows:
                    result.append(
                        {k: str(v) if not isinstance(v, (int, float, bool, type(None))) else v
                         for k, v in dict(row).items()}
                    )
                return json.dumps({"rows": result, "count": len(result)}, indent=2)
            else:
                conn.commit()
                return json.dumps({"status": "success", "rows_affected": cur.rowcount})
    except Exception as e:
        conn.rollback()
        return json.dumps({"error": str(e)})
    finally:
        conn.close()


@postgres_mcp.tool()
def list_tables() -> str:
    """List all tables in the PostgreSQL database with their row counts."""
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    table_name,
                    pg_size_pretty(pg_total_relation_size(quote_ident(table_name))) as size,
                    (SELECT count(*) FROM information_schema.columns c
                     WHERE c.table_name = t.table_name AND c.table_schema = 'public') as column_count
                FROM information_schema.tables t
                WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
                ORDER BY table_name;
            """)
            tables = [dict(row) for row in cur.fetchall()]
            return json.dumps({"tables": tables}, indent=2)
    finally:
        conn.close()


@postgres_mcp.tool()
def inspect_schema(table_name: str) -> str:
    """Get the full schema of a PostgreSQL table including columns, types, and constraints.

    Args:
        table_name: Name of the table to inspect
    """
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Get columns
            cur.execute("""
                SELECT column_name, data_type, is_nullable, column_default,
                       character_maximum_length
                FROM information_schema.columns
                WHERE table_name = %s AND table_schema = 'public'
                ORDER BY ordinal_position;
            """, (table_name,))
            columns = [dict(row) for row in cur.fetchall()]

            # Get primary key
            cur.execute("""
                SELECT kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                WHERE tc.table_name = %s AND tc.constraint_type = 'PRIMARY KEY';
            """, (table_name,))
            pk_cols = [row["column_name"] for row in cur.fetchall()]

            # Get foreign keys
            cur.execute("""
                SELECT
                    kcu.column_name,
                    ccu.table_name AS foreign_table,
                    ccu.column_name AS foreign_column
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                JOIN information_schema.constraint_column_usage ccu
                    ON tc.constraint_name = ccu.constraint_name
                WHERE tc.table_name = %s AND tc.constraint_type = 'FOREIGN KEY';
            """, (table_name,))
            fks = [dict(row) for row in cur.fetchall()]

            # Get row count
            cur.execute(f"SELECT count(*) as count FROM {table_name};")
            row_count = cur.fetchone()["count"]

            return json.dumps({
                "table": table_name,
                "columns": columns,
                "primary_key": pk_cols,
                "foreign_keys": fks,
                "row_count": row_count,
            }, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})
    finally:
        conn.close()


@postgres_mcp.tool()
def check_cdc_status() -> str:
    """Check the CDC (logical replication) status of the PostgreSQL database."""
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Check WAL level
            cur.execute("SHOW wal_level;")
            wal_level = cur.fetchone()["wal_level"]

            # Check publications
            cur.execute("""
                SELECT pubname, puballtables, pubinsert, pubupdate, pubdelete
                FROM pg_publication;
            """)
            publications = [dict(row) for row in cur.fetchall()]

            # Check replication slots
            cur.execute("""
                SELECT slot_name, slot_type, active
                FROM pg_replication_slots;
            """)
            slots = [dict(row) for row in cur.fetchall()]

            return json.dumps({
                "wal_level": wal_level,
                "cdc_ready": wal_level == "logical",
                "publications": publications,
                "replication_slots": slots,
            }, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})
    finally:
        conn.close()


@postgres_mcp.tool()
def get_sample_data(table_name: str, limit: int = 5) -> str:
    """Get sample rows from a PostgreSQL table.

    Args:
        table_name: Name of the table to sample
        limit: Maximum number of rows to return (default 5)
    """
    return query(f"SELECT * FROM {table_name} LIMIT {limit}")


def create_server() -> FastMCP:
    """Return the MCP server instance."""
    return postgres_mcp


if __name__ == "__main__":
    postgres_mcp.run()
