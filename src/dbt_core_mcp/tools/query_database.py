"""Execute SQL queries against the dbt project's database.

This module implements the query_database tool for dbt Core MCP.
"""

import csv
import io
import json
import logging
import re
from pathlib import Path
from typing import Any

from fastmcp.dependencies import Depends  # type: ignore[reportAttributeAccessIssue]
from fastmcp.server.context import Context

from ..context import DbtCoreServerContext
from ..dependencies import get_state
from . import dbtTool

logger = logging.getLogger(__name__)


async def _implementation(
    ctx: Context | None,
    sql: str,
    output_file: str | None,
    output_format: str,
    state: DbtCoreServerContext,
) -> dict[str, Any]:
    """Implementation function for query_database tool.

    Separated for testing purposes - tests call this directly with explicit state.
    The @tool() decorated query_database() function calls this with injected dependencies.
    """
    # Ensure dbt components are initialized
    await state.ensure_initialized(ctx, force_parse=False)

    async def progress_callback(current: int, total: int, message: str) -> None:
        if ctx:
            await ctx.report_progress(progress=current, total=total, message=message)

    # Execute query using dbt show with --no-populate-cache for optimal performance
    runner = await state.get_runner()
    result = await runner.invoke_query(sql, progress_callback=progress_callback if ctx else None)  # type: ignore

    if not result.success:
        error_msg = str(result.exception) if result.exception else "Unknown error"
        # Include dbt output in error message for context
        full_error = error_msg
        if result.stdout and "Database Error" in result.stdout:
            # Extract the helpful database error details
            full_error = result.stdout
        raise RuntimeError(f"Query execution failed: {full_error}")

    # Parse JSON output from dbt show (extract the "show" payload)
    output = result.stdout if hasattr(result, "stdout") else ""

    try:
        # dbt show --output json returns: {"show": [...rows...]}
        # Find the JSON object (look for {"show": pattern)
        json_match = re.search(r'\{\s*"show"\s*:\s*\[', output)
        if not json_match:
            return {
                "status": "failed",
                "error": "No JSON output found in dbt show response",
            }

        # Use JSONDecoder to parse just the first complete JSON object
        # This handles extra data after the JSON (like log lines)
        decoder = json.JSONDecoder()
        data, _ = decoder.raw_decode(output, json_match.start())

        if "show" in data:
            rows = data["show"]
            row_count = len(rows)

            # Handle different output formats
            if output_format in ("csv", "tsv"):
                # Convert to CSV/TSV format
                delimiter = "\t" if output_format == "tsv" else ","
                csv_buffer = io.StringIO()

                if rows:
                    writer = csv.DictWriter(csv_buffer, fieldnames=rows[0].keys(), delimiter=delimiter)
                    writer.writeheader()
                    writer.writerows(rows)
                    csv_string = csv_buffer.getvalue()
                else:
                    csv_string = ""

                if output_file:
                    # Save to file
                    output_path = Path(output_file)
                    output_path.parent.mkdir(parents=True, exist_ok=True)

                    with open(output_path, "w", encoding="utf-8", newline="") as f:
                        f.write(csv_string)

                    # Get file size
                    file_size_bytes = output_path.stat().st_size
                    file_size_kb = file_size_bytes / 1024

                    return {
                        "status": "success",
                        "row_count": row_count,
                        "format": output_format,
                        "saved_to": str(output_path),
                        "file_size_kb": round(file_size_kb, 2),
                    }
                else:
                    # Return CSV/TSV inline
                    return {
                        "status": "success",
                        "row_count": row_count,
                        "format": output_format,
                        output_format: csv_string,
                    }
            else:
                # JSON format (default)
                if output_file:
                    # Ensure directory exists
                    output_path = Path(output_file)
                    output_path.parent.mkdir(parents=True, exist_ok=True)

                    # Write rows to file
                    with open(output_path, "w", encoding="utf-8") as f:
                        json.dump(rows, f, indent=2)

                    # Get file size
                    file_size_bytes = output_path.stat().st_size
                    file_size_kb = file_size_bytes / 1024

                    # Return metadata with preview
                    return {
                        "status": "success",
                        "row_count": row_count,
                        "saved_to": str(output_path),
                        "file_size_kb": round(file_size_kb, 2),
                        "columns": list(rows[0].keys()) if rows else [],
                        "preview": rows[:3],  # First 3 rows as preview
                    }
                else:
                    # Return all rows inline
                    return {
                        "status": "success",
                        "row_count": row_count,
                        "rows": rows,
                    }
        else:
            return {
                "status": "failed",
                "error": "Unexpected JSON format from dbt show",
                "data": data,
            }

    except json.JSONDecodeError as e:
        return {
            "status": "error",
            "message": f"Failed to parse query results: {e}",
            "raw_output": output[:500],
        }


@dbtTool()
async def query_database(
    ctx: Context,
    sql: str,
    output_file: str | None = None,
    output_format: str = "json",
    state: DbtCoreServerContext = Depends(get_state),
) -> dict[str, Any]:
    """Execute a SQL query against the dbt project's database.

    This tool compiles and runs SQL with Jinja templating support, allowing you to use
    {{ ref('model') }} and {{ source('src', 'table') }} in your queries.

    **SQL Templating**:
    - Use {{ ref('model_name') }} to reference dbt models
    - Use {{ source('source_name', 'table_name') }} to reference source tables
    - dbt compiles these to actual table names before execution

    **Output Management**:
    - For large result sets (>100 rows), use output_file to save results
    - If output_file is omitted, all data returns inline (may consume large context)
    - output_file is automatically created with parent directories

    **Output Formats**:
    - json (default): Returns data as JSON array of objects
    - csv: Returns comma-separated values with header row
    - tsv: Returns tab-separated values with header row
    - CSV/TSV formats use proper quoting (only when necessary) and are Excel-compatible

    Args:
        sql: SQL query with Jinja templating: {{ ref('model') }}, {{ source('src', 'table') }}
             For exploratory queries, include LIMIT. For aggregations/counts, omit it.
        output_file: Optional file path to save results. Recommended for large result sets (>100 rows).
                    If provided, only metadata is returned (no preview for CSV/TSV).
                    If omitted, all data is returned inline (may consume large context).
        output_format: Output format - "json" (default), "csv", or "tsv"
        state: Shared state object injected by FastMCP

    Returns:
        JSON inline: {"status": "success", "row_count": N, "rows": [...]}
        JSON file: {"status": "success", "row_count": N, "saved_to": "path", "preview": [...]}
        CSV/TSV inline: {"status": "success", "row_count": N, "format": "csv", "csv": "..."}
        CSV/TSV file: {"status": "success", "row_count": N, "format": "csv", "saved_to": "path"}

    Raises:
        RuntimeError: If query execution fails

    Examples:
        # Simple query with ref()
        query_database(sql="SELECT * FROM {{ ref('customers') }} LIMIT 10")

        # Query with source()
        query_database(sql="SELECT * FROM {{ source('jaffle_shop', 'orders') }} LIMIT 5")

        # Aggregation (no LIMIT needed)
        query_database(sql="SELECT COUNT(*) as total FROM {{ ref('customers') }}")

        # Save large results to file
        query_database(
            sql="SELECT * FROM {{ ref('orders') }}",
            output_file="temp_auto/orders_export.json"
        )

        # Export as CSV
        query_database(
            sql="SELECT * FROM {{ ref('customers') }}",
            output_file="temp_auto/customers.csv",
            output_format="csv"
        )
    """
    return await _implementation(ctx, sql, output_file, output_format, state)
