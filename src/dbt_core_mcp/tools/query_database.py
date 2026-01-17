"""Execute SQL queries against the dbt project's database.

This module implements the query_database tool for dbt Core MCP.
"""

import csv
import io
import json
import logging
import re
import tempfile
from pathlib import Path
from typing import Any

from fastmcp.dependencies import Depends  # type: ignore[reportAttributeAccessIssue]
from fastmcp.server.context import Context

from ..context import DbtCoreServerContext
from ..cte_generator import generate_cte_model
from ..dependencies import get_state
from . import dbtTool

logger = logging.getLogger(__name__)


def extract_cte_sql(
    project_dir: Path,
    cte_name: str,
    model_name: str,
    additional_sql: str = "",
    model_paths: list[str] | None = None,
) -> str:
    """Extract CTE SQL from a model file and optionally append additional SQL.

    This function extracts a specific CTE from a dbt model file, resolves all its
    upstream dependencies, and optionally appends user-provided SQL for filtering.

    Args:
        project_dir: Path to the dbt project directory
        cte_name: Name of the CTE to extract
        model_name: Name of the model file containing the CTE (without .sql extension)
        additional_sql: Optional SQL to append (e.g., "WHERE x > 10 LIMIT 5")
        model_paths: List of model directory paths (defaults to ["models"])

    Returns:
        Complete SQL ready to execute (either the extracted CTE or wrapped with additional SQL)

    Raises:
        ValueError: If model file not found, multiple files found, or CTE extraction fails

    Examples:
        # Extract a CTE without additional SQL
        sql = extract_cte_sql(project_dir, "customer_agg", "customers")

        # Extract a CTE with filtering
        sql = extract_cte_sql(
            project_dir,
            "customer_agg",
            "customers",
            "WHERE order_count > 5 LIMIT 10"
        )
    """
    # Use default model_paths if not provided
    if model_paths is None:
        model_paths = ["models"]

    # Find the model file - search all configured model paths
    model_files = []
    for model_path in model_paths:
        models_dir = project_dir / model_path
        if models_dir.exists():
            model_files.extend(list(models_dir.rglob(f"{model_name}.sql")))

    if not model_files:
        paths_searched = ", ".join(model_paths)
        raise ValueError(f"Model file '{model_name}.sql' not found in any model paths: {paths_searched}")

    if len(model_files) > 1:
        raise ValueError(f"Multiple model files found for '{model_name}': {[str(f) for f in model_files]}")

    model_file = model_files[0]
    logger.info(f"Extracting CTE '{cte_name}' from model '{model_name}' at {model_file}")

    # Create a temporary file for the extracted CTE model
    # Use system temp directory to avoid dbt picking it up as a model
    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as tmp_file:
        tmp_path = Path(tmp_file.name)

        try:
            # Generate CTE model using the generator logic
            # The generate_cte_model expects empty test_given list when we're just extracting
            success = generate_cte_model(
                base_model_path=model_file,
                cte_name=cte_name,
                test_given=[],  # No CTE mocking when querying
                output_path=tmp_path,
            )

            if not success:
                raise ValueError(f"Failed to extract CTE '{cte_name}' from model '{model_name}'")

            # Read the generated CTE SQL
            cte_sql = tmp_path.read_text()

            # Remove the sqlfluff disable comment if present
            cte_sql = re.sub(r"^-- sqlfluff:disable\s*\n", "", cte_sql, flags=re.MULTILINE)

            # If user provided additional SQL, replace the final SELECT with a subquery
            # The CTE extraction already includes "select * from {cte_name}" at the end
            if additional_sql and additional_sql.strip():
                # Replace "select * from {cte_name}" with "select * from ({original_cte}) as _cte {additional_sql}"
                # This allows appending WHERE, ORDER BY, LIMIT, etc.
                pattern = rf"select \* from {re.escape(cte_name)}$"
                replacement = f"select * from {cte_name} {additional_sql}"
                final_sql = re.sub(pattern, replacement, cte_sql, flags=re.IGNORECASE | re.MULTILINE)
            else:
                # Use the CTE SQL as-is (already has select * from cte_name)
                final_sql = cte_sql

            logger.debug(f"Final SQL to execute:\n{final_sql[:500]}...")
            return final_sql

        finally:
            # Clean up temporary file
            # Use a small delay to allow processes to release the file
            import time

            time.sleep(0.1)
            try:
                if tmp_path.exists():
                    tmp_path.unlink()
            except PermissionError:
                # File is still in use (Windows), try to delete it later
                logger.debug(f"Temporary CTE file {tmp_path} still in use, will be cleaned up by OS")
            except Exception as e:
                logger.warning(f"Failed to delete temporary CTE file {tmp_path}: {e}")


async def _implementation(
    ctx: Context | None,
    sql: str,
    output_file: str | None,
    output_format: str,
    cte_name: str | None,
    model_name: str | None,
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

    # Handle CTE query if cte_name is provided
    if cte_name:
        if not model_name:
            raise ValueError("model_name is required when querying a CTE (cte_name is specified)")

        if not state.project_dir:
            raise ValueError("Project directory not initialized")

        # Get configured model paths
        model_paths = state.get_project_paths()["model-paths"]

        # Extract CTE SQL using the dedicated function
        sql = extract_cte_sql(
            project_dir=state.project_dir,
            cte_name=cte_name,
            model_name=model_name,
            additional_sql=sql,
            model_paths=model_paths,
        )

    # Execute query using dbt show with --no-populate-cache for optimal performance
    runner = await state.get_runner()
    result = await runner.invoke_query(sql, progress_callback=progress_callback if ctx else None)  # type: ignore

    if not result.success:
        error_msg = str(result.exception) if result.exception else "Unknown error"
        # Include dbt output in error message for context
        full_error = error_msg

        # Try to extract error from stdout or stderr
        if result.stdout and "Error" in result.stdout:
            full_error = result.stdout
        elif hasattr(result, "stderr") and result.stderr:
            full_error = result.stderr

        logger.error(f"Query execution failed. Error: {error_msg}, stdout: {result.stdout if hasattr(result, 'stdout') else 'N/A'}")
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
    cte_name: str | None = None,
    model_name: str | None = None,
    state: DbtCoreServerContext = Depends(get_state),
) -> dict[str, Any]:
    """Execute a SQL query against the dbt project's database.

    This tool compiles and runs SQL with Jinja templating support, allowing you to use
    {{ ref('model') }} and {{ source('src', 'table') }} in your queries.

    **SQL Templating**:
    - Use {{ ref('model_name') }} to reference dbt models
    - Use {{ source('source_name', 'table_name') }} to reference source tables
    - dbt compiles these to actual table names before execution

    **CTE Querying** (via parameters, NOT in SQL):
    - Use cte_name="cte_name" and model_name="model_name" parameters (NOT inside the SQL string)
    - The tool extracts the CTE and all its upstream dependencies from the model file
    - Handles all {{ ref() }} and {{ source() }} resolution automatically
    - The 'sql' parameter becomes optional additional filtering (WHERE, ORDER BY, LIMIT)
    - IMPORTANT: Do NOT use {{ ref('model', cte='cte_name') }} - that syntax does not exist

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
             When using cte_name/model_name parameters, this becomes OPTIONAL additional SQL
             to append after the CTE (e.g., "WHERE x > 10 LIMIT 5" or just "LIMIT 10")
        output_file: Optional file path to save results. Recommended for large result sets (>100 rows).
                    If provided, only metadata is returned (no preview for CSV/TSV).
                    If omitted, all data is returned inline (may consume large context).
        output_format: Output format - "json" (default), "csv", or "tsv"
        cte_name: Optional CTE name to query from a model (requires model_name)
        model_name: Optional model name containing the CTE (required when cte_name is specified)
        state: Shared state object injected by FastMCP

    Returns:
        JSON inline: {"status": "success", "row_count": N, "rows": [...]}
        JSON file: {"status": "success", "row_count": N, "saved_to": "path", "preview": [...]}
        CSV/TSV inline: {"status": "success", "row_count": N, "format": "csv", "csv": "..."}
        CSV/TSV file: {"status": "success", "row_count": N, "format": "csv", "saved_to": "path"}

    Raises:
        RuntimeError: If query execution fails
        ValueError: If invalid CTE/model parameters provided

    Examples:
        # Simple query with ref()
        query_database(sql="SELECT * FROM {{ ref('customers') }} LIMIT 10")

        # Query with source()
        query_database(sql="SELECT * FROM {{ source('jaffle_shop', 'orders') }} LIMIT 5")

        # Aggregation (no LIMIT needed)
        query_database(sql="SELECT COUNT(*) as total FROM {{ ref('customers') }}")

        # Query a specific CTE from a model
        query_database(
            cte_name="customer_agg",
            model_name="customers",
            sql="LIMIT 10"  # Optional additional SQL
        )

        # Query a CTE with filtering
        query_database(
            cte_name="customer_agg",
            model_name="customers",
            sql="WHERE order_count > 5 LIMIT 20"
        )

        # WRONG - Do NOT use ref() with cte parameter (does not exist):
        # query_database(sql="SELECT * FROM {{ ref('model', cte='cte_name') }}")
        #
        # CORRECT - Use cte_name and model_name parameters instead:
        # query_database(cte_name="cte_name", model_name="model", sql="LIMIT 10")

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
    return await _implementation(ctx, sql, output_file, output_format, cte_name, model_name, state)
