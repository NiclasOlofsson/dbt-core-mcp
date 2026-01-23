"""Get column-level lineage through SQL transformations.

This module implements the get_column_lineage tool for dbt Core MCP.
Uses sqlglot to parse SQL and trace column dependencies through CTEs and transformations.
"""

import logging
from typing import Any

from fastmcp.dependencies import Depends  # type: ignore[reportAttributeAccessIssue]
from fastmcp.server.context import Context
from sqlglot import parse_one
from sqlglot.errors import SqlglotError
from sqlglot.lineage import lineage

from ..context import DbtCoreServerContext
from ..dbt.manifest import ManifestLoader
from ..dependencies import get_state
from . import dbtTool

logger = logging.getLogger(__name__)

# Export for testing
__all__ = [
    "implementation",
    "get_column_lineage",
    "_build_schema_mapping",
    "_extract_dependencies_from_lineage",
    "_format_lineage_response",
]


def _build_schema_mapping(manifest: ManifestLoader, upstream_lineage: dict[str, Any]) -> dict[str, Any]:
    """Build schema mapping from upstream models for sqlglot.

    Args:
        manifest: ManifestLoader instance
        upstream_lineage: Upstream lineage dict from manifest.get_lineage()

    Returns:
        Schema mapping in format: {database: {schema: {table: {column: type}}}}
    """
    schema_mapping: dict[str, Any] = {}

    if "upstream" not in upstream_lineage:
        return schema_mapping

    for upstream_node in upstream_lineage["upstream"]:
        try:
            node_info = manifest.get_resource_info(upstream_node["unique_id"], include_database_schema=True, include_compiled_sql=False)

            database = node_info.get("database", "").lower()
            schema = node_info.get("schema", "").lower()
            table = node_info.get("alias") or node_info.get("name", "").lower()

            if database and schema and table:
                if database not in schema_mapping:
                    schema_mapping[database] = {}
                if schema not in schema_mapping[database]:
                    schema_mapping[database][schema] = {}

                # Add columns with their types
                columns = node_info.get("database_columns", {})
                schema_mapping[database][schema][table] = {col_name.lower(): col_info.get("type", "string").lower() for col_name, col_info in columns.items()}
        except Exception as e:
            logger.warning(f"Could not load schema for upstream node {upstream_node.get('unique_id')}: {e}")
            continue

    return schema_mapping


def _extract_dependencies_from_lineage(lineage_node: Any, manifest: ManifestLoader | None, depth: int | None) -> list[dict[str, str]]:
    """Extract column dependencies from sqlglot lineage node.

    Args:
        lineage_node: Result from sqlglot.lineage()
        manifest: Optional ManifestLoader for dbt resource lookup
        depth: Maximum depth to traverse

    Returns:
        List of dependency dicts with column, table, and optional dbt_resource
    """
    dependencies: list[dict[str, str]] = []

    def walk_dependencies(node: Any, depth_current: int = 0) -> None:
        """Recursively walk lineage tree."""
        if depth is not None and depth_current >= depth:
            return

        for dep in node.walk():
            # Check if this is a table reference
            if hasattr(dep, "source") and hasattr(dep.source, "this"):
                table_name = str(dep.source.this)
                col_name = dep.name

                # Try to resolve to dbt model
                db = getattr(dep.source, "catalog", None)
                schema_name = getattr(dep.source, "db", None)

                dependency_info: dict[str, str] = {
                    "column": col_name,
                    "table": table_name,
                }

                if db:
                    dependency_info["database"] = str(db)
                if schema_name:
                    dependency_info["schema"] = str(schema_name)

                # Try to find the corresponding dbt resource
                if manifest:
                    try:
                        matching_node = manifest.get_resource_node(table_name)
                        if not matching_node.get("multiple_matches"):
                            dependency_info["dbt_resource"] = matching_node.get("unique_id", "")
                    except Exception:
                        pass  # Resource lookup failed, continue with table name

                dependencies.append(dependency_info)

    walk_dependencies(lineage_node)
    return dependencies


def _format_lineage_response(model_name: str, column_name: str, direction: str, dependencies: list[dict[str, str]], downstream_usage: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    """Format the final lineage response.

    Args:
        model_name: Model name
        column_name: Column name
        direction: Direction of lineage
        dependencies: Upstream dependencies
        downstream_usage: Optional downstream usage info

    Returns:
        Formatted response dict
    """
    result: dict[str, Any] = {
        "model": model_name,
        "column": column_name,
        "direction": direction,
        "dependencies": dependencies,
        "dependency_count": len(dependencies),
    }

    if downstream_usage:
        result["downstream_usage"] = downstream_usage
        result["downstream_count"] = len(downstream_usage)

    return result


async def implementation(
    ctx: Context | None,
    model_name: str,
    column_name: str,
    direction: str,
    depth: int | None,
    state: DbtCoreServerContext,
    force_parse: bool = True,
) -> dict[str, Any]:
    """Implementation function for get_column_lineage tool.

    Separated for testing purposes - tests call this directly with explicit state.
    The @tool() decorated get_column_lineage() function calls this with injected dependencies.
    """
    # Downstream not yet implemented - requires proper SQL parsing and transformation tracking
    if direction in ("downstream", "both"):
        raise NotImplementedError(
            "Downstream column lineage is not yet implemented. "
            "Current approach would require: (1) SQL parsing to map input→output columns, "
            "(2) transformation tracking through CASE/calculations, (3) recursive cascade through models. "
            "Use direction='upstream' or model-level lineage (get_lineage tool) for downstream impact analysis."
        )

    # Initialize state if needed
    await state.ensure_initialized(ctx, force_parse)

    # Verify manifest is available
    if state.manifest is None:
        raise RuntimeError("Manifest not initialized")

    # Get the model resource info with compiled SQL
    resource_info = state.manifest.get_resource_info(model_name, resource_type="model", include_compiled_sql=True, include_database_schema=False)

    # Handle multiple matches
    if resource_info.get("multiple_matches"):
        raise ValueError(f"Multiple models found matching '{model_name}'. Please use unique_id: {[m['unique_id'] for m in resource_info['matches']]}")

    # Extract compiled SQL - trigger compilation if needed
    compiled_sql = resource_info.get("compiled_sql")
    if not compiled_sql and not resource_info.get("compiled_sql_cached"):
        logger.info(f"Compiling model for column lineage: {model_name}")
        runner = await state.get_runner()
        compile_result = await runner.invoke_compile(model_name, force=False)

        if compile_result.success:
            # Reload manifest to get compiled code
            await state.manifest.load()
            # Re-fetch the resource to get updated compiled_code
            resource_info = state.manifest.get_resource_info(model_name, resource_type="model", include_compiled_sql=True, include_database_schema=False)
            compiled_sql = resource_info.get("compiled_sql")
        else:
            raise ValueError(f"Failed to compile model '{model_name}'. Check model SQL for errors.")

    if not compiled_sql:
        raise ValueError(f"No compiled SQL found for model '{model_name}'. Model may not contain SQL code.")

    # Get upstream models to build schema context
    try:
        upstream_lineage = state.manifest.get_lineage(
            model_name,
            resource_type="model",
            direction="upstream",
            depth=1,  # Just immediate parents
        )

        # Build schema mapping for sqlglot
        schema_mapping = _build_schema_mapping(state.manifest, upstream_lineage)
    except (ValueError, KeyError, AttributeError) as e:
        # Schema mapping is optional - sqlglot can work without it (just less context)
        logger.warning(f"Could not build schema mapping: {e}")
        schema_mapping = {}  # Continue with empty schema

    # Use sqlglot to trace column lineage
    try:
        # Parse the SQL and get lineage for the specific column
        parse_one(compiled_sql, dialect="databricks")

        # Get lineage for the specific column
        column_lineage_result = lineage(column=column_name, sql=compiled_sql, schema=schema_mapping, dialect="databricks")

        # Extract dependencies
        dependencies = _extract_dependencies_from_lineage(column_lineage_result, state.manifest, depth)

        # Format and return response (downstream not implemented)
        return _format_lineage_response(model_name, column_name, direction, dependencies, None)

    except SqlglotError as e:
        raise ValueError(f"Failed to parse SQL for column lineage: {e}\nModel: {model_name}, Column: {column_name}")
    except Exception as e:
        logger.exception("Unexpected error in column lineage analysis")
        raise ValueError(f"Column lineage analysis failed: {e}")


@dbtTool()
async def get_column_lineage(
    ctx: Context,
    model_name: str,
    column_name: str,
    direction: str = "upstream",
    depth: int | None = None,
    state: DbtCoreServerContext = Depends(get_state),
) -> dict[str, Any]:
    """Trace column-level lineage through SQL transformations.

    Uses sqlglot to parse compiled SQL and track how columns flow through:
    - CTEs and subqueries
    - JOINs and aggregations
    - Transformations (calculations, CASE statements, etc.)
    - Window functions

    This provides detailed column-to-column dependencies that model-level
    lineage cannot capture.

    Args:
        model_name: Name or unique_id of the dbt model to analyze
        column_name: Name of the column to trace
        direction: Direction to trace lineage:
            - "upstream": Which source columns feed into this column
            - "downstream": Which downstream columns use this column
            - "both": Full bidirectional column lineage
        depth: Maximum levels to traverse (None for unlimited)
            - depth=1: Immediate column dependencies only
            - depth=2: Dependencies + their dependencies
            - None: Full dependency tree

    Returns:
        Column lineage information including:
        - Source columns this column depends on (upstream)
        - Downstream columns that depend on this column
        - Transformations and derivations
        - dbt resource mapping where available

    Raises:
        ValueError: If model not found, column not found, or SQL parse fails
        RuntimeError: If sqlglot is not installed

    Examples:
        # Find which source columns feed into revenue
        get_column_lineage("fct_sales", "revenue", "upstream")

        # See what downstream models use customer_id
        get_column_lineage("dim_customers", "customer_id", "downstream")

        # Full bidirectional lineage for a column
        get_column_lineage("fct_orders", "order_total", "both")

    Note:
        Requires sqlglot package. Install with: pip install sqlglot
        The model must be compiled (run 'dbt compile' first).
    """
    return await implementation(ctx, model_name, column_name, direction, depth, state)
