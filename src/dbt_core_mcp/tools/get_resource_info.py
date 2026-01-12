"""Get detailed information about any dbt resource.

This module implements the get_resource_info tool for dbt Core MCP.
"""

import logging
from typing import Any

from fastmcp import FastMCP
from fastmcp.server.context import Context

from ..server import DbtCoreServerContext

logger = logging.getLogger(__name__)


def setup(app: FastMCP, state: DbtCoreServerContext) -> None:
    """Register this tool with the MCP server.

    Called automatically by server._register_tools() during initialization.

    Args:
        app: FastMCP instance
        state: Shared state object accessible to all tools
    """

    @app.tool()
    async def get_resource_info(
        ctx: Context,
        name: str,
        resource_type: str | None = None,
        include_database_schema: bool = True,
        include_compiled_sql: bool = True,
    ) -> dict[str, Any]:
        """Get detailed information about any dbt resource (model, source, seed, snapshot, test, etc.).

        This unified tool works across all resource types, auto-detecting the resource or filtering by type.
        Designed for LLM consumption - returns complete data even when multiple matches exist.

        Args:
            name: Resource name. For sources, use "source_name.table_name" or just "table_name"
            resource_type: Optional filter to narrow search:
                - "model": Data transformation models
                - "source": External data sources
                - "seed": CSV reference data files
                - "snapshot": SCD Type 2 historical tables
                - "test": Data quality tests
                - "analysis": Ad-hoc analysis queries
                - None: Auto-detect (searches all types)
            include_database_schema: If True (default), query actual database table schema
                for models/seeds/snapshots/sources and add as 'database_columns' field
            include_compiled_sql: If True (default), include compiled SQL with Jinja resolved
                ({{ ref() }}, {{ source() }} → actual table names). Only applicable to models.
                Will trigger dbt compile if not already compiled. Set to False to skip compilation.

        Returns:
            Resource information dictionary. If multiple matches found, returns:
            {"multiple_matches": True, "matches": [...], "message": "..."}

        Raises:
            ValueError: If resource not found
        """
        # Initialize state if needed (metadata tool uses force_parse=True)
        await state.ensure_initialized(ctx, force_parse=True)

        # Call implementation function (pure logic)
        return await _implementation(name, resource_type, include_database_schema, include_compiled_sql, state)


async def _implementation(
    name: str,
    resource_type: str | None,
    include_database_schema: bool,
    include_compiled_sql: bool,
    state: DbtCoreServerContext,
) -> dict[str, Any]:
    """Implementation logic - separated for testability.

    Args:
        name: Resource name to retrieve
        resource_type: Optional filter by resource type
        include_database_schema: Whether to query actual database schema
        include_compiled_sql: Whether to include compiled SQL (triggers compile if needed)
        state: Shared state object

    Returns:
        Dictionary with resource information

    Raises:
        ValueError: If resource not found
    """
    try:
        # Get resource info with manifest method (handles basic enrichment)
        result = state.manifest.get_resource_info(  # type: ignore
            name,
            resource_type,
            include_database_schema=False,  # We'll handle this below for database schema
            include_compiled_sql=include_compiled_sql,
        )

        # Handle multiple matches case
        if result.get("multiple_matches"):
            # Enrich each match with database schema if requested
            if include_database_schema:
                matches = result.get("matches", [])
                for match in matches:
                    node_type = match.get("resource_type")
                    if node_type in ("model", "seed", "snapshot", "source"):
                        resource_name = match.get("name")
                        source_name = match.get("source_name") if node_type == "source" else None
                        schema = await state.get_table_schema_from_db(resource_name, source_name)
                        if schema:
                            match["database_columns"] = schema
            return result

        # Single match - check if we need to trigger compilation
        node_type = result.get("resource_type")

        if include_compiled_sql and node_type == "model":
            # If compiled SQL requested but not available, trigger compilation
            if result.get("compiled_sql") is None and not result.get("compiled_sql_cached"):
                logger.info(f"Compiling model: {name}")
                runner = await state.get_runner()
                compile_result = await runner.invoke_compile(name, force=False)  # type: ignore

                if compile_result.success:
                    # Reload manifest to get compiled code
                    await state.manifest.load()  # type: ignore
                    # Re-fetch the resource to get updated compiled_code
                    result = state.manifest.get_resource_info(  # type: ignore
                        name,
                        resource_type,
                        include_database_schema=False,
                        include_compiled_sql=True,
                    )

        # Query database schema for applicable resource types
        if include_database_schema and node_type in ("model", "seed", "snapshot", "source"):
            resource_name = result.get("name", name)
            # For sources, pass source_name to use source() instead of ref()
            source_name = result.get("source_name") if node_type == "source" else None
            schema = await state.get_table_schema_from_db(resource_name, source_name)
            if schema:
                result["database_columns"] = schema

        return result

    except ValueError as e:
        raise ValueError(f"Resource not found: {e}")
