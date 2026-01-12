"""Get lineage (dependency tree) for any dbt resource.

This module implements the get_lineage tool for dbt Core MCP.
"""

import logging
from typing import Any

from fastmcp import FastMCP
from fastmcp.server.context import Context

from ..server import SharedState

logger = logging.getLogger(__name__)


def setup(app: FastMCP, state: SharedState) -> None:
    """Register this tool with the MCP server.

    Called automatically by server._register_tools() during initialization.

    Args:
        app: FastMCP instance
        state: Shared state object accessible to all tools
    """

    @app.tool()
    async def get_lineage(
        ctx: Context,
        name: str,
        resource_type: str | None = None,
        direction: str = "both",
        depth: int | None = None,
    ) -> dict[str, Any]:
        """Get lineage (dependency tree) for any dbt resource with auto-detection.

        This unified tool works across all resource types (models, sources, seeds, snapshots, etc.)
        showing upstream and/or downstream dependencies with configurable depth.

        Args:
            name: Resource name. For sources, use "source_name.table_name" or just "table_name"
                Examples: "customers", "jaffle_shop.orders", "raw_customers"
            resource_type: Optional filter to narrow search:
                - "model": Data transformation models
                - "source": External data sources
                - "seed": CSV reference data files
                - "snapshot": SCD Type 2 historical tables
                - "test": Data quality tests
                - "analysis": Ad-hoc analysis queries
                - None: Auto-detect (searches all types)
            direction: Lineage direction:
                - "upstream": Show where data comes from (parents)
                - "downstream": Show what depends on this resource (children)
                - "both": Show full lineage (default)
            depth: Maximum levels to traverse (None for unlimited)
                - depth=1: Immediate dependencies only
                - depth=2: Dependencies + their dependencies
                - None: Full dependency tree

        Returns:
            Lineage information with upstream/downstream nodes and statistics.
            If multiple matches found, returns all matches for LLM to process.

        Raises:
            ValueError: If resource not found or invalid direction

        Examples:
            get_lineage("customers") -> auto-detect and show full lineage
            get_lineage("customers", "model", "upstream") -> where customers model gets data
            get_lineage("jaffle_shop.orders", "source", "downstream", 2) -> 2 levels of dependents
        """
        # Initialize state if needed (metadata tool uses force_parse=True)
        await state.ensure_initialized(ctx, force_parse=True)

        # Call implementation function (pure logic)
        return await _implementation(name, resource_type, direction, depth, state)


async def _implementation(
    name: str,
    resource_type: str | None,
    direction: str,
    depth: int | None,
    state: SharedState,
) -> dict[str, Any]:
    """Implementation logic - separated for testability.

    Args:
        name: Resource name to analyze
        resource_type: Optional filter by resource type
        direction: "upstream", "downstream", or "both"
        depth: Maximum traversal depth (None for unlimited)
        state: Shared state object

    Returns:
        Dictionary with lineage information

    Raises:
        ValueError: If resource not found or invalid direction
    """
    try:
        return state.manifest.get_lineage(name, resource_type, direction, depth)  # type: ignore
    except ValueError as e:
        raise ValueError(f"Lineage error: {e}")
