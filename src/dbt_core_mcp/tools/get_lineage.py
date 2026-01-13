"""Get lineage (dependency tree) for any dbt resource.

This module implements the get_lineage tool for dbt Core MCP.
"""

import logging
from typing import Any

from fastmcp.dependencies import Depends  # type: ignore[reportAttributeAccessIssue]
from fastmcp.server.context import Context

from ..context import DbtCoreServerContext
from ..dependencies import get_state
from . import dbtTool

logger = logging.getLogger(__name__)


async def _implementation(
    ctx: Context | None,
    name: str,
    resource_type: str | None,
    direction: str,
    depth: int | None,
    state: DbtCoreServerContext,
    force_parse: bool = True,
) -> dict[str, Any]:
    """Implementation function for get_lineage tool.

    Separated for testing purposes - tests call this directly with explicit state.
    The @tool() decorated get_lineage() function calls this with injected dependencies.
    """
    # Initialize state if needed (metadata tool uses force_parse=True)
    await state.ensure_initialized(ctx, force_parse)

    # Delegate to manifest helper for lineage traversal
    try:
        return state.manifest.get_lineage(name, resource_type, direction, depth)  # type: ignore
    except ValueError as e:
        raise ValueError(f"Lineage error: {e}")


@dbtTool()
async def get_lineage(
    ctx: Context,
    name: str,
    resource_type: str | None = None,
    direction: str = "both",
    depth: int | None = None,
    state: DbtCoreServerContext = Depends(get_state),
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
    return await _implementation(ctx, name, resource_type, direction, depth, state)
