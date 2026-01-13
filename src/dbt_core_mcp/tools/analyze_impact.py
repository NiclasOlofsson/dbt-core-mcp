"""Analyze the impact of changing any dbt resource.

This module implements the analyze_impact tool for dbt Core MCP.
"""

import logging
from typing import Any

from fastmcp.dependencies import Depends  # type: ignore[reportAttributeAccessIssue]
from fastmcp.server.context import Context
from fastmcp.tools import tool

from ..context import DbtCoreServerContext
from ..dependencies import get_state

logger = logging.getLogger(__name__)


async def _implementation(ctx: Context | None, name: str, resource_type: str | None, state: DbtCoreServerContext, force_parse: bool = True) -> dict[str, Any]:
    """Implementation function for analyze_impact tool.

    Separated for testing purposes - tests call this directly with explicit state.
    The @tool() decorated analyze_impact() function calls this with injected dependencies.
    """
    # Initialize state if needed (metadata tool uses force_parse=True)
    await state.ensure_initialized(ctx, force_parse)

    # Delegate to manifest helper for downstream impact calculation
    try:
        return state.manifest.analyze_impact(name, resource_type)  # type: ignore
    except ValueError as e:
        raise ValueError(f"Impact analysis error: {e}")


@tool()
async def analyze_impact(
    ctx: Context,
    name: str,
    resource_type: str | None = None,
    state: DbtCoreServerContext = Depends(get_state),
) -> dict[str, Any]:
    """Analyze the impact of changing any dbt resource with auto-detection.

    This unified tool works across all resource types (models, sources, seeds, snapshots, etc.)
    showing all downstream dependencies that would be affected by changes. Provides actionable
    recommendations for running affected resources.

    Args:
        name: Resource name. For sources, use "source_name.table_name" or just "table_name"
            Examples: "stg_customers", "jaffle_shop.orders", "raw_customers"
        resource_type: Optional filter to narrow search:
            - "model": Data transformation models
            - "source": External data sources
            - "seed": CSV reference data files
            - "snapshot": SCD Type 2 historical tables
            - "test": Data quality tests
            - "analysis": Ad-hoc analysis queries
            - None: Auto-detect (searches all types)

    Returns:
        Impact analysis with:
        - List of affected models by distance
        - Count of affected tests and other resources
        - Total impact statistics
        - Resources grouped by distance from changed resource
        - Recommended dbt command to run affected resources
        - Human-readable impact assessment message
        If multiple matches found, returns all matches for LLM to process.

    Raises:
        ValueError: If resource not found

    Examples:
        analyze_impact("stg_customers") -> auto-detect and show impact
        analyze_impact("jaffle_shop.orders", "source") -> impact of source change
        analyze_impact("raw_customers", "seed") -> impact of seed data change
    """
    return await _implementation(ctx, name, resource_type, state)
