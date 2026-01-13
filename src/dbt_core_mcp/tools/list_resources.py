"""List all resources in the dbt project.

This module implements the list_resources tool for dbt Core MCP.
"""

import logging
from typing import Any

from fastmcp.dependencies import Depends  # type: ignore[reportAttributeAccessIssue]
from fastmcp.server.context import Context
from fastmcp.tools import tool

from ..context import DbtCoreServerContext
from ..dependencies import get_state

logger = logging.getLogger(__name__)


async def _implementation(
    ctx: Context | None,
    resource_type: str | None,
    state: DbtCoreServerContext,
    force_parse: bool = True,
) -> list[dict[str, Any]]:
    """Implementation function for list_resources tool.

    Separated for testing purposes - tests call this directly with explicit state.
    The @tool() decorated list_resources() function calls this with injected dependencies.
    """
    # Initialize state if needed (metadata tool uses force_parse=True)
    await state.ensure_initialized(ctx, force_parse)

    # Return simplified manifest resources (LLM-friendly structure)
    return state.manifest.get_resources(resource_type)  # type: ignore


@tool()
async def list_resources(
    ctx: Context,
    resource_type: str | None = None,
    state: DbtCoreServerContext = Depends(get_state),
) -> list[dict[str, Any]]:
    """List all resources in the dbt project with optional filtering by type.

    This unified tool provides a consistent view across all dbt resource types.
    Returns simplified resource information optimized for LLM consumption.

    Args:
        resource_type: Optional filter to narrow results:
            - "model": Data transformation models
            - "source": External data sources
            - "seed": CSV reference data files
            - "snapshot": SCD Type 2 historical tables
            - "test": Data quality tests
            - "analysis": Ad-hoc analysis queries
            - "macro": Jinja macros (includes macros from installed packages)
            - None: Return all resources (default)

    Returns:
        List of resource dictionaries with consistent structure across types.
        Each resource includes: name, unique_id, resource_type, description, tags, etc.

    Package Discovery:
        Use resource_type="macro" to discover installed dbt packages.
        Macros follow the naming pattern: macro.{package_name}.{macro_name}

        Example - Check if dbt_utils is installed:
            macros = list_resources("macro")
            has_dbt_utils = any(m["unique_id"].startswith("macro.dbt_utils.") for m in macros)

        Example - List all installed packages:
            macros = list_resources("macro")
            packages = {m["unique_id"].split(".")[1] for m in macros
                       if m["unique_id"].startswith("macro.") and
                       m["unique_id"].split(".")[1] != "dbt"}

    Examples:
        list_resources() -> all resources
        list_resources("model") -> only models
        list_resources("source") -> only sources
        list_resources("test") -> only tests
        list_resources("macro") -> all macros (discover installed packages)
    """
    return await _implementation(ctx, resource_type, state)
