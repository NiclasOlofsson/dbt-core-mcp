"""Install dbt packages defined in packages.yml.

This module implements the install_deps tool for dbt Core MCP.
"""

import logging
from typing import Any

from fastmcp.dependencies import Depends  # type: ignore[reportAttributeAccessIssue]
from fastmcp.server.context import Context

from . import dbtTool
from ..context import DbtCoreServerContext
from ..dependencies import get_state

logger = logging.getLogger(__name__)


async def _implementation(
    ctx: Context | None,
    state: DbtCoreServerContext,
) -> dict[str, Any]:
    """Implementation function for install_deps tool.

    Separated for testing purposes - tests call this directly with explicit state.
    The @tool() decorated install_deps() function calls this with injected dependencies.
    """  # Ensure dbt components are initialized
    await state.ensure_initialized(ctx, force_parse=False)
    # Execute dbt deps
    logger.info("Running dbt deps to install packages")

    runner = await state.get_runner()
    result = await runner.invoke(["deps"])

    if not result.success:
        # Bubble up installer failure with context
        raise RuntimeError(f"dbt deps failed: {result.exception}")

    # Parse installed packages from manifest
    installed_packages: set[str] = set()

    assert state.manifest is not None
    manifest_dict = state.manifest.get_manifest_dict()
    macros = manifest_dict.get("macros", {})
    project_name = manifest_dict.get("metadata", {}).get("project_name", "")

    for unique_id in macros:
        # macro.package_name.macro_name format
        if unique_id.startswith("macro."):
            parts = unique_id.split(".")
            if len(parts) >= 2:
                package_name = parts[1]
                # Exclude built-in dbt package and project package
                if package_name != "dbt" and package_name != project_name:
                    installed_packages.add(package_name)

    return {
        "status": "success",
        "command": "dbt deps",
        "installed_packages": sorted(installed_packages),
        "message": f"Successfully installed {len(installed_packages)} package(s)",
    }


@dbtTool()
async def install_deps(
    ctx: Context,
    state: DbtCoreServerContext = Depends(get_state),
) -> dict[str, Any]:
    """Install dbt packages defined in packages.yml.

    This tool enables interactive workflow where an LLM can:
    1. Suggest using a dbt package (e.g., dbt_utils)
    2. Edit packages.yml to add the package
    3. Run install_deps() to install it
    4. Write code that uses the package's macros

    This completes the recommendation workflow without breaking conversation flow.

    **When to use**:
    - After adding/modifying packages.yml
    - Before using macros from external packages
    - When setting up a new dbt project

    **Package Discovery**:
    After installation, use list_resources(resource_type="macro") to verify
    installed packages and discover available macros.

    Returns:
        Installation results with status and installed packages

    Example workflow:
        User: "Create a date dimension table"
        LLM: 1. Checks: list_resources(type="macro") -> no dbt_utils
             2. Edits: packages.yml (adds dbt_utils package)
             3. Runs: install_deps() (installs package)
             4. Creates: models/date_dim.sql (uses dbt_utils.date_spine)

    Note: This is an interactive development tool, not infrastructure automation.
    It enables the LLM to act on its own recommendations mid-conversation.
    """
    return await _implementation(ctx, state)
