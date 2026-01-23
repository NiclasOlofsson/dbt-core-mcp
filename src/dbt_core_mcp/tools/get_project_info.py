"""Get information about the dbt project with optional diagnostics.

This module implements the get_project_info tool for dbt Core MCP.
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
    run_debug: bool,
    state: DbtCoreServerContext,
    force_parse: bool = True,
) -> dict[str, Any]:
    """Implementation function for get_project_info tool.

    Separated for testing purposes - tests call this directly with explicit state.
    The @tool() decorated get_project_info() function calls this with injected dependencies.
    """
    # Initialize state if needed (metadata tool uses force_parse=True)
    await state.ensure_initialized(ctx, force_parse)

    try:
        # Collect manifest metadata for quick status check
        info = state.manifest.get_project_info()  # type: ignore
        info["project_dir"] = str(state.project_dir)
        info["profiles_dir"] = state.profiles_dir
        info["status"] = "ready"

        # Add dbt-core-mcp version
        from .. import __version__

        info["dbt_core_mcp_version"] = __version__

        # Optionally run full dbt debug for connectivity diagnostics
        if run_debug:
            runner = await state.get_runner()
            debug_result_obj = await runner.invoke(["debug"])  # type: ignore

            # Convert DbtRunnerResult to dictionary
            debug_result = {
                "success": debug_result_obj.success,
                "output": debug_result_obj.stdout if debug_result_obj.stdout else "",
            }

            # Normalize debug output into structured diagnostics
            diagnostics: dict[str, Any] = {
                "command_run": "dbt debug",
                "success": debug_result.get("success", False),
                "output": debug_result.get("output", ""),
            }

            # Extract connection status from output
            output = str(debug_result.get("output", ""))
            if "Connection test: [OK connection ok]" in output or "Connection test: OK" in output:
                diagnostics["connection_status"] = "ok"
            elif "Connection test: [ERROR" in output or "Connection test: FAIL" in output:
                diagnostics["connection_status"] = "failed"
            else:
                diagnostics["connection_status"] = "unknown"

            info["diagnostics"] = diagnostics

        return info

    except Exception as e:
        raise ValueError(f"Failed to get project info: {e}")


@dbtTool()
async def get_project_info(
    ctx: Context,
    run_debug: bool = True,
    state: DbtCoreServerContext = Depends(get_state),
) -> dict[str, Any]:
    """Get information about the dbt project with optional diagnostics.

    Args:
        ctx: MCP context (provided by FastMCP)
        run_debug: Run `dbt debug` to validate environment and test connection (default: True)
        state: Shared state object injected by FastMCP

    Returns:
        Dictionary with project information and diagnostic results
    """
    return await _implementation(ctx, run_debug, state)
