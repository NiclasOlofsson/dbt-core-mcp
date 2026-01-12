"""Get information about the dbt project with optional diagnostics.

This module implements the get_project_info tool for dbt Core MCP.
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
    async def get_project_info(
        ctx: Context,
        run_debug: bool = True,
    ) -> dict[str, Any]:
        """Get information about the dbt project with optional diagnostics.

        Args:
            ctx: MCP context (provided by FastMCP)
            run_debug: Run `dbt debug` to validate environment and test connection (default: True)

        Returns:
            Dictionary with project information and diagnostic results
        """
        # Initialize state if needed (metadata tool uses force_parse=True)
        await state.ensure_initialized(ctx, force_parse=True)

        # Call implementation function (pure logic)
        return await _implementation(run_debug, state)


async def _implementation(
    run_debug: bool,
    state: SharedState,
) -> dict[str, Any]:
    """Implementation logic - separated for testability.

    This is what gets unit tested - pure logic without MCP decorators.

    Args:
        run_debug: Whether to run dbt debug diagnostics
        state: Shared state object

    Returns:
        Dictionary with project info and optional diagnostics
    """
    try:
        # Get project info from manifest
        info = state.manifest.get_project_info()  # type: ignore
        info["project_dir"] = str(state.project_dir)
        info["profiles_dir"] = state.profiles_dir
        info["status"] = "ready"

        # Run full dbt debug if requested (default behavior)
        if run_debug:
            runner = await state.get_runner()
            debug_result_obj = await runner.invoke(["debug"])  # type: ignore

            # Convert DbtRunnerResult to dictionary
            debug_result = {
                "success": debug_result_obj.success,
                "output": debug_result_obj.stdout if debug_result_obj.stdout else "",
            }

            # Parse the debug output
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
