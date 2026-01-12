"""Snapshot models (capture historical changes).

This module implements the snapshot_models tool for dbt Core MCP.
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
    async def snapshot_models(
        ctx: Context,
        select: str | None = None,
        exclude: str | None = None,
    ) -> dict[str, Any]:
        """Snapshot models (capture historical changes - SCD Type 2).

        Snapshots capture historical changes in data, enabling you to track slowly changing
        dimensions over time. This is particularly useful for maintaining accurate historical
        records in data warehouses.

        **When to use**: To track changes in slowly changing dimensions (SCD Type 2).
        For example, tracking customer address changes over time while preserving history.

        **How it works**: dbt compares current source data with existing snapshot table,
        identifies changes, and inserts new rows with validity timestamps (dbt_valid_from,
        dbt_valid_to, dbt_updated_at). Original rows are closed by setting dbt_valid_to.

        Args:
            select: dbt selector syntax (e.g., "snapshot_name", "tag:daily")
            exclude: Exclude specific snapshots

        Returns:
            Snapshot results with status and timing info

        Examples:
            # Run all snapshots
            snapshot_models()

            # Run specific snapshot
            snapshot_models(select="customers_snapshot")

            # Run tagged snapshots
            snapshot_models(select="tag:daily")
        """
        # Initialize state if needed (execution tool uses force_parse=False)
        await state.ensure_initialized(ctx, force_parse=False)

        # Call implementation function (pure logic)
        return await _implementation(ctx, select, exclude, state)


async def _implementation(
    ctx: Context | None,
    select: str | None,
    exclude: str | None,
    state: SharedState,
) -> dict[str, Any]:
    """Implementation logic - separated for testability.

    Args:
        ctx: MCP context for progress reporting
        select: Selector for snapshots to run
        exclude: Exclude selector
        state: Shared state object

    Returns:
        Dictionary with snapshot results
    """

    # Build command args
    args = ["snapshot"]

    if select:
        args.extend(["-s", select])

    if exclude:
        args.extend(["--exclude", exclude])

    # Execute
    logger.info(f"Running DBT snapshot with args: {args}")

    # Delete stale run_results.json to ensure we only read fresh results
    state.clear_stale_run_results()

    runner = await state.get_runner()
    result = await runner.invoke(args)  # type: ignore

    # Parse run_results.json to discriminate system errors from business outcomes
    run_results = state.validate_and_parse_results(result, "snapshot")

    # Send tool-specific final progress
    if ctx:
        if run_results.get("results"):
            results_list = run_results["results"]
            total = len(results_list)
            passed_count = sum(1 for r in results_list if r.get("status") == "success")
            failed_count = sum(1 for r in results_list if r.get("status") in ("error", "fail"))

            parts = []
            if passed_count > 0:
                parts.append(f"✅ {passed_count} passed" if failed_count > 0 else "✅ All passed")
            if failed_count > 0:
                parts.append(f"❌ {failed_count} failed")

            summary = f"Snapshot: {total}/{total} snapshots completed ({', '.join(parts)})"
            await ctx.report_progress(progress=total, total=total, message=summary)
        else:
            await ctx.report_progress(progress=0, total=0, message="0 snapshots matched selector")

    # Empty results means selector matched nothing - this is an error
    if not run_results.get("results"):
        raise RuntimeError(f"No snapshots matched selector: {select or 'all'}")

    return {
        "status": "success",
        "command": " ".join(args),
        "results": run_results.get("results", []),
        "elapsed_time": run_results.get("elapsed_time"),
    }
