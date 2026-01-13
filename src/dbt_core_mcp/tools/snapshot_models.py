"""Snapshot models (capture historical changes).

This module implements the snapshot_models tool for dbt Core MCP.
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
    select: str | None,
    exclude: str | None,
    state: DbtCoreServerContext,
) -> dict[str, Any]:
    """Implementation function for snapshot_models tool.

    Separated for testing purposes - tests call this directly with explicit state.
    The @tool() decorated snapshot_models() function calls this with injected dependencies.
    """
    # Ensure dbt components are initialized
    await state.ensure_initialized(ctx, force_parse=False)

    # Construct dbt CLI args for snapshot
    args = ["snapshot"]

    if select:
        args.extend(["-s", select])

    if exclude:
        args.extend(["--exclude", exclude])

    logger.info(f"Running DBT snapshot with args: {args}")

    # Clear stale run_results so we parse only fresh output
    state.clear_stale_run_results()

    runner = await state.get_runner()
    result = await runner.invoke(args)  # type: ignore

    run_results = state.validate_and_parse_results(result, "snapshot")

    if ctx:
        if run_results.get("results"):
            results_list = run_results["results"]
            total = len(results_list)
            passed_count = sum(1 for r in results_list if r.get("status") == "success")
            failed_count = sum(1 for r in results_list if r.get("status") in ("error", "fail"))

            # Summarize final progress back to caller
            parts = []
            if passed_count > 0:
                parts.append(f"✅ {passed_count} passed" if failed_count > 0 else "✅ All passed")
            if failed_count > 0:
                parts.append(f"❌ {failed_count} failed")

            summary = f"Snapshot: {total}/{total} snapshots completed ({', '.join(parts)})"
            await ctx.report_progress(progress=total, total=total, message=summary)
        else:
            await ctx.report_progress(progress=0, total=0, message="0 snapshots matched selector")

    if not run_results.get("results"):
        raise RuntimeError(f"No snapshots matched selector: {select or 'all'}")

    return {
        "status": "success",
        "command": " ".join(args),
        "results": run_results.get("results", []),
        "elapsed_time": run_results.get("elapsed_time"),
    }


@dbtTool()
async def snapshot_models(
    ctx: Context,
    select: str | None = None,
    exclude: str | None = None,
    state: DbtCoreServerContext = Depends(get_state),
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
        state: Shared state object injected by FastMCP

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
    return await _implementation(ctx, select, exclude, state)
