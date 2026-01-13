"""Load seed data (CSV files) from seeds/ directory into database tables.

This module implements the load_seeds tool for dbt Core MCP.
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
    select_state_modified: bool,
    select_state_modified_plus_downstream: bool,
    full_refresh: bool,
    show: bool,
    state: DbtCoreServerContext,
) -> dict[str, Any]:
    """Implementation function for load_seeds tool.

    Separated for testing purposes - tests call this directly with explicit state.
    The @tool() decorated load_seeds() function calls this with injected dependencies.
    """
    # Ensure dbt components are initialized
    await state.ensure_initialized(ctx, force_parse=False)

    # Build selector (state-based when available)
    selector = await state.prepare_state_based_selection(select_state_modified, select_state_modified_plus_downstream, select)

    if select_state_modified and not selector:
        raise RuntimeError("No previous state found - cannot determine modifications. Run 'dbt seed' first to create baseline state.")

    # Construct dbt CLI args for seed
    args = ["seed"]

    if selector:
        args.extend(["-s", selector, "--state", "target/state_last_run"])
    elif select:
        args.extend(["-s", select])

    if exclude:
        args.extend(["--exclude", exclude])

    if full_refresh:
        args.append("--full-refresh")

    if show:
        args.append("--show")

    logger.info(f"Running DBT seed with args: {args}")

    # Stream progress back to MCP client (if provided)
    async def progress_callback(current: int, total: int, message: str) -> None:
        if ctx:
            await ctx.report_progress(progress=current, total=total, message=message)

    # Clear stale run_results so we parse only fresh output
    state.clear_stale_run_results()

    runner = await state.get_runner()
    result = await runner.invoke(args, progress_callback=progress_callback if ctx else None)  # type: ignore

    run_results = state.validate_and_parse_results(result, "seed")

    if result.success:
        await state.save_execution_state()

    run_results = state.parse_run_results()

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

            summary = f"Seed: {total}/{total} seeds completed ({', '.join(parts)})"
            await ctx.report_progress(progress=total, total=total, message=summary)
        else:
            await ctx.report_progress(progress=0, total=0, message="0 seeds matched selector")

    if not run_results.get("results"):
        raise RuntimeError(f"No seeds matched selector: {select or selector or 'all'}")

    return {
        "status": "success",
        "command": " ".join(args),
        "results": run_results.get("results", []),
        "elapsed_time": run_results.get("elapsed_time"),
    }


@dbtTool()
async def load_seeds(
    ctx: Context,
    select: str | None = None,
    exclude: str | None = None,
    select_state_modified: bool = False,
    select_state_modified_plus_downstream: bool = False,
    full_refresh: bool = False,
    show: bool = False,
    state: DbtCoreServerContext = Depends(get_state),
) -> dict[str, Any]:
    """Load seed data (CSV files) from seeds/ directory into database tables.

    **When to use**: Run this before building models or tests that depend on reference data.
    Seeds must be loaded before models that reference them can execute.

    **What are seeds**: CSV files containing static reference data (country codes,
    product categories, lookup tables, etc.). Unlike models (which are .sql files),
    seeds are CSV files that are loaded directly into database tables.

    State-based selection modes (detects changed CSV files):
    - select_state_modified: Load only seeds modified since last successful run (state:modified)
    - select_state_modified_plus_downstream: Load modified + downstream dependencies (state:modified+)
      Note: Requires select_state_modified=True

    Manual selection (alternative to state-based):
    - select: dbt selector syntax (e.g., "raw_customers", "tag:lookup")
    - exclude: Exclude specific seeds

    Important: Change detection for seeds works via file hash comparison:
    - Seeds < 1 MiB: Content hash is compared (recommended)
    - Seeds >= 1 MiB: Only file path changes are detected (content changes ignored)
    For large seeds, use manual selection or run all seeds.

    Args:
        select: Manual selector for seeds
        exclude: Exclude selector
        select_state_modified: Use state:modified selector (changed seeds only)
        select_state_modified_plus_downstream: Extend to state:modified+ (changed + downstream)
        full_refresh: Truncate and reload seed tables (default behavior)
        show: Show preview of loaded data
        state: Shared state object injected by FastMCP

    Returns:
        Seed results with status and loaded seed info

    See also:
        - run_models(): Execute .sql model files (not CSV seeds)
        - build_models(): Runs both seeds and models together in DAG order
        - test_models(): Run tests (requires seeds to be loaded first if tests reference them)

    Examples:
        # Before running tests that depend on reference data
        load_seeds()
        test_models(select="test_customer_country_code")

        # After adding a new CSV lookup table
        load_seeds(select="new_product_categories")

        # Fix "relation does not exist" errors from models referencing seeds
        load_seeds()  # Load missing seed tables first
        run_models(select="stg_orders")

        # Incremental workflow: only reload what changed
        load_seeds(select_state_modified=True)

        # Full refresh of a specific seed
        load_seeds(select="country_codes", full_refresh=True)
    """
    return await _implementation(ctx, select, exclude, select_state_modified, select_state_modified_plus_downstream, full_refresh, show, state)
