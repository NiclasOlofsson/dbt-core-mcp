"""Run dbt build (execute models and tests together in dependency order).

This module implements the build_models tool for dbt Core MCP.
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
    resource_types: list[str] | None,
    fail_fast: bool,
    state: DbtCoreServerContext,
) -> dict[str, Any]:
    """Implementation function for build_models tool.

    Separated for testing purposes - tests call this directly with explicit state.
    The @tool() decorated build_models() function calls this with injected dependencies.
    """
    # Ensure dbt components are initialized
    await state.ensure_initialized(ctx, force_parse=False)

    # Build selector (state-based preferred when available)
    selector = await state.prepare_state_based_selection(select_state_modified, select_state_modified_plus_downstream, select)

    if select_state_modified and not selector:
        raise RuntimeError("No previous state found - cannot determine modifications. Run 'dbt build' first to create baseline state.")

    # Construct dbt CLI args for build
    args = ["build"]

    cache_selected_only = True
    if cache_selected_only and (select or selector or select_state_modified):
        args.append("--cache-selected-only")

    if selector:
        args.extend(["-s", selector, "--state", "target/state_last_run"])
    elif select:
        args.extend(["-s", select])

    if exclude:
        args.extend(["--exclude", exclude])

    if resource_types:
        for rt in resource_types:
            args.extend(["--resource-type", rt])

    if full_refresh:
        args.append("--full-refresh")

    if fail_fast:
        args.append("--fail-fast")

    logger.info(f"Running DBT build with args: {args}")

    # Stream progress back to MCP client (if provided)
    async def progress_callback(current: int, total: int, message: str) -> None:
        if ctx:
            await ctx.report_progress(progress=current, total=total, message=message)

    # Clear stale run_results so we parse only fresh output
    state.clear_stale_run_results()

    runner = await state.get_runner()
    result = await runner.invoke(args, progress_callback=progress_callback if ctx else None)  # type: ignore

    run_results = state.validate_and_parse_results(result, "build")

    if result and result.success:
        await state.save_execution_state()

    results_list = run_results.get("results", [])
    if ctx:
        if results_list:
            passed_count = sum(1 for r in results_list if r.get("status") in ("success", "pass"))
            failed_count = sum(1 for r in results_list if r.get("status") in ("error", "fail"))
            skip_count = sum(1 for r in results_list if r.get("status") == "skipped")

            total = len(results_list)
            parts = []
            if passed_count > 0:
                parts.append(f"✅ {passed_count} passed" if failed_count > 0 or skip_count > 0 else "✅ All passed")
            if failed_count > 0:
                parts.append(f"❌ {failed_count} failed")
            if skip_count > 0:
                parts.append(f"⏭️ {skip_count} skipped")

            summary = f"Build: {total}/{total} resources completed ({', '.join(parts)})"
            await ctx.report_progress(progress=total, total=total, message=summary)
        else:
            await ctx.report_progress(progress=0, total=0, message="0 resources matched selector")

    if not results_list:
        raise RuntimeError(f"No resources matched selector: {select or selector or 'all'}")

    return {
        "status": "success",
        "command": " ".join(args),
        "results": run_results.get("results", []),
        "elapsed_time": run_results.get("elapsed_time"),
    }


@dbtTool()
async def build_models(
    ctx: Context,
    select: str | None = None,
    exclude: str | None = None,
    select_state_modified: bool = False,
    select_state_modified_plus_downstream: bool = False,
    full_refresh: bool = False,
    resource_types: list[str] | None = None,
    fail_fast: bool = False,
    state: DbtCoreServerContext = Depends(get_state),
) -> dict[str, Any]:
    """Run dbt build (execute models and tests together in correct dependency order).

    **When to use**: This is the recommended "do everything" command that runs seeds, models,
    snapshots, and tests in the correct order based on your DAG. It automatically handles
    dependencies, so you don't need to run load_seeds() → run_models() → test_models() separately.

    **How it works**: Executes resources in dependency order:
    1. Seeds (if selected)
    2. Models (with their upstream dependencies)
    3. Tests (after their parent models complete)
    4. Snapshots (if selected)

    State-based selection modes (uses dbt state:modified selector):
    - select_state_modified: Build only resources modified since last successful run (state:modified)
    - select_state_modified_plus_downstream: Build modified + downstream dependencies (state:modified+)
      Note: Requires select_state_modified=True

    Manual selection (alternative to state-based):
    - select: dbt selector syntax (e.g., "customers", "tag:mart", "stg_*")
    - exclude: Exclude specific models

    Args:
        select: Manual selector
        exclude: Exclude selector
        select_state_modified: Use state:modified selector (changed resources only)
        select_state_modified_plus_downstream: Extend to state:modified+ (changed + downstream)
        full_refresh: Force full refresh of incremental models
        resource_types: Filter by resource types (model, test, seed, snapshot)
        fail_fast: Stop execution on first failure
        state: Shared state object injected by FastMCP

    Returns:
        Build results with status, models run/tested, and timing info

    See also:
        - run_models(): Run only models (no tests)
        - test_models(): Run only tests
        - load_seeds(): Run only seeds

    Examples:
        # Full project build (first-time setup or comprehensive run)
        build_models()

        # Build only what changed (efficient incremental workflow)
        build_models(select_state_modified=True)

        # Build changed resources + everything downstream
        build_models(select_state_modified=True, select_state_modified_plus_downstream=True)

        # Build specific model and its dependencies + tests
        build_models(select="customers")

        # Build all marts (includes their seed dependencies automatically)
        build_models(select="tag:mart")

        # Quick feedback: stop on first test failure
        build_models(fail_fast=True)
    """
    return await _implementation(ctx, select, exclude, select_state_modified, select_state_modified_plus_downstream, full_refresh, resource_types, fail_fast, state)
