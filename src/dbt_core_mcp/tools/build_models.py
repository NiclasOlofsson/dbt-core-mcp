"""Run dbt build (execute models and tests together in dependency order).

This module implements the build_models tool for dbt Core MCP.
"""

import logging
from typing import Any

from fastmcp import FastMCP
from fastmcp.server.context import Context

from ..server import DbtCoreServerContext

logger = logging.getLogger(__name__)


def setup(app: FastMCP, state: DbtCoreServerContext) -> None:
    """Register this tool with the MCP server.

    Called automatically by server._register_tools() during initialization.

    Args:
        app: FastMCP instance
        state: Shared state object accessible to all tools
    """

    @app.tool()
    async def build_models(
        ctx: Context,
        select: str | None = None,
        exclude: str | None = None,
        select_state_modified: bool = False,
        select_state_modified_plus_downstream: bool = False,
        full_refresh: bool = False,
        fail_fast: bool = False,
        cache_selected_only: bool = True,
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
            fail_fast: Stop execution on first failure
            cache_selected_only: Only cache schemas for selected models (default True for performance)

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
        # Initialization handled by InitializationMiddleware
        # Call implementation function (pure logic)
        return await _implementation(
            ctx,
            select,
            exclude,
            select_state_modified,
            select_state_modified_plus_downstream,
            full_refresh,
            fail_fast,
            cache_selected_only,
            state,
        )


async def _implementation(
    ctx: Context | None,
    select: str | None,
    exclude: str | None,
    select_state_modified: bool,
    select_state_modified_plus_downstream: bool,
    full_refresh: bool,
    fail_fast: bool,
    cache_selected_only: bool,
    state: DbtCoreServerContext,
) -> dict[str, Any]:
    """Implementation logic - separated for testability.

    Args:
        ctx: MCP context for progress reporting
        select: Manual selector
        exclude: Exclude selector
        select_state_modified: Use state-based modified selector
        select_state_modified_plus_downstream: Extend to modified+
        full_refresh: Force full refresh
        fail_fast: Stop on first failure
        cache_selected_only: Only cache selected model schemas
        state: Shared state object

    Returns:
        Dictionary with build results
    """

    # Prepare state-based selection (validates and returns selector)
    selector = await state.prepare_state_based_selection(select_state_modified, select_state_modified_plus_downstream, select)

    # Early return if state-based requested but no state exists
    if select_state_modified and not selector:
        raise RuntimeError("No previous state found - cannot determine modifications. Run 'dbt build' first to create baseline state.")

    # Build command args
    args = ["build"]

    # Optimize cache: only cache schemas containing selected models (if enabled)
    # Default True for performance, can be disabled if full caching needed
    if cache_selected_only and (select or selector or select_state_modified):
        args.append("--cache-selected-only")

    # Add selector if we have one (state-based or manual)
    if selector:
        args.extend(["-s", selector, "--state", "target/state_last_run"])
    elif select:
        args.extend(["-s", select])

    if exclude:
        args.extend(["--exclude", exclude])

    if full_refresh:
        args.append("--full-refresh")

    if fail_fast:
        args.append("--fail-fast")

    # Execute with progress reporting
    logger.info(f"Running DBT build with args: {args}")

    # Define progress callback if context available
    async def progress_callback(current: int, total: int, message: str) -> None:
        if ctx:
            await ctx.report_progress(progress=current, total=total, message=message)

    # Delete stale run_results.json to ensure we only read fresh results
    state.clear_stale_run_results()

    runner = await state.get_runner()
    result = await runner.invoke(args, progress_callback=progress_callback if ctx else None)  # type: ignore

    # Parse run_results.json to discriminate system errors from business outcomes
    run_results = state.validate_and_parse_results(result, "build")

    # Business outcome - dbt executed successfully
    # Save state on success for next modified run
    if result and result.success:
        await state.save_execution_state()

    # Send final progress update with build summary
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

    # Empty results means selector matched nothing - this is an error
    if not results_list:
        raise RuntimeError(f"No resources matched selector: {select or selector or 'all'}")

    return {
        "status": "success",
        "command": " ".join(args),
        "results": run_results.get("results", []),
        "elapsed_time": run_results.get("elapsed_time"),
    }
