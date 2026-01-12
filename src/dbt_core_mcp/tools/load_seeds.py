"""Load seed data (CSV files) from seeds/ directory into database tables.

This module implements the load_seeds tool for dbt Core MCP.
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
    async def load_seeds(
        ctx: Context,
        select: str | None = None,
        exclude: str | None = None,
        select_state_modified: bool = False,
        select_state_modified_plus_downstream: bool = False,
        full_refresh: bool = False,
        show: bool = False,
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
        # Initialization handled by InitializationMiddleware
        # Call implementation function (pure logic)
        return await _implementation(
            ctx,
            select,
            exclude,
            select_state_modified,
            select_state_modified_plus_downstream,
            full_refresh,
            show,
            state,
        )


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
    """Implementation logic - separated for testability.

    Args:
        ctx: MCP context for progress reporting
        select: Manual selector
        exclude: Exclude selector
        select_state_modified: Use state-based modified selector
        select_state_modified_plus_downstream: Extend to modified+
        full_refresh: Truncate and reload
        show: Show preview of data
        state: Shared state object

    Returns:
        Dictionary with seed results
    """

    # Prepare state-based selection (validates and returns selector)
    selector = await state.prepare_state_based_selection(select_state_modified, select_state_modified_plus_downstream, select)

    # Early return if state-based requested but no state exists
    if select_state_modified and not selector:
        raise RuntimeError("No previous state found - cannot determine modifications. Run 'dbt seed' first to create baseline state.")

    # Build command args
    args = ["seed"]

    # Add selector if we have one (state-based or manual)
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

    # Execute with progress reporting
    logger.info(f"Running DBT seed with args: {args}")

    # Define progress callback if context available
    async def progress_callback(current: int, total: int, message: str) -> None:
        if ctx:
            await ctx.report_progress(progress=current, total=total, message=message)

    # Delete stale run_results.json to ensure we only read fresh results
    state.clear_stale_run_results()

    runner = await state.get_runner()
    result = await runner.invoke(args, progress_callback=progress_callback if ctx else None)  # type: ignore

    # Parse run_results.json to discriminate system errors from business outcomes
    run_results = state.validate_and_parse_results(result, "seed")

    # Business outcome - dbt executed successfully
    # Save state on success for next modified run
    if result.success:
        await state.save_execution_state()

    # Parse run_results.json for details
    run_results = state.parse_run_results()

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

            summary = f"Seed: {total}/{total} seeds completed ({', '.join(parts)})"
            await ctx.report_progress(progress=total, total=total, message=summary)
        else:
            await ctx.report_progress(progress=0, total=0, message="0 seeds matched selector")

    # Empty results means selector matched nothing - this is an error
    if not run_results.get("results"):
        raise RuntimeError(f"No seeds matched selector: {select or selector or 'all'}")

    return {
        "status": "success",
        "command": " ".join(args),
        "results": run_results.get("results", []),
        "elapsed_time": run_results.get("elapsed_time"),
    }
