"""Run dbt models (compile SQL and execute against database).

This module implements the run_models tool for dbt Core MCP.
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
    async def run_models(
        ctx: Context,
        select: str | None = None,
        exclude: str | None = None,
        select_state_modified: bool = False,
        select_state_modified_plus_downstream: bool = False,
        full_refresh: bool = False,
        fail_fast: bool = False,
        check_schema_changes: bool = False,
        cache_selected_only: bool = True,
    ) -> dict[str, Any]:
        """Run dbt models (compile SQL and execute against database).

        **What are models**: SQL files (.sql) containing SELECT statements that define data transformations.
        Models are compiled and executed to create/update tables and views in your database.

        **Important**: This tool runs models only (SQL files). For CSV seed files, use load_seeds().
        For running everything together (seeds + models + tests), use build_models().

        State-based selection modes (uses dbt state:modified selector):
        - select_state_modified: Run only models modified since last successful run (state:modified)
        - select_state_modified_plus_downstream: Run modified + downstream dependencies (state:modified+)
          Note: Requires select_state_modified=True

        Manual selection (alternative to state-based):
        - select: dbt selector syntax (e.g., "customers", "tag:mart", "stg_*")
        - exclude: Exclude specific models

        Args:
            select: Manual selector (e.g., "customers", "tag:mart", "path:marts/*")
            exclude: Exclude selector (e.g., "tag:deprecated")
            select_state_modified: Use state:modified selector (changed models only)
            select_state_modified_plus_downstream: Extend to state:modified+ (changed + downstream)
            full_refresh: Force full refresh of incremental models
            fail_fast: Stop execution on first failure
            check_schema_changes: Detect schema changes and recommend downstream runs
            cache_selected_only: Only cache schemas for selected models (default True for performance)

        Returns:
            Execution results with status, models run, timing info, and optional schema_changes

        See also:
            - seed_data(): Load CSV files (must run before models that reference them)
            - build_models(): Run models + tests together in DAG order
            - test_models(): Run tests after models complete

        Examples:
            # Run a specific model
            run_models(select="customers")

            # After loading seeds, run dependent models
            seed_data()
            run_models(select="stg_orders")

            # Incremental: run only what changed
            run_models(select_state_modified=True)

            # Run changed models + everything downstream
            run_models(select_state_modified=True, select_state_modified_plus_downstream=True)

            # Full refresh marts (rebuild from scratch)
            run_models(select="tag:mart", full_refresh=True)
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
            check_schema_changes,
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
    check_schema_changes: bool,
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
        check_schema_changes: Detect schema changes
        cache_selected_only: Only cache selected model schemas
        state: Shared state object

    Returns:
        Dictionary with execution results
    """

    # Prepare state-based selection (validates and returns selector)
    selector = await state.prepare_state_based_selection(select_state_modified, select_state_modified_plus_downstream, select)

    # Early return if state-based requested but no state exists
    if select_state_modified and not selector:
        raise RuntimeError("No previous state found - cannot determine modifications. Run 'dbt run' or 'dbt build' first to create baseline state.")

    # Build command args
    args = ["run"]

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

    # Capture pre-run table columns for schema change detection
    # Also get expected count of models for progress reporting
    pre_run_columns: dict[str, list[str]] = {}
    expected_total: int | None = None

    if check_schema_changes or True:  # Always get count for progress
        # Use dbt list to get models that will be run (without actually running them)
        list_args = ["list", "--resource-type", "model", "--output", "name"]

        if select_state_modified:
            selector = "state:modified+" if select_state_modified_plus_downstream else "state:modified"
            list_args.extend(["-s", selector, "--state", "target/state_last_run"])
        elif select:
            list_args.extend(["-s", select])

        if exclude:
            list_args.extend(["--exclude", exclude])

        # Get list of models
        logger.info(f"Getting model list: {list_args}")
        runner = await state.get_runner()
        list_result = await runner.invoke(list_args)  # type: ignore

        if list_result.success and list_result.stdout:
            model_count = 0
            # Parse model names from output (one per line with --output name)
            for line in list_result.stdout.strip().split("\n"):
                line = line.strip()
                # Skip log lines, timestamps, empty lines, and JSON output
                if (
                    not line
                    or line.startswith("{")
                    or ":" in line[:10]  # Timestamp like "07:39:44"
                    or "Running with dbt=" in line
                    or "Registered adapter:" in line
                ):
                    continue
                model_count += 1

                # For schema change detection, query pre-run columns
                if check_schema_changes:
                    model_name = line
                    logger.info(f"Querying pre-run columns for {model_name}")
                    cols = await state.get_table_columns_from_db(model_name)
                    if cols:
                        pre_run_columns[model_name] = cols
                    else:
                        # Table doesn't exist yet - mark as new
                        pre_run_columns[model_name] = []

            # Set expected total from model count
            if model_count > 0:
                expected_total = model_count
                logger.info(f"Expected total models to run: {expected_total}")

    # Execute with progress reporting
    logger.info(f"Running dbt models with args: {args}")
    logger.info(f"Expected total for progress: {expected_total}")

    # Define progress callback if context available
    async def progress_callback(current: int, total: int, message: str) -> None:
        if ctx:
            await ctx.report_progress(progress=current, total=total, message=message)

    # Delete stale run_results.json to ensure we only read fresh results
    state.clear_stale_run_results()

    runner = await state.get_runner()
    result = await runner.invoke(args, progress_callback=progress_callback if ctx else None, expected_total=expected_total)  # type: ignore

    # Parse run_results.json to discriminate system errors from business outcomes
    run_results = state.validate_and_parse_results(result, "run")

    # Business outcome - dbt executed models (some may have failed)
    # Continue with existing run_results parsing

    # Check for schema changes if requested
    schema_changes: dict[str, dict[str, list[str]]] = {}
    if check_schema_changes and pre_run_columns:
        logger.info("Detecting schema changes by comparing pre/post-run database columns")

        for model_name, old_columns in pre_run_columns.items():
            # Query post-run columns from database
            new_columns = await state.get_table_columns_from_db(model_name)

            if not new_columns:
                # Model failed to build or was skipped
                continue

            # Compare columns
            added = [c for c in new_columns if c not in old_columns]
            removed = [c for c in old_columns if c not in new_columns] if old_columns else []

            if added or removed:
                schema_changes[model_name] = {}
                if added:
                    schema_changes[model_name]["added"] = added
                if removed:
                    schema_changes[model_name]["removed"] = removed

    # Save state on success for next modified run
    if result.success:
        await state.save_execution_state()

    # Send final progress update with run summary
    results_list = run_results.get("results", [])
    await state.report_final_progress(ctx, results_list, "Run", "models")

    # Empty results means selector matched nothing - this is an error
    if not results_list:
        raise RuntimeError(f"No models matched selector: {select or selector or 'all'}")

    response: dict[str, Any] = {
        "status": "success",
        "command": " ".join(args),
        "results": run_results.get("results", []),
        "elapsed_time": run_results.get("elapsed_time"),
    }

    if schema_changes:
        response["schema_changes"] = schema_changes
        response["recommendation"] = "Schema changes detected. Consider running downstream models with modified_downstream=True to propagate changes."

    return response
