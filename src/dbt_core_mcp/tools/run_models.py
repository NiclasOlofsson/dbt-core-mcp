"""Run dbt models (compile SQL and execute against database).

This module implements the run_models tool for dbt Core MCP.
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
    fail_fast: bool,
    check_schema_changes: bool,
    cache_selected_only: bool,
    state: DbtCoreServerContext,
) -> dict[str, Any]:
    """Implementation function for run_models tool.

    Separated for testing purposes - tests call this directly with explicit state.
    The @tool() decorated run_models() function calls this with injected dependencies.
    """
    # Ensure dbt components are initialized
    await state.ensure_initialized(ctx, force_parse=False)

    # Build selector (state-based if requested, otherwise manual)
    selector = await state.prepare_state_based_selection(select_state_modified, select_state_modified_plus_downstream, select)

    if select_state_modified and not selector:
        raise RuntimeError("No previous state found - cannot determine modifications. Run 'dbt run' or 'dbt build' first to create baseline state.")

    # Construct dbt CLI args for run
    args = ["run"]

    if cache_selected_only and (select or selector or select_state_modified):
        args.append("--cache-selected-only")

    if selector:
        target_path = state.get_project_paths().get("target-path", "target")
        args.extend(["-s", selector, "--state", f"{target_path}/state_last_run"])
    elif select:
        args.extend(["-s", select])

    if exclude:
        args.extend(["--exclude", exclude])

    if full_refresh:
        args.append("--full-refresh")

    if fail_fast:
        args.append("--fail-fast")

    pre_run_columns: dict[str, list[str]] = {}
    expected_total: int | None = None

    # Optional pre-run schema snapshot for change detection and accurate progress totals
    if check_schema_changes or True:
        list_args = ["list", "--resource-type", "model", "--output", "name"]

        if select_state_modified:
            selector = "state:modified+" if select_state_modified_plus_downstream else "state:modified"
            target_path = state.get_project_paths().get("target-path", "target")
            list_args.extend(["-s", selector, "--state", f"{target_path}/state_last_run"])
        elif select:
            list_args.extend(["-s", select])

        if exclude:
            list_args.extend(["--exclude", exclude])

        logger.info(f"Getting model list: {list_args}")
        runner = await state.get_runner()
        list_result = await runner.invoke(list_args)  # type: ignore

        if list_result.success and list_result.stdout:
            model_count = 0
            for line in list_result.stdout.strip().split("\n"):
                line = line.strip()
                if not line or line.startswith("{") or ":" in line[:10] or "Running with dbt=" in line or "Registered adapter:" in line:
                    continue
                model_count += 1

                if check_schema_changes:
                    model_name = line
                    logger.info(f"Querying pre-run columns for {model_name}")
                    cols = await state.get_table_columns_from_db(model_name)
                    if cols:
                        pre_run_columns[model_name] = cols
                    else:
                        pre_run_columns[model_name] = []

            if model_count > 0:
                expected_total = model_count
                logger.info(f"Expected total models to run: {expected_total}")

    logger.info(f"Running dbt models with args: {args}")
    logger.info(f"Expected total for progress: {expected_total}")

    # Stream progress back to MCP client (if provided)
    async def progress_callback(current: int, total: int, message: str) -> None:
        if ctx:
            await ctx.report_progress(progress=current, total=total, message=message)

    # Clear stale run_results so we parse only fresh output
    state.clear_stale_run_results()

    runner = await state.get_runner()
    result = await runner.invoke(args, progress_callback=progress_callback if ctx else None, expected_total=expected_total)  # type: ignore

    run_results = state.validate_and_parse_results(result, "run")

    schema_changes: dict[str, dict[str, list[str]]] = {}
    if check_schema_changes and pre_run_columns:
        logger.info("Detecting schema changes by comparing pre/post-run database columns")

        for model_name, old_columns in pre_run_columns.items():
            new_columns = await state.get_table_columns_from_db(model_name)

            if not new_columns:
                continue

            added = [c for c in new_columns if c not in old_columns]
            removed = [c for c in old_columns if c not in new_columns] if old_columns else []

            if added or removed:
                schema_changes[model_name] = {}
                if added:
                    schema_changes[model_name]["added"] = added
                if removed:
                    schema_changes[model_name]["removed"] = removed

    if result.success:
        await state.save_execution_state()

    # Summarize final progress back to caller
    results_list = run_results.get("results", [])
    await state.report_final_progress(ctx, results_list, "Run", "models")

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


@dbtTool()
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
    state: DbtCoreServerContext = Depends(get_state),
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
        state: Shared state object injected by FastMCP

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
    return await _implementation(ctx, select, exclude, select_state_modified, select_state_modified_plus_downstream, full_refresh, fail_fast, check_schema_changes, cache_selected_only, state)
