"""Run dbt tests on models and sources.

This module implements the test_models tool for dbt Core MCP.
"""

import json
import logging
from typing import Any

from fastmcp.dependencies import Depends  # type: ignore[reportAttributeAccessIssue]
from fastmcp.exceptions import McpError  # type: ignore[attr-defined]
from fastmcp.server.context import Context
from mcp.types import ErrorData

from ..context import DbtCoreServerContext
from ..cte_generator import generate_cte_tests
from ..dependencies import get_state
from . import dbtTool

logger = logging.getLogger(__name__)


async def _implementation(
    ctx: Context | None,
    select: str | None,
    exclude: str | None,
    select_state_modified: bool,
    select_state_modified_plus_downstream: bool,
    fail_fast: bool,
    state: DbtCoreServerContext,
) -> dict[str, Any]:
    """Implementation function for test_models tool.

    Separated for testing purposes - tests call this directly with explicit state.
    The @tool() decorated test_models() function calls this with injected dependencies.
    """
    # Ensure dbt components are initialized
    await state.ensure_initialized(ctx, force_parse=False)

    # Generate CTE tests if experimental features enabled
    if state.experimental_features and state.project_dir:
        logger.info("Experimental features enabled - generating CTE tests")
        try:
            cte_count = generate_cte_tests(state.project_dir)
            if cte_count > 0:
                logger.info(f"Generated {cte_count} CTE tests")
        except Exception as e:
            logger.warning(f"CTE test generation failed: {e}")
            # Don't fail the entire test run if CTE generation fails

    # Build state-based selector if requested (avoids redundant parsing when possible)
    selector = await state.prepare_state_based_selection(select_state_modified, select_state_modified_plus_downstream, select)

    # If user asked for state:modified but no baseline exists, fail fast with clear guidance
    if select_state_modified and not selector:
        raise RuntimeError("No previous state found - cannot determine modifications. Run 'dbt run' or 'dbt build' first to create baseline state.")

    args = ["test"]

    # Add selector if we have one (state-based or manual)
    if selector:
        args.extend(["-s", selector, "--state", "target/state_last_run"])
    elif select:
        args.extend(["-s", select])

    if exclude:
        args.extend(["--exclude", exclude])

    if fail_fast:
        args.append("--fail-fast")

    # Execute with progress reporting
    logger.info(f"Running dbt tests with args: {args}")

    # Define progress callback if context available
    async def progress_callback(current: int, total: int, message: str) -> None:
        if ctx:
            await ctx.report_progress(progress=current, total=total, message=message)

    # Delete stale run_results.json to ensure we only read fresh results
    state.clear_stale_run_results()

    runner = await state.get_runner()
    result = await runner.invoke(args, progress_callback=progress_callback if ctx else None)  # type: ignore

    # Parse run_results.json to discriminate system errors from business outcomes
    run_results = state.validate_and_parse_results(result, "test")

    # Business outcome - dbt executed tests (some may have failed)
    # Continue with existing run_results parsing
    results_list = run_results.get("results", [])

    # Remove heavy fields from results to reduce token usage
    for result in results_list:
        result.pop("compiled_code", None)
        result.pop("raw_code", None)

    # Send final progress update with test summary
    await state.report_final_progress(ctx, results_list, "Test", "tests")

    # Empty results means selector matched nothing - this is an error
    if not results_list:
        raise RuntimeError(f"No tests matched selector: {select or selector or 'all'}")

    # Check if any tests failed - if so, return RPC error with structured data
    failed_tests = [r for r in results_list if r.get("status") == "fail"]
    if failed_tests:
        # Check if any failed tests are unit tests (they have diff output)
        has_unit_test_failures = any("unit_test" in r.get("unique_id", "") for r in failed_tests)

        # Add diff format legend for unit test failures
        diff_legend = ""
        if has_unit_test_failures:
            diff_legend = (
                "\n\nUnit test diff format (daff tabular diff):\n"
                "  @@    Header row with column names\n"
                "  +++   Row present in actual output but missing from expected\n"
                "  ---   Row present in expected but missing from actual output\n"
                "  →     Row with at least one modified cell (→, -->, etc.)\n"
                "  ...   Rows omitted for brevity\n"
                "  old_value→new_value   Shows the change in a specific cell\n"
                "\nFull specification: https://paulfitz.github.io/daff-doc/spec.html"
            )

        error_data = {
            "error": "test_failure",
            "message": f"{len(failed_tests)} test(s) failed{diff_legend}",
            "command": " ".join(args),
            "results": results_list,  # Include ALL results (both passing and failing)
            "elapsed_time": run_results.get("elapsed_time"),
            "summary": {
                "total": len(results_list),
                "passed": len([r for r in results_list if r.get("status") == "pass"]),
                "failed": len(failed_tests),
            },
        }
        # Use McpError with custom code for business failures (not system errors)
        # Code -32000 to -32099 are reserved for implementation-defined server errors
        raise McpError(
            ErrorData(
                code=-32000,  # Server error (business failure, not system error)
                message=json.dumps(error_data, indent=2),
            )
        )

    # All tests passed - return success
    return {
        "status": "success",
        "command": " ".join(args),
        "results": results_list,
        "elapsed_time": run_results.get("elapsed_time"),
    }


@dbtTool()
async def test_models(
    ctx: Context,
    select: str | None = None,
    exclude: str | None = None,
    select_state_modified: bool = False,
    select_state_modified_plus_downstream: bool = False,
    fail_fast: bool = False,
    state: DbtCoreServerContext = Depends(get_state),
) -> dict[str, Any]:
    """Run dbt tests on models and sources.

    **When to use**: After running models to validate data quality. Tests check constraints
    like uniqueness, not-null, relationships, and custom data quality rules.

    **Important**: Ensure seeds and models are built before running tests that depend on them.

    State-based selection modes (uses dbt state:modified selector):
    - select_state_modified: Test only models modified since last successful run (state:modified)
    - select_state_modified_plus_downstream: Test modified + downstream dependencies (state:modified+)
      Note: Requires select_state_modified=True

    Manual selection (alternative to state-based):
    - select: dbt selector syntax (e.g., "customers", "tag:mart", "test_type:generic")
    - exclude: Exclude specific tests

    Args:
        select: Manual selector for tests/models to test
        exclude: Exclude selector
        select_state_modified: Use state:modified selector (changed models only)
        select_state_modified_plus_downstream: Extend to state:modified+ (changed + downstream)
        fail_fast: Stop execution on first failure
        state: Shared state object injected by FastMCP

    Returns:
        Test results with status and failures

    See also:
        - run_models(): Execute models before testing them
        - build_models(): Run models + tests together automatically
        - load_seeds(): Load seeds if tests reference seed data

    Examples:
        # After building a model, test it
        run_models(select="customers")
        test_models(select="customers")

        # Test only generic tests (not singular)
        test_models(select="test_type:generic")

        # Test everything that changed
        test_models(select_state_modified=True)

        # Stop on first failure for quick feedback
        test_models(fail_fast=True)

    Note: Unit test failures show diffs in the "daff" tabular format:
        @@ = column headers
        +++ = row in actual, not in expected (extra row)
        --- = row in expected, not in actual (missing row)
        → = row with modified cell(s), shown as old_value→new_value
        ... = omitted matching rows
        Full format spec: https://paulfitz.github.io/daff-doc/spec.html
    """
    return await _implementation(ctx, select, exclude, select_state_modified, select_state_modified_plus_downstream, fail_fast, state)
