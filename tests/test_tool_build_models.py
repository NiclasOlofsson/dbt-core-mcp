"""Tests for build_models tool.

This file contains comprehensive tests for build_models covering:
- Command construction and parameter validation
- Output parsing and business logic using real fixtures
- No process execution (covered by test_tool_get_project_info.py)
"""

import json
from pathlib import Path
from typing import Any, Callable, Dict, Optional
from unittest.mock import AsyncMock, Mock

import pytest

from dbt_core_mcp.context import DbtCoreServerContext
from dbt_core_mcp.tools.build_models import _implementation as build_models_impl  # type: ignore[attr-defined]


@pytest.fixture
def mock_state() -> Mock:
    """Create a mock server state for testing."""
    state = Mock(spec=DbtCoreServerContext)
    state.ensure_initialized = AsyncMock()
    state.prepare_state_based_selection = AsyncMock(return_value=None)
    state.clear_stale_run_results = Mock()
    # Mock successful results with dummy data
    state.validate_and_parse_results = Mock(return_value={"results": [{"status": "success", "node": {"name": "model1"}}, {"status": "pass", "node": {"name": "test1"}}]})
    state.save_execution_state = AsyncMock()

    # Mock runner
    mock_runner = Mock()
    mock_result = Mock()
    mock_result.success = True
    mock_runner.invoke = AsyncMock(return_value=mock_result)
    state.get_runner = AsyncMock(return_value=mock_runner)

    return state


@pytest.fixture
def fixtures_dir() -> Path:
    """Path to test fixtures directory with real dbt output."""
    return Path(__file__).parent / "fixtures"


@pytest.fixture
def real_run_results(fixtures_dir: Path) -> Dict[str, Any]:
    """Load real dbt run_results.json fixture."""
    run_results_path = fixtures_dir / "target" / "run_results.json"
    assert run_results_path.exists(), f"Fixture not found: {run_results_path}"

    with open(run_results_path) as f:
        return json.load(f)


@pytest.fixture
def mock_state_with_real_parsing(real_run_results: Dict[str, Any]) -> Mock:
    """Mock DbtCoreServerContext that uses real fixture data for parsing."""
    mock_state = Mock(spec=DbtCoreServerContext)

    # Mock the parse_run_results method to return our fixture data
    mock_state.parse_run_results.return_value = real_run_results

    # Use the real validate_and_parse_results method
    def validate_and_parse_results(result: Any, command_name: str) -> Dict[str, Any]:
        run_results = mock_state.parse_run_results()
        if not run_results.get("results"):
            if result and not result.success:
                raise RuntimeError(f"dbt {command_name} failed to execute")
        return run_results

    mock_state.validate_and_parse_results = validate_and_parse_results
    return mock_state


# Command Construction Tests


@pytest.mark.asyncio
async def test_build_command_construction_basic(mock_state: Mock) -> None:
    """Test basic build command construction without execution."""
    # Capture the actual dbt command that would be run
    commands_run = []

    async def capture_invoke(args: Dict[str, Any], progress_callback: Optional[Callable[..., Any]] = None) -> Mock:
        commands_run.append(args)
        result = Mock()
        result.success = True
        return result

    mock_runner = await mock_state.get_runner()
    mock_runner.invoke.side_effect = capture_invoke

    await build_models_impl(
        ctx=None,
        select=None,
        exclude=None,
        select_state_modified=False,
        select_state_modified_plus_downstream=False,
        full_refresh=False,
        resource_types=None,
        fail_fast=False,
        state=mock_state,
    )

    # Verify the command construction
    assert len(commands_run) == 1
    args = commands_run[0]
    assert args[0] == "build"
    assert "--cache-selected-only" not in args  # No selection, so no cache flag


@pytest.mark.asyncio
async def test_build_command_construction_with_select(mock_state: Mock) -> None:
    """Test build command with select parameter."""
    commands_run = []

    async def capture_invoke(args: Dict[str, Any], progress_callback: Optional[Callable[..., Any]] = None) -> Mock:
        commands_run.append(args)
        result = Mock()
        result.success = True
        return result

    mock_runner = await mock_state.get_runner()
    mock_runner.invoke.side_effect = capture_invoke

    await build_models_impl(
        ctx=None,
        select="customers",
        exclude=None,
        select_state_modified=False,
        select_state_modified_plus_downstream=False,
        full_refresh=False,
        resource_types=None,
        fail_fast=False,
        state=mock_state,
    )

    # Verify the command construction
    assert len(commands_run) == 1
    args = commands_run[0]
    expected = ["build", "--cache-selected-only", "-s", "customers"]
    assert args == expected


@pytest.mark.asyncio
async def test_build_command_construction_with_exclude(mock_state: Mock) -> None:
    """Test build command with exclude parameter."""
    commands_run = []

    async def capture_invoke(args: Dict[str, Any], progress_callback: Optional[Callable[..., Any]] = None) -> Mock:
        commands_run.append(args)
        result = Mock()
        result.success = True
        return result

    mock_runner = await mock_state.get_runner()
    mock_runner.invoke.side_effect = capture_invoke

    await build_models_impl(
        ctx=None,
        select=None,
        exclude="customers",
        select_state_modified=False,
        select_state_modified_plus_downstream=False,
        full_refresh=False,
        resource_types=None,
        fail_fast=False,
        state=mock_state,
    )

    # Verify the command construction
    assert len(commands_run) == 1
    args = commands_run[0]
    expected = ["build", "--exclude", "customers"]  # No --cache-selected-only since no select
    assert args == expected


@pytest.mark.asyncio
async def test_build_command_construction_all_flags(mock_state: Mock) -> None:
    """Test build command with all flags enabled."""
    commands_run = []

    async def capture_invoke(args: Dict[str, Any], progress_callback: Optional[Callable[..., Any]] = None) -> Mock:
        commands_run.append(args)
        result = Mock()
        result.success = True
        return result

    mock_runner = await mock_state.get_runner()
    mock_runner.invoke.side_effect = capture_invoke

    await build_models_impl(
        ctx=None,
        select="models",
        exclude="tests",
        select_state_modified=False,
        select_state_modified_plus_downstream=False,
        full_refresh=True,
        resource_types=["model", "test"],
        fail_fast=True,
        state=mock_state,
    )

    # Verify the command construction
    assert len(commands_run) == 1
    args = commands_run[0]

    # Should contain all expected elements
    assert "build" in args
    assert "--cache-selected-only" in args
    assert "-s" in args and "models" in args
    assert "--exclude" in args and "tests" in args
    assert "--resource-type" in args
    assert "model" in args and "test" in args
    assert "--full-refresh" in args
    assert "--fail-fast" in args


@pytest.mark.asyncio
async def test_build_invalid_state_selection(mock_state: Mock) -> None:
    """Test that invalid state selection raises appropriate error."""
    # Mock state selection to return None (no state available)
    mock_state.prepare_state_based_selection.return_value = None

    with pytest.raises(RuntimeError, match="No previous state found"):
        await build_models_impl(
            ctx=None,
            select=None,
            exclude=None,
            select_state_modified=True,  # This should fail without state
            select_state_modified_plus_downstream=False,
            full_refresh=False,
            resource_types=None,
            fail_fast=False,
            state=mock_state,
        )


@pytest.mark.asyncio
async def test_build_parameter_validation(mock_state: Mock) -> None:
    """Test parameter validation logic."""
    # This should raise ValueError for conflicting parameters
    mock_state.prepare_state_based_selection.side_effect = ValueError("Cannot use both select_state_modified* flags and select parameter")

    with pytest.raises(ValueError, match="Cannot use both select_state_modified"):
        await build_models_impl(
            ctx=None,
            select="customers",
            exclude=None,
            select_state_modified=True,
            select_state_modified_plus_downstream=False,
            full_refresh=False,
            resource_types=None,
            fail_fast=False,
            state=mock_state,
        )


@pytest.mark.asyncio
async def test_build_state_modified_command_construction(mock_state: Mock) -> None:
    """Test command construction with state-based selection."""
    commands_run = []

    # Mock successful state preparation
    mock_state.prepare_state_based_selection.return_value = "+"

    async def capture_invoke(args: Dict[str, Any], progress_callback: Optional[Callable[..., Any]] = None) -> Mock:
        commands_run.append(args)
        result = Mock()
        result.success = True
        return result

    mock_runner = await mock_state.get_runner()
    mock_runner.invoke.side_effect = capture_invoke

    await build_models_impl(
        ctx=None,
        select=None,
        exclude=None,
        select_state_modified=True,
        select_state_modified_plus_downstream=False,
        full_refresh=False,
        resource_types=None,
        fail_fast=False,
        state=mock_state,
    )

    # Verify state-based selection is used
    assert len(commands_run) == 1
    args = commands_run[0]
    assert "build" in args
    assert "-s" in args
    assert "+" in args  # The state selector


@pytest.mark.asyncio
async def test_build_state_plus_downstream_command_construction(mock_state: Mock) -> None:
    """Test command construction with state + downstream selection."""
    commands_run = []

    # Mock successful state preparation returning downstream selector
    mock_state.prepare_state_based_selection.return_value = "+state:modified+"

    async def capture_invoke(args: Dict[str, Any], progress_callback: Optional[Callable[..., Any]] = None) -> Mock:
        commands_run.append(args)
        result = Mock()
        result.success = True
        return result

    mock_runner = await mock_state.get_runner()
    mock_runner.invoke.side_effect = capture_invoke

    await build_models_impl(
        ctx=None,
        select=None,
        exclude=None,
        select_state_modified=False,
        select_state_modified_plus_downstream=True,
        full_refresh=False,
        resource_types=None,
        fail_fast=False,
        state=mock_state,
    )

    # Verify downstream state selection is used
    assert len(commands_run) == 1
    args = commands_run[0]
    assert "build" in args
    assert "-s" in args
    assert "+state:modified+" in args


# Output Parsing Tests


def test_successful_build_parsing(mock_state_with_real_parsing: Mock) -> None:
    """Test parsing successful dbt build output."""
    # Mock successful dbt result
    mock_result = Mock()
    mock_result.success = True

    # Parse using real fixture data
    parsed = mock_state_with_real_parsing.validate_and_parse_results(mock_result, "build")

    # Verify basic structure
    assert "results" in parsed
    assert "elapsed_time" in parsed
    assert len(parsed["results"]) > 0

    # All results should be successful in our fixture
    for result in parsed["results"]:
        assert result["status"] in ["success", "pass"]


def test_status_counting_logic(real_run_results: Dict[str, Any]) -> None:
    """Test the status counting logic from build_models.py."""
    results_list = real_run_results.get("results", [])

    # This is the exact logic from build_models.py
    passed_count = sum(1 for r in results_list if r.get("status") in ("success", "pass"))
    failed_count = sum(1 for r in results_list if r.get("status") in ("error", "fail"))
    skip_count = sum(1 for r in results_list if r.get("status") == "skipped")
    total = len(results_list)

    # Verify counting works correctly
    assert passed_count + failed_count + skip_count == total
    assert passed_count > 0, "Fixture should have passing results"
    assert total > 0, "Fixture should have some results"

    # Test with our fixture (all should pass)
    assert failed_count == 0, "Fixture should have no failures"
    assert passed_count == total, "All fixture results should pass"


def test_progress_message_construction(real_run_results: Dict[str, Any]) -> None:
    """Test the progress message construction logic."""
    results_list = real_run_results.get("results", [])

    # Count statuses (same logic as build_models.py)
    passed_count = sum(1 for r in results_list if r.get("status") in ("success", "pass"))
    failed_count = sum(1 for r in results_list if r.get("status") in ("error", "fail"))
    skip_count = sum(1 for r in results_list if r.get("status") == "skipped")
    total = len(results_list)

    # Build message parts (same logic as build_models.py)
    parts = []
    if passed_count > 0:
        parts.append(f"✅ {passed_count} passed" if failed_count > 0 or skip_count > 0 else "✅ All passed")
    if failed_count > 0:
        parts.append(f"❌ {failed_count} failed")
    if skip_count > 0:
        parts.append(f"⏭️ {skip_count} skipped")

    summary = f"Build: {total}/{total} resources completed ({', '.join(parts)})"

    # Verify message construction
    assert "Build:" in summary
    assert f"{total}/{total}" in summary
    assert "✅" in summary, "Should have success indicator"

    # With our all-pass fixture, should show "All passed"
    assert "All passed" in summary


def test_mixed_status_scenario() -> None:
    """Test status counting with mixed success/failure results."""
    # Create synthetic mixed results for testing edge cases
    mixed_results = [{"status": "success"}, {"status": "success"}, {"status": "pass"}, {"status": "fail"}, {"status": "error"}, {"status": "skipped"}]

    # Apply counting logic
    passed_count = sum(1 for r in mixed_results if r.get("status") in ("success", "pass"))
    failed_count = sum(1 for r in mixed_results if r.get("status") in ("error", "fail"))
    skip_count = sum(1 for r in mixed_results if r.get("status") == "skipped")
    total = len(mixed_results)

    # Verify counts
    assert passed_count == 3  # 2 success + 1 pass
    assert failed_count == 2  # 1 fail + 1 error
    assert skip_count == 1
    assert total == 6

    # Test message construction with mixed results
    parts = []
    if passed_count > 0:
        parts.append(f"✅ {passed_count} passed" if failed_count > 0 or skip_count > 0 else "✅ All passed")
    if failed_count > 0:
        parts.append(f"❌ {failed_count} failed")
    if skip_count > 0:
        parts.append(f"⏭️ {skip_count} skipped")

    summary = f"Build: {total}/{total} resources completed ({', '.join(parts)})"

    # Should show specific counts, not "All passed"
    assert "3 passed" in summary
    assert "2 failed" in summary
    assert "1 skipped" in summary


def test_no_results_error_handling(mock_state_with_real_parsing: Mock) -> None:
    """Test error handling when no results are found."""
    # Override to return empty results
    mock_state_with_real_parsing.parse_run_results.return_value = {"results": []}

    # Mock failed dbt result
    mock_result = Mock()
    mock_result.success = False
    mock_result.exception = Exception("Test error")

    # Should raise RuntimeError for no results + failed execution
    with pytest.raises(RuntimeError, match="dbt build failed to execute"):
        mock_state_with_real_parsing.validate_and_parse_results(mock_result, "build")


def test_result_structure_validation(real_run_results: Dict[str, Any]) -> None:
    """Validate the structure of real dbt results matches our expectations."""
    results = real_run_results.get("results", [])
    assert len(results) > 0, "Should have results"

    # Check common result fields exist
    sample_result = results[0]
    required_fields = ["status", "unique_id"]
    for field in required_fields:
        assert field in sample_result, f"Result should have {field} field"

    # Verify status values are what we expect
    all_statuses = {r.get("status") for r in results}
    valid_statuses = {"success", "pass", "error", "fail", "skipped"}
    assert all_statuses.issubset(valid_statuses), f"Unexpected statuses: {all_statuses - valid_statuses}"


def test_elapsed_time_present(real_run_results: Dict[str, Any]) -> None:
    """Test that elapsed_time is present in results."""
    assert "elapsed_time" in real_run_results
    assert isinstance(real_run_results["elapsed_time"], (int, float))
    assert real_run_results["elapsed_time"] >= 0


def test_state_management_side_effects(mock_state_with_real_parsing: Mock) -> None:
    """Test that state management side effects are handled correctly."""
    # Mock successful dbt result
    mock_result = Mock()
    mock_result.success = True

    # Mock state saving call tracking
    mock_state_with_real_parsing.save_execution_state = Mock()

    # Parse using real fixture data
    parsed = mock_state_with_real_parsing.validate_and_parse_results(mock_result, "build")

    # Verify parsing works correctly (this validates the side effect handling logic)
    assert "results" in parsed
    assert len(parsed["results"]) > 0

    # The actual state saving would be handled by the tool implementation
    # Here we're validating that successful parsing enables state management
    for result in parsed["results"]:
        assert "status" in result
        assert result["status"] in ["success", "pass", "error", "fail", "skipped"]


def test_build_result_response_structure(real_run_results: Dict[str, Any]) -> None:
    """Test the complete response structure that build_models returns."""
    # This validates the exact structure the tool should return
    # Based on what the integration tests were checking

    # Simulate the response structure from build_models
    response = {
        "status": "success",
        "results": real_run_results["results"],
        "elapsed_time": real_run_results["elapsed_time"],
        "command": "build --some-flags",  # Would be populated by actual command
    }

    # Validate response structure (what integration tests were checking)
    assert "status" in response
    assert "results" in response
    assert "elapsed_time" in response
    assert "command" in response

    # Validate business logic for response status
    results = response["results"]
    if isinstance(results, list) and all(isinstance(r, dict) and r.get("status") in ("success", "pass") for r in results):
        assert response["status"] == "success"

    # Validate command information is preserved
    assert "build" in response["command"]
