"""
Tests for run_models tool.
"""

from typing import TYPE_CHECKING, Any, Callable, Optional
from unittest.mock import AsyncMock, Mock

import pytest

from dbt_core_mcp.tools.run_models import _implementation as run_models_impl  # type: ignore[reportPrivateUsage]

if TYPE_CHECKING:
    pass


@pytest.fixture
def mock_state() -> Mock:
    """Create a mock server state for testing."""
    state = Mock()
    state.ensure_initialized = AsyncMock()
    state.prepare_state_based_selection = AsyncMock(return_value=None)
    state.clear_stale_run_results = Mock()
    # Mock with at least one result to avoid "no models matched" error
    state.validate_and_parse_results = Mock(
        return_value={
            "results": [{"status": "success", "unique_id": "model.test.customers"}],
            "status": "success",
            "elapsed_time": 0.5,
            "command": "dbt run",
        }
    )
    state.save_execution_state = AsyncMock()
    state.report_final_progress = AsyncMock()
    state.get_table_columns_from_db = AsyncMock(return_value=[])

    # Mock runner
    mock_runner = Mock()
    mock_result = Mock()
    mock_result.success = True
    mock_result.stdout = ""  # Empty stdout for list command
    mock_runner.invoke = AsyncMock(return_value=mock_result)
    state.get_runner = AsyncMock(return_value=mock_runner)

    return state


@pytest.mark.asyncio
async def test_run_models_command_construction_basic(mock_state: Mock) -> None:
    """Test basic command construction without execution."""
    commands_run: list[dict[str, Any]] = []

    async def capture_invoke(args: dict[str, Any], progress_callback: Optional[Callable[..., Any]] = None, expected_total: Optional[int] = None) -> Mock:
        commands_run.append(args)
        result = Mock()
        result.success = True
        # For list commands, return empty stdout (no models)
        result.stdout = ""
        return result

    mock_runner = await mock_state.get_runner()
    mock_runner.invoke.side_effect = capture_invoke

    # Call tool implementation
    await run_models_impl(None, None, None, False, False, False, False, False, True, mock_state)

    # Assert run command was created
    assert len(commands_run) >= 1
    # Find the run command (skip the list command)
    run_cmd = [cmd for cmd in commands_run if "run" in cmd][0]
    assert "run" in run_cmd


@pytest.mark.asyncio
async def test_run_models_command_construction_with_select(mock_state: Mock) -> None:
    """Test command construction with select parameter."""
    commands_run: list[dict[str, Any]] = []

    async def capture_invoke(args: dict[str, Any], progress_callback: Optional[Callable[..., Any]] = None, expected_total: Optional[int] = None) -> Mock:
        commands_run.append(args)
        result = Mock()
        result.success = True
        result.stdout = ""
        return result

    mock_runner = await mock_state.get_runner()
    mock_runner.invoke.side_effect = capture_invoke

    await run_models_impl(None, "customers", None, False, False, False, False, False, True, mock_state)

    # Find the list command and check it has select
    assert len(commands_run) >= 2
    list_cmd = commands_run[0]
    assert "list" in list_cmd
    assert "-s" in list_cmd
    assert "customers" in list_cmd


@pytest.mark.asyncio
async def test_run_models_command_construction_with_exclude(mock_state: Mock) -> None:
    """Test command construction with exclude parameter."""
    commands_run: list[dict[str, Any]] = []

    async def capture_invoke(args: dict[str, Any], progress_callback: Optional[Callable[..., Any]] = None, expected_total: Optional[int] = None) -> Mock:
        commands_run.append(args)
        result = Mock()
        result.success = True
        result.stdout = ""
        return result

    mock_runner = await mock_state.get_runner()
    mock_runner.invoke.side_effect = capture_invoke

    await run_models_impl(None, None, "stg_*", False, False, False, False, False, True, mock_state)

    assert len(commands_run) >= 1
    run_cmd = [cmd for cmd in commands_run if "run" in cmd][0]
    assert "--exclude" in run_cmd
    assert "stg_*" in run_cmd


@pytest.mark.asyncio
async def test_run_models_command_construction_full_refresh(mock_state: Mock) -> None:
    """Test command construction with full_refresh flag."""
    commands_run: list[dict[str, Any]] = []

    async def capture_invoke(args: dict[str, Any], progress_callback: Optional[Callable[..., Any]] = None, expected_total: Optional[int] = None) -> Mock:
        commands_run.append(args)
        result = Mock()
        result.success = True
        result.stdout = ""
        return result

    mock_runner = await mock_state.get_runner()
    mock_runner.invoke.side_effect = capture_invoke

    await run_models_impl(None, None, None, False, False, True, False, False, True, mock_state)

    assert len(commands_run) >= 1
    run_cmd = [cmd for cmd in commands_run if "run" in cmd][0]
    assert "--full-refresh" in run_cmd


@pytest.mark.asyncio
async def test_run_models_parameter_validation_select_and_state_modified(mock_state: Mock) -> None:
    """Test that using both select_state_modified and select raises error."""
    # The actual validation happens in prepare_state_based_selection
    # which we mock to raise the error
    mock_state.prepare_state_based_selection = AsyncMock(side_effect=ValueError("Cannot use both select_state_modified and select"))

    with pytest.raises(ValueError, match="Cannot use both select_state_modified"):
        await run_models_impl(None, "customers", None, True, False, False, False, False, True, mock_state)


@pytest.mark.asyncio
async def test_run_models_parameter_validation_exclude_and_state_modified(mock_state: Mock) -> None:
    """Test that using both select_state_modified and exclude raises error."""
    mock_state.prepare_state_based_selection = AsyncMock(side_effect=ValueError("Cannot use both select_state_modified and exclude"))

    with pytest.raises(ValueError, match="Cannot use both select_state_modified"):
        await run_models_impl(None, None, "stg_*", True, False, False, False, False, True, mock_state)


@pytest.mark.asyncio
async def test_run_models_successful_parsing(mock_state: Mock) -> None:
    """Test successful model run parsing with real fixture."""
    from pathlib import Path

    fixtures_dir = Path(__file__).parent / "fixtures"
    run_results_path = fixtures_dir / "target" / "run_results.json"

    with open(run_results_path) as f:
        import json

        real_results = json.load(f)

    mock_state.validate_and_parse_results.return_value = {
        "status": "success",
        "results": real_results.get("results", []),
        "elapsed_time": real_results.get("elapsed_time", 0),
        "command": "dbt run",
    }

    result = await run_models_impl(None, None, None, False, False, False, False, False, True, mock_state)

    assert result["status"] == "success"
    assert "results" in result
    assert len(result["results"]) > 0
    assert "elapsed_time" in result


@pytest.mark.asyncio
async def test_run_models_state_based_selection_preparation(mock_state: Mock) -> None:
    """Test that state-based selection calls preparation step."""

    async def capture_invoke(args: dict[str, Any], progress_callback: Optional[Callable[..., Any]] = None, expected_total: Optional[int] = None) -> Mock:
        result = Mock()
        result.success = True
        # Return a model name in stdout for state:modified
        result.stdout = "customers\n"
        return result

    mock_state.prepare_state_based_selection = AsyncMock(return_value="state:modified")
    mock_runner = await mock_state.get_runner()
    mock_runner.invoke.side_effect = capture_invoke

    await run_models_impl(None, None, None, True, False, False, False, False, True, mock_state)

    # Verify prepare_state_based_selection was called
    mock_state.prepare_state_based_selection.assert_called_once()
