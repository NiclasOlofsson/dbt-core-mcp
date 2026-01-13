"""Tests for test_models tool."""

from typing import Any, Callable, Optional
from unittest.mock import AsyncMock, Mock

import pytest

from dbt_core_mcp.tools.test_models import (
    _implementation as impl_test_models,
)  # type: ignore[reportPrivateUsage]


@pytest.fixture
def mock_state() -> Mock:
    """Create a mock server state for testing."""
    state = Mock()
    state.ensure_initialized = AsyncMock()
    state.prepare_state_based_selection = AsyncMock(return_value=None)
    state.clear_stale_run_results = Mock()
    # Mock with at least one test result
    state.validate_and_parse_results = Mock(
        return_value={
            "results": [{"status": "pass", "unique_id": "test.jaffle_shop.test_customers"}],
            "status": "success",
            "elapsed_time": 0.3,
            "command": "dbt test",
        }
    )
    state.save_execution_state = AsyncMock()
    state.report_final_progress = AsyncMock()

    # Mock runner
    mock_runner = Mock()
    mock_result = Mock()
    mock_result.success = True
    mock_result.stdout = ""
    mock_runner.invoke = AsyncMock(return_value=mock_result)
    state.get_runner = AsyncMock(return_value=mock_runner)

    return state


@pytest.mark.asyncio
async def test_test_command_construction_basic(mock_state: Mock) -> None:
    """Test basic test command construction."""

    async def capture_invoke(
        args: dict[str, Any],
        progress_callback: Optional[Callable[..., Any]] = None,
    ) -> Mock:
        result = Mock()
        result.success = True
        result.stdout = ""
        return result

    mock_runner = await mock_state.get_runner()
    mock_runner.invoke.side_effect = capture_invoke

    result = await impl_test_models(None, None, None, False, False, False, mock_state)

    assert result["status"] == "success"
    assert "results" in result
    assert len(result["results"]) > 0


@pytest.mark.asyncio
async def test_test_command_construction_with_select(mock_state: Mock) -> None:
    """Test test command with select parameter."""
    commands_run: list[dict[str, Any]] = []

    async def capture_invoke(
        args: dict[str, Any],
        progress_callback: Optional[Callable[..., Any]] = None,
    ) -> Mock:
        commands_run.append(args)
        result = Mock()
        result.success = True
        result.stdout = ""
        return result

    mock_runner = await mock_state.get_runner()
    mock_runner.invoke.side_effect = capture_invoke

    result = await impl_test_models(None, "customers", None, False, False, False, mock_state)

    assert result["status"] == "success"
    # Find test command
    test_cmd = [cmd for cmd in commands_run if "test" in cmd]
    assert len(test_cmd) > 0
    assert "-s" in test_cmd[0]
    assert "customers" in test_cmd[0]


@pytest.mark.asyncio
async def test_test_parameter_validation(mock_state: Mock) -> None:
    """Test that combining select_state_modified and select raises error."""
    mock_state.prepare_state_based_selection = AsyncMock(side_effect=ValueError("Cannot use both select_state_modified and select"))

    with pytest.raises(ValueError, match="Cannot use both select_state_modified"):
        await impl_test_models(None, "customers", None, True, False, False, mock_state)


@pytest.mark.asyncio
async def test_test_fail_fast_flag(mock_state: Mock) -> None:
    """Test fail_fast flag is included in command."""
    commands_run: list[dict[str, Any]] = []

    async def capture_invoke(
        args: dict[str, Any],
        progress_callback: Optional[Callable[..., Any]] = None,
    ) -> Mock:
        commands_run.append(args)
        result = Mock()
        result.success = True
        result.stdout = ""
        return result

    mock_runner = await mock_state.get_runner()
    mock_runner.invoke.side_effect = capture_invoke

    result = await impl_test_models(None, None, None, False, False, True, mock_state)

    assert result["status"] == "success"
    test_cmd = [cmd for cmd in commands_run if "test" in cmd][0]
    assert "--fail-fast" in test_cmd


@pytest.mark.asyncio
async def test_test_exclude_parameter(mock_state: Mock) -> None:
    """Test exclude parameter in test command."""
    commands_run: list[dict[str, Any]] = []

    async def capture_invoke(
        args: dict[str, Any],
        progress_callback: Optional[Callable[..., Any]] = None,
    ) -> Mock:
        commands_run.append(args)
        result = Mock()
        result.success = True
        result.stdout = ""
        return result

    mock_runner = await mock_state.get_runner()
    mock_runner.invoke.side_effect = capture_invoke

    result = await impl_test_models(None, None, "not_null*", False, False, False, mock_state)

    assert result["status"] == "success"
    test_cmd = [cmd for cmd in commands_run if "test" in cmd][0]
    assert "--exclude" in test_cmd
    assert "not_null*" in test_cmd


@pytest.mark.asyncio
async def test_test_state_based_selection(mock_state: Mock) -> None:
    """Test state-based selection preparation."""
    mock_state.prepare_state_based_selection = AsyncMock(return_value="state:modified")

    async def capture_invoke(
        args: dict[str, Any],
        progress_callback: Optional[Callable[..., Any]] = None,
    ) -> Mock:
        result = Mock()
        result.success = True
        result.stdout = "test1\n"
        return result

    mock_runner = await mock_state.get_runner()
    mock_runner.invoke.side_effect = capture_invoke

    await impl_test_models(None, None, None, True, False, False, mock_state)

    mock_state.prepare_state_based_selection.assert_called_once()
