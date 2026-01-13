"""Tests for snapshot_models tool."""

import json
from pathlib import Path
from typing import Any, Callable, Dict, Optional
from unittest.mock import AsyncMock, Mock

import pytest

from dbt_core_mcp.tools.snapshot_models import _implementation as snapshot_models_impl  # type: ignore[reportPrivateUsage]


@pytest.fixture
def real_run_results() -> Dict[str, Any]:
    """Load real dbt snapshot results for parsing validation."""
    fixtures_dir = Path(__file__).parent / "fixtures"
    with open(fixtures_dir / "target" / "run_results.json") as f:
        return json.load(f)


@pytest.fixture
def mock_state(real_run_results: Dict[str, Any]) -> Mock:
    """Create mock state for snapshot tool testing."""
    state = Mock()
    state.ensure_initialized = AsyncMock()
    state.prepare_state_based_selection = AsyncMock(return_value=None)
    state.clear_stale_run_results = Mock()
    state.save_execution_state = AsyncMock()

    # Mock runner that captures commands
    mock_runner = Mock()

    def create_mock_result() -> Mock:
        result = Mock()
        result.success = True
        result.stdout = json.dumps(real_run_results)
        return result

    mock_runner.invoke = AsyncMock(side_effect=lambda args, progress_callback=None: create_mock_result())
    state.get_runner = AsyncMock(return_value=mock_runner)

    # Mock validate_and_parse_results to return realistic parsing
    def validate_and_parse_results(result: Any, command_name: str) -> Dict[str, Any]:
        parsed = real_run_results.copy()
        parsed["command"] = "dbt snapshot"
        return parsed

    state.validate_and_parse_results = validate_and_parse_results
    state.report_final_progress = Mock()

    return state


@pytest.mark.asyncio
async def test_snapshot_all(mock_state: Mock) -> None:
    """Test running all snapshots - command construction."""
    commands_run = []

    async def capture_invoke(args: Dict[str, Any], progress_callback: Optional[Callable[..., Any]] = None) -> Mock:
        commands_run.append(args)
        result = Mock()
        result.success = True
        result.stdout = json.dumps(
            {
                "metadata": {},
                "results": [
                    {"status": "success", "unique_id": "snapshot.jaffle_shop.customers_snapshot"},
                ],
                "elapsed_time": 3.5,
            }
        )
        return result

    mock_runner = await mock_state.get_runner()
    mock_runner.invoke.side_effect = capture_invoke

    result = await snapshot_models_impl(None, None, None, mock_state)

    assert result["status"] == "success"
    assert "results" in result
    assert len(commands_run) == 1
    assert commands_run[0][0] == "snapshot"


@pytest.mark.asyncio
async def test_snapshot_select_specific(mock_state: Mock) -> None:
    """Test running a specific snapshot - command construction."""
    commands_run = []

    async def capture_invoke(args: Dict[str, Any], progress_callback: Optional[Callable[..., Any]] = None) -> Mock:
        commands_run.append(args)
        result = Mock()
        result.success = True
        result.stdout = json.dumps(
            {
                "metadata": {},
                "results": [
                    {"status": "success", "unique_id": "snapshot.jaffle_shop.customers_snapshot"},
                ],
                "elapsed_time": 1.8,
            }
        )
        return result

    mock_runner = await mock_state.get_runner()
    mock_runner.invoke.side_effect = capture_invoke

    result = await snapshot_models_impl(None, "customers_snapshot", None, mock_state)

    assert result["status"] == "success"
    assert len(commands_run) == 1
    args = commands_run[0]
    assert "-s" in args or "--select" in args
    assert "customers_snapshot" in args


@pytest.mark.asyncio
async def test_snapshot_exclude() -> None:
    """Test excluding all snapshots raises RuntimeError."""
    # Jaffle shop only has customers_snapshot, so excluding it means no snapshots match
    mock_state = Mock()
    mock_state.ensure_initialized = AsyncMock()
    mock_state.prepare_state_based_selection = AsyncMock(return_value=None)
    mock_state.clear_stale_run_results = Mock()

    # Mock runner that returns empty results
    mock_runner = Mock()
    empty_result = Mock()
    empty_result.success = True
    empty_result.stdout = json.dumps(
        {
            "metadata": {},
            "results": [],
            "elapsed_time": 0.5,
        }
    )
    mock_runner.invoke = AsyncMock(return_value=empty_result)
    mock_state.get_runner = AsyncMock(return_value=mock_runner)

    # Mock validate_and_parse_results to check for empty results
    def validate_and_parse_results(result: Any, command_name: str) -> Dict[str, Any]:
        if result.success and not json.loads(result.stdout).get("results"):
            raise RuntimeError("No snapshots matched selector")
        return json.loads(result.stdout)

    mock_state.validate_and_parse_results = validate_and_parse_results

    with pytest.raises(RuntimeError, match="No snapshots matched selector"):
        await snapshot_models_impl(None, None, "customers_snapshot", mock_state)
