"""Tests for seed_data tool."""

import json
from pathlib import Path
from typing import Any, Callable, Dict, Optional
from unittest.mock import AsyncMock, Mock

import pytest

from dbt_core_mcp.tools.load_seeds import _implementation as load_seeds_impl  # type: ignore[reportPrivateUsage]


@pytest.fixture
def real_run_results() -> Dict[str, Any]:
    """Load real dbt seed results for parsing validation."""
    fixtures_dir = Path(__file__).parent / "fixtures"
    with open(fixtures_dir / "target" / "run_results.json") as f:
        return json.load(f)


@pytest.fixture
def mock_state(real_run_results: Dict[str, Any]) -> Mock:
    """Create mock state for seed tool testing."""
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
        parsed["command"] = "dbt seed"
        return parsed

    state.validate_and_parse_results = validate_and_parse_results
    state.report_final_progress = Mock()

    return state


@pytest.mark.asyncio
async def test_seed_all(mock_state: Mock) -> None:
    """Test loading all seed files - command construction."""
    commands_run = []

    async def capture_invoke(args: Dict[str, Any], progress_callback: Optional[Callable[..., Any]] = None) -> Mock:
        commands_run.append(args)
        result = Mock()
        result.success = True
        result.stdout = json.dumps(
            {
                "metadata": {},
                "results": [
                    {"status": "success", "unique_id": "seed.jaffle_shop.raw_customers"},
                    {"status": "success", "unique_id": "seed.jaffle_shop.raw_orders"},
                ],
                "elapsed_time": 2.5,
            }
        )
        return result

    mock_runner = await mock_state.get_runner()
    mock_runner.invoke.side_effect = capture_invoke

    result = await load_seeds_impl(None, None, None, False, False, False, False, mock_state)

    assert result["status"] == "success"
    assert "results" in result
    assert len(commands_run) == 1
    assert commands_run[0][0] == "seed"


@pytest.mark.asyncio
async def test_seed_select_specific(mock_state: Mock) -> None:
    """Test loading a specific seed file - command construction."""
    commands_run = []

    async def capture_invoke(args: Dict[str, Any], progress_callback: Optional[Callable[..., Any]] = None) -> Mock:
        commands_run.append(args)
        result = Mock()
        result.success = True
        result.stdout = json.dumps(
            {
                "metadata": {},
                "results": [
                    {"status": "success", "unique_id": "seed.jaffle_shop.raw_customers"},
                ],
                "elapsed_time": 1.2,
            }
        )
        return result

    mock_runner = await mock_state.get_runner()
    mock_runner.invoke.side_effect = capture_invoke

    result = await load_seeds_impl(None, "raw_customers", None, False, False, False, False, mock_state)

    assert result["status"] == "success"
    assert len(commands_run) == 1
    args = commands_run[0]
    assert "-s" in args or "--select" in args
    assert "raw_customers" in args


@pytest.mark.asyncio
async def test_seed_invalid_combination() -> None:
    """Test that combining select_state_modified and select raises error."""
    # Mock state with prepare_state_based_selection that raises ValueError (as the real code does)
    mock_state = Mock()
    mock_state.ensure_initialized = AsyncMock()
    mock_state.prepare_state_based_selection = AsyncMock(side_effect=ValueError("Cannot use both select_state_modified* flags and select parameter"))

    with pytest.raises(ValueError, match="Cannot use both select_state_modified"):
        await load_seeds_impl(None, "raw_customers", None, True, False, False, False, mock_state)


@pytest.mark.asyncio
async def test_seed_modified_only_requires_state() -> None:
    """Test that select_state_modified without state raises RuntimeError."""
    mock_state = Mock()
    mock_state.ensure_initialized = AsyncMock()
    mock_state.prepare_state_based_selection = AsyncMock(side_effect=RuntimeError("No previous state found"))

    with pytest.raises(RuntimeError, match="No previous state found"):
        await load_seeds_impl(None, None, None, True, False, False, False, mock_state)


@pytest.mark.asyncio
async def test_seed_full_refresh(mock_state: Mock) -> None:
    """Test full_refresh flag is passed to dbt - command construction."""
    commands_run = []

    async def capture_invoke(args: Dict[str, Any], progress_callback: Optional[Callable[..., Any]] = None) -> Mock:
        commands_run.append(args)
        result = Mock()
        result.success = True
        result.stdout = json.dumps(
            {
                "metadata": {},
                "results": [
                    {"status": "success", "unique_id": "seed.jaffle_shop.raw_customers"},
                    {"status": "success", "unique_id": "seed.jaffle_shop.raw_orders"},
                ],
                "elapsed_time": 2.8,
            }
        )
        return result

    mock_runner = await mock_state.get_runner()
    mock_runner.invoke.side_effect = capture_invoke

    result = await load_seeds_impl(None, None, None, False, False, True, False, mock_state)

    assert result["status"] == "success"
    assert len(commands_run) == 1
    args = commands_run[0]
    assert "--full-refresh" in args


@pytest.mark.asyncio
async def test_seed_show(mock_state: Mock) -> None:
    """Test show flag is passed to dbt - command construction."""
    commands_run = []

    async def capture_invoke(args: Dict[str, Any], progress_callback: Optional[Callable[..., Any]] = None) -> Mock:
        commands_run.append(args)
        result = Mock()
        result.success = True
        result.stdout = json.dumps(
            {
                "metadata": {},
                "results": [
                    {"status": "success", "unique_id": "seed.jaffle_shop.raw_customers"},
                    {"status": "success", "unique_id": "seed.jaffle_shop.raw_orders"},
                ],
                "elapsed_time": 1.5,
            }
        )
        return result

    mock_runner = await mock_state.get_runner()
    mock_runner.invoke.side_effect = capture_invoke

    result = await load_seeds_impl(None, None, None, False, False, False, True, mock_state)

    assert result["status"] == "success"
    assert len(commands_run) == 1
    args = commands_run[0]
    assert "--show" in args


@pytest.mark.asyncio
async def test_seed_exclude(mock_state: Mock) -> None:
    """Test excluding specific seeds - command construction."""
    commands_run = []

    async def capture_invoke(args: Dict[str, Any], progress_callback: Optional[Callable[..., Any]] = None) -> Mock:
        commands_run.append(args)
        result = Mock()
        result.success = True
        result.stdout = json.dumps(
            {
                "metadata": {},
                "results": [
                    {"status": "success", "unique_id": "seed.jaffle_shop.raw_orders"},
                ],
                "elapsed_time": 1.3,
            }
        )
        return result

    mock_runner = await mock_state.get_runner()
    mock_runner.invoke.side_effect = capture_invoke

    result = await load_seeds_impl(None, None, "raw_customers", False, False, False, False, mock_state)

    assert result["status"] == "success"
    assert len(commands_run) == 1
    args = commands_run[0]
    assert "--exclude" in args or "-e" in args
    assert "raw_customers" in args
