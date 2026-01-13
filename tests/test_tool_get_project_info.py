"""Tests for get_project_info tool."""

from unittest.mock import AsyncMock, Mock

import pytest

from dbt_core_mcp.context import DbtCoreServerContext
from dbt_core_mcp.tools.get_project_info import _implementation as get_project_info_impl  # type: ignore[reportPrivateUsage]


@pytest.fixture
def mock_state() -> Mock:
    """Create a mock server state for testing."""
    state = Mock(spec=DbtCoreServerContext)
    state.ensure_initialized = AsyncMock()
    state.project_dir = "/path/to/jaffle_shop"
    state.profiles_dir = "/path/to/profiles"

    # Mock manifest with get_project_info method
    mock_manifest = Mock()
    mock_manifest.get_project_info = Mock(
        return_value={
            "project_name": "jaffle_shop",
            "dbt_version": "1.7.0",
            "adapter_type": "duckdb",
            "model_count": 3,
            "source_count": 2,
        }
    )
    state.manifest = mock_manifest

    # Mock runner for debug command
    mock_runner = Mock()
    mock_result = Mock()
    mock_result.success = True
    mock_result.stdout = "Connection test: [OK connection ok]"
    mock_runner.invoke = AsyncMock(return_value=mock_result)
    state.get_runner = AsyncMock(return_value=mock_runner)

    return state


@pytest.mark.asyncio
async def test_get_project_info_with_debug(mock_state: Mock) -> None:
    """Test get_project_info with dbt debug enabled (default)."""
    result = await get_project_info_impl(None, True, mock_state, force_parse=False)

    # Basic project info
    assert result["project_name"] == "jaffle_shop"
    assert result["status"] == "ready"
    assert "project_dir" in result
    assert "profiles_dir" in result
    assert "adapter_type" in result

    # Diagnostics should be present
    assert "diagnostics" in result
    assert result["diagnostics"]["command_run"] == "dbt debug"
    assert result["diagnostics"]["success"] is True
    assert result["diagnostics"]["connection_status"] in ["ok", "failed", "unknown"]
    assert "output" in result["diagnostics"]


@pytest.mark.asyncio
async def test_get_project_info_without_debug(mock_state: Mock) -> None:
    """Test get_project_info without running dbt debug."""
    result = await get_project_info_impl(None, False, mock_state, force_parse=False)

    # Basic project info should still be present
    assert result["project_name"] == "jaffle_shop"
    assert result["status"] == "ready"
    assert "project_dir" in result
    assert "profiles_dir" in result
    assert "adapter_type" in result

    # Diagnostics should NOT be present
    assert "diagnostics" not in result


@pytest.mark.asyncio
async def test_get_project_info_contains_metadata(mock_state: Mock) -> None:
    """Test get_project_info contains expected metadata fields."""
    result = await get_project_info_impl(None, False, mock_state, force_parse=False)

    # Check for common metadata fields
    assert "project_name" in result
    assert "dbt_version" in result
    assert "model_count" in result
    assert "source_count" in result
    assert result["model_count"] >= 0
    assert result["source_count"] >= 0
