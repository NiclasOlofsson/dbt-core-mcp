"""Tests for install_deps tool."""

from typing import Any, Callable, Dict, Optional
from unittest.mock import AsyncMock, Mock

import pytest

from dbt_core_mcp.context import DbtCoreServerContext
from dbt_core_mcp.tools.install_deps import _implementation as install_deps_impl  # type: ignore[reportPrivateUsage]


@pytest.fixture
def mock_state() -> Mock:
    """Create a mock server state for testing."""
    state = Mock(spec=DbtCoreServerContext)
    state.ensure_initialized = AsyncMock()

    # Mock manifest with get_manifest_dict
    mock_manifest = Mock()
    mock_manifest.get_manifest_dict = Mock(
        return_value={
            "metadata": {"project_name": "jaffle_shop"},
            "macros": {},  # Empty macros dict = no packages
        }
    )
    state.manifest = mock_manifest

    # Mock runner
    mock_runner = Mock()
    mock_result = Mock()
    mock_result.success = True
    # Simulate dbt deps output with no packages
    mock_result.result = "Up to date!\n"
    mock_runner.invoke = AsyncMock(return_value=mock_result)
    state.get_runner = AsyncMock(return_value=mock_runner)

    # Mock parse_lock_file to return empty packages list
    state.parse_lock_file = Mock(return_value=[])

    return state


@pytest.mark.asyncio
async def test_install_deps_no_packages(mock_state: Mock) -> None:
    """Test install_deps when no packages.yml exists (or empty)."""
    commands_run = []

    async def capture_invoke(args: Dict[str, Any], progress_callback: Optional[Callable[..., Any]] = None) -> Mock:
        commands_run.append(args)
        result = Mock()
        result.success = True
        result.result = "Up to date!\n"
        return result

    mock_runner = await mock_state.get_runner()
    mock_runner.invoke.side_effect = capture_invoke

    result = await install_deps_impl(None, mock_state)

    # Verify command was constructed correctly
    assert len(commands_run) == 1
    args = commands_run[0]
    assert args[0] == "deps"

    # Verify response structure
    assert result["status"] == "success"
    assert "installed_packages" in result
    assert result["command"] == "dbt deps"
    assert "message" in result

    # Should report package count
    package_count = len(result["installed_packages"])
    assert f"Successfully installed {package_count} package(s)" in result["message"]
