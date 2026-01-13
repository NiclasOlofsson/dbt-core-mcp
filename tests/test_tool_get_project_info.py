"""
Tests for get_project_info tool.
"""

from typing import TYPE_CHECKING

import pytest

from dbt_core_mcp.tools.get_project_info import _implementation as get_project_info_impl

if TYPE_CHECKING:
    from dbt_core_mcp.server import DbtCoreMcpServer


@pytest.mark.asyncio
async def test_get_project_info_with_debug(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test get_project_info with dbt debug enabled (default)."""
    result = await get_project_info_impl(None, True, jaffle_shop_server.state)

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
async def test_get_project_info_without_debug(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test get_project_info without running dbt debug."""
    result = await get_project_info_impl(None, False, jaffle_shop_server.state)

    # Basic project info should still be present
    assert result["project_name"] == "jaffle_shop"
    assert result["status"] == "ready"
    assert "project_dir" in result
    assert "profiles_dir" in result
    assert "adapter_type" in result

    # Diagnostics should NOT be present
    assert "diagnostics" not in result


@pytest.mark.asyncio
async def test_get_project_info_contains_metadata(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test get_project_info contains expected metadata fields."""
    result = await get_project_info_impl(None, False, jaffle_shop_server.state)

    # Check for common metadata fields
    assert "project_name" in result
    assert "dbt_version" in result
    assert "model_count" in result
    assert "source_count" in result
    assert result["model_count"] >= 0
    assert result["source_count"] >= 0
