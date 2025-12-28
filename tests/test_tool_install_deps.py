"""Tests for install_deps tool."""

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from dbt_core_mcp.server import DbtCoreMcpServer


@pytest.mark.asyncio
async def test_install_deps_no_packages(jaffle_shop_server: "DbtCoreMcpServer"):
    """Test install_deps when no packages.yml exists (or empty)."""
    # Jaffle shop doesn't have packages.yml, so this should succeed with 0 packages
    result = await jaffle_shop_server.toolImpl_install_deps()

    assert result["status"] == "success"
    assert "installed_packages" in result
    assert result["command"] == "dbt deps"
    assert "message" in result

    # Should report package count
    package_count = len(result["installed_packages"])
    assert f"Successfully installed {package_count} package(s)" in result["message"]
