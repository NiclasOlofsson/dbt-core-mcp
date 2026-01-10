"""Tests for snapshot_models tool."""

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from dbt_core_mcp.server import DbtCoreMcpServer


@pytest.mark.asyncio
async def test_snapshot_all(jaffle_shop_server: "DbtCoreMcpServer"):
    """Test running all snapshots."""
    result = await jaffle_shop_server.toolImpl_snapshot_models(ctx=None)

    assert result["status"] == "success"
    assert "results" in result
    assert "elapsed_time" in result
    assert "snapshot" in result["command"]

    # Jaffle shop has customers_snapshot
    results = result["results"]
    assert len(results) >= 1

    # Check that snapshots ran successfully
    for snapshot_result in results:
        assert snapshot_result["status"] in ["success", "pass"]


@pytest.mark.asyncio
async def test_snapshot_select_specific(jaffle_shop_server: "DbtCoreMcpServer"):
    """Test running a specific snapshot."""
    result = await jaffle_shop_server.toolImpl_snapshot_models(ctx=None, select="customers_snapshot")

    assert result["status"] == "success"
    assert "results" in result
    assert "-s customers_snapshot" in result["command"]

    # Should have run only customers_snapshot
    results = result["results"]
    assert len(results) == 1


@pytest.mark.asyncio
async def test_snapshot_exclude(jaffle_shop_server: "DbtCoreMcpServer"):
    """Test excluding all snapshots raises RuntimeError."""
    # Jaffle shop only has customers_snapshot, so excluding it means no snapshots match
    with pytest.raises(RuntimeError, match="No snapshots matched selector"):
        await jaffle_shop_server.toolImpl_snapshot_models(ctx=None, exclude="customers_snapshot")
