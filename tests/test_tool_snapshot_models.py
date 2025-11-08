"""Tests for snapshot_models tool."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dbt_core_mcp.server import DbtCoreMcpServer


async def test_snapshot_all(jaffle_shop_server: "DbtCoreMcpServer"):
    """Test running all snapshots."""
    result = await jaffle_shop_server.toolImpl_snapshot_models()

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


async def test_snapshot_select_specific(jaffle_shop_server: "DbtCoreMcpServer"):
    """Test running a specific snapshot."""
    result = await jaffle_shop_server.toolImpl_snapshot_models(select="customers_snapshot")

    assert result["status"] == "success"
    assert "results" in result
    assert "-s customers_snapshot" in result["command"]

    # Should have run only customers_snapshot
    results = result["results"]
    assert len(results) == 1


async def test_snapshot_exclude(jaffle_shop_server: "DbtCoreMcpServer"):
    """Test excluding specific snapshots."""
    result = await jaffle_shop_server.toolImpl_snapshot_models(exclude="customers_snapshot")

    assert result["status"] == "success"
    assert "--exclude customers_snapshot" in result["command"]

    # Should have no results if only snapshot is excluded
    results = result["results"]
    # Jaffle shop only has customers_snapshot, so excluding it means no snapshots run
    # But dbt snapshot still succeeds with 0 snapshots
    customer_snapshots = [r for r in results if "customers_snapshot" in r.get("unique_id", "")]
    assert len(customer_snapshots) == 0
