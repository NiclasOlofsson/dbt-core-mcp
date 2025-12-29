"""Tests for seed_data tool."""

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from dbt_core_mcp.server import DbtCoreMcpServer


@pytest.mark.asyncio
async def test_seed_all(jaffle_shop_server: "DbtCoreMcpServer"):
    """Test loading all seed files."""
    result = await jaffle_shop_server.toolImpl_seed_data()

    assert result["status"] == "success"
    assert "results" in result
    assert "elapsed_time" in result
    assert "seed" in result["command"]

    # Jaffle shop has raw_customers and raw_orders seeds
    results = result["results"]
    assert len(results) >= 2

    # Check that seeds loaded successfully
    for seed_result in results:
        assert seed_result["status"] in ["success", "pass"]


@pytest.mark.asyncio
async def test_seed_select_specific(jaffle_shop_server: "DbtCoreMcpServer"):
    """Test loading a specific seed file."""
    result = await jaffle_shop_server.toolImpl_seed_data(select="raw_customers")

    assert result["status"] == "success"
    assert "results" in result
    assert "-s raw_customers" in result["command"]

    # Should have loaded only raw_customers
    results = result["results"]
    assert len(results) == 1


@pytest.mark.asyncio
async def test_seed_invalid_combination(jaffle_shop_server: "DbtCoreMcpServer"):
    """Test that combining select_state_modified and select raises error."""
    with pytest.raises(ValueError, match="Cannot use both select_state_modified\\* flags and select parameter"):
        await jaffle_shop_server.toolImpl_seed_data(select="raw_customers", select_state_modified=True)


@pytest.mark.asyncio
async def test_seed_modified_only_requires_state(jaffle_shop_server: "DbtCoreMcpServer"):
    """Test that select_state_modified without state returns success (cannot determine modifications)."""
    # Remove state if it exists
    assert jaffle_shop_server.project_dir is not None
    state_dir = jaffle_shop_server.project_dir / "target" / "state_last_run"
    if state_dir.exists():
        import shutil

        shutil.rmtree(state_dir)

    result = await jaffle_shop_server.toolImpl_seed_data(select_state_modified=True)

    assert result["status"] == "success"
    assert "No previous state" in result["message"]
    assert result["results"] == []


@pytest.mark.asyncio
async def test_seed_creates_state(jaffle_shop_server: "DbtCoreMcpServer"):
    """Test that successful seed creates state for modified runs."""
    assert jaffle_shop_server.project_dir is not None
    state_dir = jaffle_shop_server.project_dir / "target" / "state_last_run"

    # First seed should create state
    result = await jaffle_shop_server.toolImpl_seed_data()

    assert result["status"] == "success"
    assert state_dir.exists()
    assert (state_dir / "manifest.json").exists()


@pytest.mark.asyncio
async def test_seed_full_refresh(jaffle_shop_server: "DbtCoreMcpServer"):
    """Test full_refresh flag is passed to dbt."""
    result = await jaffle_shop_server.toolImpl_seed_data(full_refresh=True)

    assert result["status"] == "success"
    assert "--full-refresh" in result["command"]


@pytest.mark.asyncio
async def test_seed_show(jaffle_shop_server: "DbtCoreMcpServer"):
    """Test show flag is passed to dbt."""
    result = await jaffle_shop_server.toolImpl_seed_data(show=True)

    assert result["status"] == "success"
    assert "--show" in result["command"]


@pytest.mark.asyncio
async def test_seed_exclude(jaffle_shop_server: "DbtCoreMcpServer"):
    """Test excluding specific seeds."""
    result = await jaffle_shop_server.toolImpl_seed_data(exclude="raw_customers")

    assert result["status"] == "success"
    assert "--exclude raw_customers" in result["command"]

    # Should have loaded raw_orders but not raw_customers
    results = result["results"]
    assert len(results) >= 1
    # Check no customers seed in results
    customer_seeds = [r for r in results if "raw_customers" in r.get("unique_id", "")]
    assert len(customer_seeds) == 0
