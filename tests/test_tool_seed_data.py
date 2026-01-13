"""Tests for seed_data tool."""

from typing import TYPE_CHECKING

import pytest

from dbt_core_mcp.tools.load_seeds import _implementation as load_seeds_impl  # type: ignore[reportPrivateUsage]

if TYPE_CHECKING:
    from dbt_core_mcp.server import DbtCoreMcpServer


@pytest.mark.asyncio
async def test_seed_all(jaffle_shop_server: "DbtCoreMcpServer"):
    """Test loading all seed files."""
    result = await load_seeds_impl(None, None, None, False, False, False, False, jaffle_shop_server.state)

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
    result = await load_seeds_impl(None, "raw_customers", None, False, False, False, False, jaffle_shop_server.state)

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
        await load_seeds_impl(None, "raw_customers", None, True, False, False, False, jaffle_shop_server.state)


@pytest.mark.asyncio
async def test_seed_modified_only_requires_state(jaffle_shop_server: "DbtCoreMcpServer"):
    """Test that select_state_modified without state raises RuntimeError."""
    # Remove state if it exists
    assert jaffle_shop_server.project_dir is not None
    state_dir = jaffle_shop_server.project_dir / "target" / "state_last_run"
    if state_dir.exists():
        import shutil

        shutil.rmtree(state_dir)

    with pytest.raises(RuntimeError, match="No previous state found"):
        await load_seeds_impl(None, None, None, True, False, False, False, jaffle_shop_server.state)


@pytest.mark.asyncio
async def test_seed_creates_state(jaffle_shop_server: "DbtCoreMcpServer"):
    """Test that successful seed creates state for modified runs."""
    assert jaffle_shop_server.project_dir is not None
    state_dir = jaffle_shop_server.project_dir / "target" / "state_last_run"

    # First seed should create state
    result = await load_seeds_impl(None, None, None, False, False, False, False, jaffle_shop_server.state)

    assert result["status"] == "success"
    assert state_dir.exists()
    assert (state_dir / "manifest.json").exists()


@pytest.mark.asyncio
async def test_seed_full_refresh(jaffle_shop_server: "DbtCoreMcpServer"):
    """Test full_refresh flag is passed to dbt."""
    result = await load_seeds_impl(None, None, None, False, False, True, False, jaffle_shop_server.state)

    assert result["status"] == "success"
    assert "--full-refresh" in result["command"]


@pytest.mark.asyncio
async def test_seed_show(jaffle_shop_server: "DbtCoreMcpServer"):
    """Test show flag is passed to dbt."""
    result = await load_seeds_impl(None, None, None, False, False, False, True, jaffle_shop_server.state)

    assert result["status"] == "success"
    assert "--show" in result["command"]


@pytest.mark.asyncio
async def test_seed_exclude(jaffle_shop_server: "DbtCoreMcpServer"):
    """Test excluding specific seeds."""
    result = await load_seeds_impl(None, None, "raw_customers", False, False, False, False, jaffle_shop_server.state)

    assert result["status"] == "success"
    assert "--exclude raw_customers" in result["command"]

    # Should have loaded raw_orders but not raw_customers
    results = result["results"]
    assert len(results) >= 1
    # Check no customers seed in results
    customer_seeds = [r for r in results if "raw_customers" in r.get("unique_id", "")]
    assert len(customer_seeds) == 0
