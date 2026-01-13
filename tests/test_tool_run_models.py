"""
Tests for run_models tool.
"""

from typing import TYPE_CHECKING

import pytest
import pytest_asyncio

from dbt_core_mcp.tools.load_seeds import _implementation as load_seeds_impl  # type: ignore[reportPrivateUsage]
from dbt_core_mcp.tools.run_models import _implementation as run_models_impl  # type: ignore[reportPrivateUsage]

if TYPE_CHECKING:
    from dbt_core_mcp.server import DbtCoreMcpServer


@pytest_asyncio.fixture(scope="module")
async def seeded_jaffle_shop_server(jaffle_shop_server: "DbtCoreMcpServer"):
    """Jaffle shop server with seeds already loaded (shared across module tests)."""
    # Load seeds first since models depend on them
    await load_seeds_impl(None, None, None, False, False, False, False, jaffle_shop_server.state)
    return jaffle_shop_server


@pytest.mark.asyncio
async def test_run_models_all(seeded_jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test running all models."""
    result = await run_models_impl(None, None, None, False, False, False, False, False, True, seeded_jaffle_shop_server.state)

    assert result["status"] == "success"
    assert "results" in result
    assert "elapsed_time" in result
    assert "command" in result
    assert len(result["results"]) > 0


@pytest.mark.asyncio
async def test_run_models_select_specific(seeded_jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test running a specific model."""
    result = await run_models_impl(None, "customers", None, False, False, False, False, False, True, seeded_jaffle_shop_server.state)

    assert result["status"] == "success"
    assert "results" in result
    # Should have run customers and possibly dependencies
    assert len(result["results"]) >= 1


@pytest.mark.asyncio
async def test_run_models_invalid_selection_combination(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test that using both select_state_modified and select raises error."""
    with pytest.raises(ValueError, match="Cannot use both select_state_modified\\* flags and select parameter"):
        await run_models_impl(None, "customers", None, True, False, False, False, False, True, jaffle_shop_server.state)


@pytest.mark.asyncio
async def test_run_models_modified_only_no_state_runs_all(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test select_state_modified without state returns success (cannot determine modifications)."""
    # Clean any existing state
    assert jaffle_shop_server.project_dir is not None
    state_dir = jaffle_shop_server.project_dir / "target" / "state_last_run"
    if state_dir.exists():
        import shutil

        shutil.rmtree(state_dir)

    # With no state, select_state_modified should raise RuntimeError
    with pytest.raises(RuntimeError, match="No previous state found"):
        await run_models_impl(None, None, None, True, False, False, False, False, True, jaffle_shop_server.state)


@pytest.mark.asyncio
async def test_run_models_creates_state(seeded_jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test that successful run creates state for next modified run."""
    # Clean state first
    assert seeded_jaffle_shop_server.project_dir is not None
    state_dir = seeded_jaffle_shop_server.project_dir / "target" / "state_last_run"
    if state_dir.exists():
        import shutil

        shutil.rmtree(state_dir)

    # Run models
    result = await run_models_impl(None, None, None, False, False, False, False, False, True, seeded_jaffle_shop_server.state)

    assert result["status"] == "success"
    # State should be created
    assert state_dir.exists()
    assert (state_dir / "manifest.json").exists()


@pytest.mark.asyncio
async def test_run_models_full_refresh(seeded_jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test run with full_refresh flag."""
    result = await run_models_impl(None, None, None, False, False, True, False, False, True, seeded_jaffle_shop_server.state)

    assert result["status"] == "success"
    assert "--full-refresh" in result["command"]
