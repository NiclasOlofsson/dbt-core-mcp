"""
Tests for run_models tool.
"""

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from dbt_core_mcp.server import DbtCoreMcpServer


@pytest.fixture
async def seeded_jaffle_shop_server(jaffle_shop_server: "DbtCoreMcpServer"):
    """Jaffle shop server with seeds already loaded."""
    # Load seeds first since models depend on them
    await jaffle_shop_server.toolImpl_seed_data()
    return jaffle_shop_server


async def test_run_models_all(seeded_jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test running all models."""
    result = await seeded_jaffle_shop_server.toolImpl_run_models()

    assert result["status"] == "success"
    assert "results" in result
    assert "elapsed_time" in result
    assert "command" in result
    assert len(result["results"]) > 0


async def test_run_models_select_specific(seeded_jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test running a specific model."""
    result = await seeded_jaffle_shop_server.toolImpl_run_models(select="customers")

    assert result["status"] == "success"
    assert "results" in result
    # Should have run customers and possibly dependencies
    assert len(result["results"]) >= 1


async def test_run_models_invalid_selection_combination(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test that using both modified_only and select raises error."""
    with pytest.raises(ValueError, match="Cannot use both modified_\\* flags and select parameter"):
        await jaffle_shop_server.toolImpl_run_models(select="customers", modified_only=True)


async def test_run_models_modified_only_no_state(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test modified_only without previous state returns appropriate error."""
    # Clean any existing state
    assert jaffle_shop_server.project_dir is not None
    state_dir = jaffle_shop_server.project_dir / "target" / "state_last_run"
    if state_dir.exists():
        import shutil

        shutil.rmtree(state_dir)

    result = await jaffle_shop_server.toolImpl_run_models(modified_only=True)

    assert result["status"] == "error"
    assert "No previous run state found" in result["message"]


async def test_run_models_creates_state(seeded_jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test that successful run creates state for next modified run."""
    # Clean state first
    assert seeded_jaffle_shop_server.project_dir is not None
    state_dir = seeded_jaffle_shop_server.project_dir / "target" / "state_last_run"
    if state_dir.exists():
        import shutil

        shutil.rmtree(state_dir)

    # Run models
    result = await seeded_jaffle_shop_server.toolImpl_run_models()

    assert result["status"] == "success"
    # State should be created
    assert state_dir.exists()
    assert (state_dir / "manifest.json").exists()


async def test_run_models_full_refresh(seeded_jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test run with full_refresh flag."""
    result = await seeded_jaffle_shop_server.toolImpl_run_models(full_refresh=True)

    assert result["status"] == "success"
    assert "--full-refresh" in result["command"]
