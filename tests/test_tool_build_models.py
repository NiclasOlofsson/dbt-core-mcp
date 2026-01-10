"""Tests for build_models tool."""

from typing import TYPE_CHECKING

import pytest
import pytest_asyncio

if TYPE_CHECKING:
    from dbt_core_mcp.server import DbtCoreMcpServer


@pytest_asyncio.fixture
async def seeded_jaffle_shop_server(jaffle_shop_server: "DbtCoreMcpServer"):
    """Jaffle shop server with seeds already loaded."""
    # Load seeds first since build depends on them
    await jaffle_shop_server.toolImpl_seed_data(ctx=None)
    return jaffle_shop_server


@pytest.mark.asyncio
async def test_build_all_models(seeded_jaffle_shop_server: "DbtCoreMcpServer"):
    """Test building all models (run + test in DAG order)."""
    result = await seeded_jaffle_shop_server.toolImpl_build_models(ctx=None)

    assert result["status"] == "success"
    assert "results" in result
    assert "elapsed_time" in result
    assert "build" in result["command"]

    # Build should run models and tests
    results = result["results"]
    assert len(results) > 0

    # Verify build ran successfully
    for r in results:
        assert r["status"] in ["success", "pass"]


@pytest.mark.asyncio
async def test_build_select_specific(seeded_jaffle_shop_server: "DbtCoreMcpServer"):
    """Test building a specific model."""
    result = await seeded_jaffle_shop_server.toolImpl_build_models(ctx=None, select="customers")

    assert result["status"] == "success"
    assert "results" in result
    assert "-s customers" in result["command"]


@pytest.mark.asyncio
async def test_build_invalid_combination(jaffle_shop_server: "DbtCoreMcpServer"):
    """Test that combining select_state_modified and select raises error."""
    with pytest.raises(ValueError, match="Cannot use both select_state_modified\\* flags and select parameter"):
        await jaffle_shop_server.toolImpl_build_models(ctx=None, select="customers", select_state_modified=True)


@pytest.mark.asyncio
async def test_build_modified_only_no_state_builds_all(jaffle_shop_server: "DbtCoreMcpServer"):
    """Test select_state_modified without state raises RuntimeError."""
    # Remove state if it exists
    assert jaffle_shop_server.project_dir is not None
    state_dir = jaffle_shop_server.project_dir / "target" / "state_last_run"
    if state_dir.exists():
        import shutil

        shutil.rmtree(state_dir)

    # With no state, select_state_modified should raise RuntimeError
    with pytest.raises(RuntimeError, match="No previous state found"):
        await jaffle_shop_server.toolImpl_build_models(ctx=None, select_state_modified=True)


@pytest.mark.asyncio
async def test_build_creates_state(seeded_jaffle_shop_server: "DbtCoreMcpServer"):
    """Test that successful build creates state for modified runs."""
    assert seeded_jaffle_shop_server.project_dir is not None
    state_dir = seeded_jaffle_shop_server.project_dir / "target" / "state_last_run"

    # First build should create state
    result = await seeded_jaffle_shop_server.toolImpl_build_models(ctx=None)

    assert result["status"] == "success"
    assert state_dir.exists()
    assert (state_dir / "manifest.json").exists()


@pytest.mark.asyncio
async def test_build_fail_fast(seeded_jaffle_shop_server: "DbtCoreMcpServer"):
    """Test fail_fast flag is passed to dbt."""
    result = await seeded_jaffle_shop_server.toolImpl_build_models(ctx=None, fail_fast=True)

    assert result["status"] == "success"
    assert "--fail-fast" in result["command"]


@pytest.mark.asyncio
async def test_build_exclude(seeded_jaffle_shop_server: "DbtCoreMcpServer"):
    """Test excluding specific models."""
    result = await seeded_jaffle_shop_server.toolImpl_build_models(ctx=None, exclude="customers")

    assert result["status"] == "success"
    assert "--exclude customers" in result["command"]

    # Verify command includes exclude parameter
    assert "build" in result["command"]
