"""Tests for build_models tool."""

from typing import TYPE_CHECKING

import pytest
import pytest_asyncio

from dbt_core_mcp.tools.build_models import _implementation as build_models
from dbt_core_mcp.tools.load_seeds import _implementation as load_seeds

if TYPE_CHECKING:
    from dbt_core_mcp.server import DbtCoreMcpServer


@pytest_asyncio.fixture
async def seeded_jaffle_shop_server(jaffle_shop_server: "DbtCoreMcpServer"):
    """Jaffle shop server with seeds already loaded."""
    # Load seeds first since build depends on them
    await load_seeds(
        ctx=None,
        select=None,
        exclude=None,
        select_state_modified=False,
        select_state_modified_plus_downstream=False,
        full_refresh=False,
        show=False,
        state=jaffle_shop_server.state,
    )
    return jaffle_shop_server


@pytest.mark.asyncio
async def test_build_all_models(seeded_jaffle_shop_server: "DbtCoreMcpServer"):
    """Test building all models (run + test in DAG order)."""
    result = await build_models(
        ctx=None,
        select=None,
        exclude=None,
        select_state_modified=False,
        select_state_modified_plus_downstream=False,
        full_refresh=False,
        resource_types=None,
        fail_fast=False,
        state=seeded_jaffle_shop_server.state,
    )

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
    result = await build_models(
        ctx=None,
        select="customers",
        exclude=None,
        select_state_modified=False,
        select_state_modified_plus_downstream=False,
        full_refresh=False,
        resource_types=None,
        fail_fast=False,
        state=seeded_jaffle_shop_server.state,
    )

    assert result["status"] == "success"
    assert "results" in result
    assert "-s customers" in result["command"]


@pytest.mark.asyncio
async def test_build_invalid_combination(jaffle_shop_server: "DbtCoreMcpServer"):
    """Test that combining select_state_modified and select raises error."""
    with pytest.raises(ValueError, match="Cannot use both select_state_modified\\* flags and select parameter"):
        await build_models(
            ctx=None,
            select="customers",
            exclude=None,
            select_state_modified=True,
            select_state_modified_plus_downstream=False,
            full_refresh=False,
            resource_types=None,
            fail_fast=False,
            state=jaffle_shop_server.state,
        )


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
        await build_models(
            ctx=None,
            select=None,
            exclude=None,
            select_state_modified=True,
            select_state_modified_plus_downstream=False,
            full_refresh=False,
            resource_types=None,
            fail_fast=False,
            state=jaffle_shop_server.state,
        )


@pytest.mark.asyncio
async def test_build_creates_state(seeded_jaffle_shop_server: "DbtCoreMcpServer"):
    """Test that successful build creates state for modified runs."""
    assert seeded_jaffle_shop_server.project_dir is not None
    state_dir = seeded_jaffle_shop_server.project_dir / "target" / "state_last_run"

    # First build should create state
    result = await build_models(
        ctx=None,
        select=None,
        exclude=None,
        select_state_modified=False,
        select_state_modified_plus_downstream=False,
        full_refresh=False,
        resource_types=None,
        fail_fast=False,
        state=seeded_jaffle_shop_server.state,
    )

    assert result["status"] == "success"
    assert state_dir.exists()
    assert (state_dir / "manifest.json").exists()


@pytest.mark.asyncio
async def test_build_fail_fast(seeded_jaffle_shop_server: "DbtCoreMcpServer"):
    """Test fail_fast flag is passed to dbt."""
    result = await build_models(
        ctx=None,
        select=None,
        exclude=None,
        select_state_modified=False,
        select_state_modified_plus_downstream=False,
        full_refresh=False,
        resource_types=None,
        fail_fast=True,
        state=seeded_jaffle_shop_server.state,
    )

    assert result["status"] == "success"
    assert "--fail-fast" in result["command"]


@pytest.mark.asyncio
async def test_build_exclude(seeded_jaffle_shop_server: "DbtCoreMcpServer"):
    """Test excluding specific models."""
    result = await build_models(
        ctx=None,
        select=None,
        exclude="customers",
        select_state_modified=False,
        select_state_modified_plus_downstream=False,
        full_refresh=False,
        resource_types=None,
        fail_fast=False,
        state=seeded_jaffle_shop_server.state,
    )

    assert result["status"] == "success"
    assert "--exclude customers" in result["command"]

    # Verify command includes exclude parameter
    assert "build" in result["command"]
