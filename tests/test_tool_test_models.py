"""Tests for test_models tool."""

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from dbt_core_mcp.server import DbtCoreMcpServer


@pytest.mark.asyncio
async def test_test_all_models(jaffle_shop_server: "DbtCoreMcpServer"):
    """Test running all tests."""
    result = await jaffle_shop_server.toolImpl_test_models(ctx=None)

    assert result["status"] == "success"
    assert "results" in result
    assert "elapsed_time" in result
    assert "test" in result["command"]

    # Jaffle shop has tests defined in schema.yml
    results = result["results"]
    assert len(results) > 0

    # Check that tests passed
    for test_result in results:
        assert test_result["status"] in ["pass", "success"]


@pytest.mark.asyncio
async def test_test_specific_model(jaffle_shop_server: "DbtCoreMcpServer"):
    """Test running tests for a specific model."""
    result = await jaffle_shop_server.toolImpl_test_models(ctx=None, select="customers")

    assert result["status"] == "success"
    assert "results" in result
    assert "-s customers" in result["command"]

    # Should have tests related to customers model
    results = result["results"]
    assert len(results) > 0


@pytest.mark.asyncio
async def test_test_invalid_combination(jaffle_shop_server: "DbtCoreMcpServer"):
    """Test that combining select_state_modified and select raises error."""
    with pytest.raises(ValueError, match="Cannot use both select_state_modified\\* flags and select parameter"):
        await jaffle_shop_server.toolImpl_test_models(ctx=None, select="customers", select_state_modified=True)


@pytest.mark.asyncio
async def test_test_modified_only_no_state_tests_all(jaffle_shop_server: "DbtCoreMcpServer"):
    """Test select_state_modified without state returns success (cannot determine modifications)."""
    # Remove state if it exists
    assert jaffle_shop_server.project_dir is not None
    state_dir = jaffle_shop_server.project_dir / "target" / "state_last_run"
    if state_dir.exists():
        import shutil

        shutil.rmtree(state_dir)

    # With no state, select_state_modified should return success with message (not test anything)
    result = await jaffle_shop_server.toolImpl_test_models(ctx=None, select_state_modified=True)

    assert result["status"] == "success"
    assert "No previous state" in result["message"]
    assert result["results"] == []


@pytest.mark.asyncio
async def test_test_creates_uses_state(jaffle_shop_server: "DbtCoreMcpServer"):
    """Test that running tests uses state from previous run."""
    # First run models to create state
    assert jaffle_shop_server.project_dir is not None
    state_dir = jaffle_shop_server.project_dir / "target" / "state_last_run"

    # Ensure we have state by running models first
    run_result = await jaffle_shop_server.toolImpl_run_models(ctx=None)
    assert run_result["status"] == "success"
    assert state_dir.exists()

    # Now select_state_modified should work (even if nothing modified, should succeed)
    result = await jaffle_shop_server.toolImpl_test_models(ctx=None, select_state_modified=True)

    # Should succeed even with no modified models
    assert result["status"] == "success"
    assert "--state target/state_last_run" in result["command"]


@pytest.mark.asyncio
async def test_test_fail_fast(jaffle_shop_server: "DbtCoreMcpServer"):
    """Test fail_fast flag is passed to dbt."""
    result = await jaffle_shop_server.toolImpl_test_models(ctx=None, fail_fast=True)

    assert result["status"] == "success"
    assert "--fail-fast" in result["command"]


@pytest.mark.asyncio
async def test_test_exclude(jaffle_shop_server: "DbtCoreMcpServer"):
    """Test excluding specific tests."""
    result = await jaffle_shop_server.toolImpl_test_models(ctx=None, exclude="not_null*")

    assert result["status"] == "success"
    assert "--exclude not_null*" in result["command"]
