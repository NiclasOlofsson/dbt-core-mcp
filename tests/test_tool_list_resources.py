"""
Tests for list_resources tool.
"""

from typing import TYPE_CHECKING

import pytest

from dbt_core_mcp.tools.list_resources import _implementation as list_resources_impl

if TYPE_CHECKING:
    from dbt_core_mcp.server import DbtCoreMcpServer


@pytest.mark.asyncio
async def test_list_resources_all(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test listing all resources without filter."""
    result = await list_resources_impl(None, None, jaffle_shop_server.state)

    assert isinstance(result, list)
    assert len(result) > 0

    # Should have multiple resource types
    resource_types = {r["resource_type"] for r in result}
    assert "model" in resource_types
    assert "source" in resource_types


@pytest.mark.asyncio
async def test_list_resources_filter_models(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test filtering by model resource type."""
    result = await list_resources_impl(None, "model", jaffle_shop_server.state)

    assert isinstance(result, list)
    assert len(result) > 0

    # All results should be models
    for resource in result:
        assert resource["resource_type"] == "model"

    # Should include known models
    model_names = {r["name"] for r in result}
    assert "customers" in model_names


@pytest.mark.asyncio
async def test_list_resources_filter_sources(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test filtering by source resource type."""
    result = await list_resources_impl(None, "source", jaffle_shop_server.state)

    assert isinstance(result, list)
    assert len(result) > 0

    # All results should be sources
    for resource in result:
        assert resource["resource_type"] == "source"

    # Should include known sources
    source_names = {r["name"] for r in result}
    assert "customers" in source_names or "orders" in source_names


@pytest.mark.asyncio
async def test_list_resources_filter_seeds(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test filtering by seed resource type."""
    result = await list_resources_impl(None, "seed", jaffle_shop_server.state)

    assert isinstance(result, list)
    assert len(result) > 0

    # All results should be seeds
    for resource in result:
        assert resource["resource_type"] == "seed"

    # Should include known seeds
    seed_names = {r["name"] for r in result}
    assert "raw_customers" in seed_names or "raw_orders" in seed_names


@pytest.mark.asyncio
async def test_list_resources_consistent_structure(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test that all resources have consistent structure."""
    result = await list_resources_impl(None, None, jaffle_shop_server.state)

    assert len(result) > 0

    # Check that each resource has required fields
    for resource in result:
        assert "name" in resource
        assert "unique_id" in resource
        assert "resource_type" in resource
        assert "package_name" in resource
        assert "description" in resource
        assert "tags" in resource


@pytest.mark.asyncio
async def test_list_resources_invalid_type(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test that invalid resource type raises ValueError."""
    import pytest

    with pytest.raises(ValueError, match="Invalid resource_type"):
        await list_resources_impl(None, "invalid_type", jaffle_shop_server.state)
