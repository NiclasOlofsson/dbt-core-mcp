"""Tests for list_resources tool."""

from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest

from dbt_core_mcp.context import DbtCoreServerContext
from dbt_core_mcp.tools.list_resources import _implementation as list_resources_impl  # type: ignore[reportPrivateUsage]


@pytest.fixture
def mock_state() -> Mock:
    """Create a mock server state for testing."""
    state = Mock(spec=DbtCoreServerContext)
    state.ensure_initialized = AsyncMock()

    # Mock manifest with get_resources method
    mock_manifest = Mock()

    def mock_get_resources(resource_type: str | None = None) -> list[dict[str, Any]]:
        all_resources = [
            {
                "name": "customers",
                "unique_id": "model.jaffle_shop.customers",
                "resource_type": "model",
                "package_name": "jaffle_shop",
                "description": "Customer dimension",
                "tags": ["mart"],
            },
            {
                "name": "stg_customers",
                "unique_id": "model.jaffle_shop.stg_customers",
                "resource_type": "model",
                "package_name": "jaffle_shop",
                "description": "Staging customers",
                "tags": ["staging"],
            },
            {
                "name": "customers",
                "unique_id": "source.jaffle_shop.jaffle_shop.customers",
                "resource_type": "source",
                "package_name": "jaffle_shop",
                "description": "Raw customers source",
                "tags": [],
            },
            {
                "name": "orders",
                "unique_id": "source.jaffle_shop.jaffle_shop.orders",
                "resource_type": "source",
                "package_name": "jaffle_shop",
                "description": "Raw orders source",
                "tags": [],
            },
            {
                "name": "raw_customers",
                "unique_id": "seed.jaffle_shop.raw_customers",
                "resource_type": "seed",
                "package_name": "jaffle_shop",
                "description": "Raw customer seed data",
                "tags": [],
            },
            {
                "name": "raw_orders",
                "unique_id": "seed.jaffle_shop.raw_orders",
                "resource_type": "seed",
                "package_name": "jaffle_shop",
                "description": "Raw order seed data",
                "tags": [],
            },
        ]

        if resource_type is None:
            return all_resources

        # Validate resource type
        valid_types = {"model", "source", "seed", "snapshot", "test", "analysis", "macro"}
        if resource_type not in valid_types:
            raise ValueError(f"Invalid resource_type: {resource_type}")

        return [r for r in all_resources if r["resource_type"] == resource_type]

    mock_manifest.get_resources = mock_get_resources
    state.manifest = mock_manifest

    return state


@pytest.mark.asyncio
async def test_list_resources_all(mock_state: Mock) -> None:
    """Test listing all resources without filter."""
    result = await list_resources_impl(None, None, mock_state, force_parse=False)

    assert isinstance(result, list)
    assert len(result) > 0

    # Should have multiple resource types
    resource_types = {r["resource_type"] for r in result}
    assert "model" in resource_types
    assert "source" in resource_types


@pytest.mark.asyncio
async def test_list_resources_filter_models(mock_state: Mock) -> None:
    """Test filtering by model resource type."""
    result = await list_resources_impl(None, "model", mock_state, force_parse=False)

    assert isinstance(result, list)
    assert len(result) > 0

    # All results should be models
    for resource in result:
        assert resource["resource_type"] == "model"

    # Should include known models
    model_names = {r["name"] for r in result}
    assert "customers" in model_names


@pytest.mark.asyncio
async def test_list_resources_filter_sources(mock_state: Mock) -> None:
    """Test filtering by source resource type."""
    result = await list_resources_impl(None, "source", mock_state, force_parse=False)

    assert isinstance(result, list)
    assert len(result) > 0

    # All results should be sources
    for resource in result:
        assert resource["resource_type"] == "source"

    # Should include known sources
    source_names = {r["name"] for r in result}
    assert "customers" in source_names or "orders" in source_names


@pytest.mark.asyncio
async def test_list_resources_filter_seeds(mock_state: Mock) -> None:
    """Test filtering by seed resource type."""
    result = await list_resources_impl(None, "seed", mock_state, force_parse=False)

    assert isinstance(result, list)
    assert len(result) > 0

    # All results should be seeds
    for resource in result:
        assert resource["resource_type"] == "seed"

    # Should include known seeds
    seed_names = {r["name"] for r in result}
    assert "raw_customers" in seed_names or "raw_orders" in seed_names


@pytest.mark.asyncio
async def test_list_resources_consistent_structure(mock_state: Mock) -> None:
    """Test that all resources have consistent structure."""
    result = await list_resources_impl(None, None, mock_state, force_parse=False)

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
async def test_list_resources_invalid_type(mock_state: Mock) -> None:
    """Test that invalid resource type raises ValueError."""
    with pytest.raises(ValueError, match="Invalid resource_type"):
        await list_resources_impl(None, "invalid_type", mock_state, force_parse=False)
