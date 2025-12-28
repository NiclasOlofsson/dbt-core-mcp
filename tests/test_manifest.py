"""
Tests for ManifestLoader helper methods (get_resource_node).
"""

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from dbt_core_mcp.server import DbtCoreMcpServer


# Resource Discovery Tests


def test_get_resource_node_model(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test get_resource_node with a model resource type filter."""
    assert jaffle_shop_server.manifest is not None
    result = jaffle_shop_server.manifest.get_resource_node("customers", "model")

    assert result["resource_type"] == "model"
    assert result["unique_id"] == "model.jaffle_shop.customers"
    assert result["name"] == "customers"


def test_get_resource_node_source(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test get_resource_node with a source resource type filter."""
    assert jaffle_shop_server.manifest is not None
    result = jaffle_shop_server.manifest.get_resource_node("customers", "source")

    assert result["resource_type"] == "source"
    assert result["source_name"] == "jaffle_shop"
    assert result["name"] == "customers"


def test_get_resource_node_source_dot_notation(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test get_resource_node with source_name.table_name notation."""
    assert jaffle_shop_server.manifest is not None
    result = jaffle_shop_server.manifest.get_resource_node("jaffle_shop.customers")

    assert result["resource_type"] == "source"
    assert result["unique_id"] == "source.jaffle_shop.jaffle_shop.customers"
    assert result["source_name"] == "jaffle_shop"


def test_get_resource_node_multiple_matches(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test get_resource_node returns all matches when ambiguous."""
    assert jaffle_shop_server.manifest is not None
    result = jaffle_shop_server.manifest.get_resource_node("customers")

    assert result["multiple_matches"] is True
    assert result["match_count"] == 2
    assert result["name"] == "customers"
    assert len(result["matches"]) == 2

    types = {m["resource_type"] for m in result["matches"]}
    assert types == {"model", "source"}


def test_get_resource_node_not_found(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test get_resource_node raises ValueError when resource not found."""
    assert jaffle_shop_server.manifest is not None
    with pytest.raises(ValueError, match="Resource 'nonexistent' not found"):
        jaffle_shop_server.manifest.get_resource_node("nonexistent")


def test_get_resource_node_invalid_type(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test get_resource_node raises ValueError for invalid resource_type."""
    assert jaffle_shop_server.manifest is not None
    with pytest.raises(ValueError, match="Invalid resource_type"):
        jaffle_shop_server.manifest.get_resource_node("customers", "invalid_type")
