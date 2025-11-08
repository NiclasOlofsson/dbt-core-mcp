"""
Tests for unified domain-based tools (get_resource_info, list_resources).
"""

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from dbt_core_mcp.server import DbtCoreMcpServer


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


def test_list_resources_all(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test list_resources returns all resources when no filter specified."""
    assert jaffle_shop_server.manifest is not None
    resources = jaffle_shop_server.manifest.get_resources()

    assert len(resources) == 11

    # Count by type
    types = {}
    for r in resources:
        rt = r["resource_type"]
        types[rt] = types.get(rt, 0) + 1

    assert types == {"model": 3, "source": 2, "seed": 2, "snapshot": 1, "test": 3}


def test_list_resources_filter_models(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test list_resources filters by resource_type='model'."""
    assert jaffle_shop_server.manifest is not None
    resources = jaffle_shop_server.manifest.get_resources("model")

    assert len(resources) == 3
    assert all(r["resource_type"] == "model" for r in resources)

    names = {r["name"] for r in resources}
    assert names == {"stg_customers", "stg_orders", "customers"}


def test_list_resources_filter_sources(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test list_resources filters by resource_type='source'."""
    assert jaffle_shop_server.manifest is not None
    resources = jaffle_shop_server.manifest.get_resources("source")

    assert len(resources) == 2
    assert all(r["resource_type"] == "source" for r in resources)

    identifiers = {(r["source_name"], r["name"]) for r in resources}
    assert identifiers == {("jaffle_shop", "customers"), ("jaffle_shop", "orders")}


def test_list_resources_filter_seeds(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test list_resources filters by resource_type='seed'."""
    assert jaffle_shop_server.manifest is not None
    resources = jaffle_shop_server.manifest.get_resources("seed")

    assert len(resources) == 2
    names = {r["name"] for r in resources}
    assert names == {"raw_orders", "raw_customers"}


def test_list_resources_filter_tests(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test list_resources filters by resource_type='test'."""
    assert jaffle_shop_server.manifest is not None
    resources = jaffle_shop_server.manifest.get_resources("test")

    assert len(resources) == 3
    assert all(r["resource_type"] == "test" for r in resources)


def test_list_resources_consistent_structure(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test list_resources returns consistent structure across resource types."""
    assert jaffle_shop_server.manifest is not None
    resources = jaffle_shop_server.manifest.get_resources()

    common_keys = {"name", "unique_id", "resource_type", "description", "tags", "package_name"}

    # Check all resources have common keys
    for r in resources:
        assert common_keys.issubset(r.keys()), f"Resource {r['name']} missing common keys"


def test_list_resources_invalid_type(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test list_resources raises ValueError for invalid resource_type."""
    assert jaffle_shop_server.manifest is not None
    with pytest.raises(ValueError, match="Invalid resource_type"):
        jaffle_shop_server.manifest.get_resources("invalid_type")
