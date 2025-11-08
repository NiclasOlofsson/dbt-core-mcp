"""
Tests for unified domain-based tools (get_resource_info, list_resources).
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


def test_get_resource_info_with_compiled_sql(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test get_resource_info includes compiled SQL for models when available in manifest."""
    assert jaffle_shop_server.manifest is not None

    # Get resource info for a model - check if compiled SQL is available
    result = jaffle_shop_server.manifest.get_resource_info(
        name="customers",
        resource_type="model",
        include_compiled_sql=True
    )

    assert result["name"] == "customers"
    assert result["resource_type"] == "model"
    assert "compiled_sql" in result
    assert "compiled_sql_cached" in result
    
    # The compiled SQL may be None if not yet compiled, which is fine
    # The test is verifying the structure exists and the logic works
    if result["compiled_sql"] is not None:
        # If it's there, verify it's compiled (no Jinja)
        assert "{{" not in result["compiled_sql"]
        assert "jaffle_shop" in result["compiled_sql"] or "main" in result["compiled_sql"]
        assert result["compiled_sql_cached"] is True
    else:
        # Not compiled yet - should indicate it's not cached
        assert result["compiled_sql_cached"] is False


def test_get_resource_info_skip_compiled_sql(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test get_resource_info can skip compiled SQL with include_compiled_sql=False."""
    assert jaffle_shop_server.manifest is not None

    result = jaffle_shop_server.manifest.get_resource_info(
        name="customers",
        resource_type="model",
        include_compiled_sql=False
    )

    assert result["name"] == "customers"
    assert result["resource_type"] == "model"
    assert "compiled_sql" not in result


def test_get_resource_info_compiled_sql_only_for_models(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test get_resource_info only includes compiled SQL for models, not sources/seeds."""
    assert jaffle_shop_server.manifest is not None

    # Test with source - should not have compiled_sql even if requested
    source_result = jaffle_shop_server.manifest.get_resource_info(
        name="jaffle_shop.customers",
        resource_type="source",
        include_compiled_sql=True
    )
    assert source_result["resource_type"] == "source"
    assert "compiled_sql" not in source_result

    # Test with seed - should not have compiled_sql even if requested
    seed_result = jaffle_shop_server.manifest.get_resource_info(
        name="raw_customers",
        resource_type="seed",
        include_compiled_sql=True
    )
    assert seed_result["resource_type"] == "seed"
    assert "compiled_sql" not in seed_result


# List Resources Tests


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


# Lineage Tests


def test_get_lineage_model_both_directions(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test get_lineage for a model in both directions."""
    assert jaffle_shop_server.manifest is not None
    result = jaffle_shop_server.manifest.get_lineage("customers", "model", "both")

    assert result["resource"]["name"] == "customers"
    assert result["resource"]["resource_type"] == "model"
    assert "upstream" in result
    assert "downstream" in result
    assert "stats" in result

    # Customers model depends on stg_customers and stg_orders
    assert result["stats"]["upstream_count"] >= 2


def test_get_lineage_upstream_only(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test get_lineage with upstream direction only."""
    assert jaffle_shop_server.manifest is not None
    result = jaffle_shop_server.manifest.get_lineage("customers", "model", "upstream")

    assert "upstream" in result
    assert "downstream" not in result
    assert result["stats"]["upstream_count"] >= 2
    assert result["stats"]["downstream_count"] == 0


def test_get_lineage_downstream_only(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test get_lineage with downstream direction only."""
    assert jaffle_shop_server.manifest is not None
    result = jaffle_shop_server.manifest.get_lineage("stg_customers", "model", "downstream")

    assert "downstream" in result
    assert "upstream" not in result
    assert result["stats"]["downstream_count"] >= 1  # At least the customers model depends on it
    assert result["stats"]["upstream_count"] == 0


def test_get_lineage_with_depth_limit(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test get_lineage with depth limitation."""
    assert jaffle_shop_server.manifest is not None
    result = jaffle_shop_server.manifest.get_lineage("customers", "model", "upstream", depth=1)

    # Depth 1 should only show immediate dependencies
    assert "upstream" in result
    assert all(node["distance"] == 1 for node in result["upstream"])


def test_get_lineage_source(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test get_lineage for a source (sources typically have no upstream, only downstream)."""
    assert jaffle_shop_server.manifest is not None
    result = jaffle_shop_server.manifest.get_lineage("jaffle_shop.customers", "source")

    assert result["resource"]["resource_type"] == "source"
    assert result["stats"]["upstream_count"] == 0  # Sources don't have upstream dependencies
    assert result["stats"]["downstream_count"] >= 1  # stg_customers depends on this source


def test_get_lineage_auto_detect(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test get_lineage with auto-detection (using type filter to avoid ambiguity)."""
    assert jaffle_shop_server.manifest is not None
    result = jaffle_shop_server.manifest.get_lineage("stg_customers", resource_type="model")

    assert result["resource"]["name"] == "stg_customers"
    assert result["resource"]["resource_type"] == "model"


def test_get_lineage_multiple_matches(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test get_lineage returns multiple_matches for ambiguous names."""
    assert jaffle_shop_server.manifest is not None
    result = jaffle_shop_server.manifest.get_lineage("customers")  # Matches both model and source

    assert result["multiple_matches"] is True
    assert result["match_count"] == 2


def test_get_lineage_invalid_direction(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test get_lineage raises ValueError for invalid direction."""
    assert jaffle_shop_server.manifest is not None
    with pytest.raises(ValueError, match="Invalid direction"):
        jaffle_shop_server.manifest.get_lineage("customers", "model", "invalid")


def test_get_lineage_not_found(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test get_lineage raises ValueError when resource not found."""
    assert jaffle_shop_server.manifest is not None
    with pytest.raises(ValueError, match="not found"):
        jaffle_shop_server.manifest.get_lineage("nonexistent", "model")


# Impact Analysis Tests


def test_analyze_impact_model(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test analyze_impact for a model."""
    assert jaffle_shop_server.manifest is not None
    result = jaffle_shop_server.manifest.analyze_impact("stg_customers", "model")

    assert result["resource"]["name"] == "stg_customers"
    assert result["resource"]["resource_type"] == "model"
    assert "impact" in result
    assert "affected_by_distance" in result
    assert "recommendation" in result
    assert "message" in result

    # stg_customers should have downstream dependencies (customers model)
    assert result["impact"]["models_affected_count"] >= 1
    assert result["impact"]["total_affected"] >= 1


def test_analyze_impact_source(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test analyze_impact for a source."""
    assert jaffle_shop_server.manifest is not None
    result = jaffle_shop_server.manifest.analyze_impact("jaffle_shop.customers", "source")

    assert result["resource"]["resource_type"] == "source"
    assert "impact" in result

    # Source should have downstream models
    assert result["impact"]["models_affected_count"] >= 1
    assert "source:" in result["recommendation"] or "+" in result["recommendation"]


def test_analyze_impact_seed(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test analyze_impact for a seed."""
    assert jaffle_shop_server.manifest is not None
    result = jaffle_shop_server.manifest.analyze_impact("raw_customers", "seed")

    assert result["resource"]["name"] == "raw_customers"
    assert result["resource"]["resource_type"] == "seed"
    assert "dbt seed" in result["recommendation"]


def test_analyze_impact_distance_grouping(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test analyze_impact groups affected resources by distance."""
    assert jaffle_shop_server.manifest is not None
    result = jaffle_shop_server.manifest.analyze_impact("stg_customers", "model")

    assert "affected_by_distance" in result
    # Should have at least distance 1 (immediate dependents)
    assert len(result["affected_by_distance"]) >= 1
    assert "1" in result["affected_by_distance"]

    # Each distance group should have resources
    for _distance, resources in result["affected_by_distance"].items():
        assert len(resources) > 0
        assert all("distance" in r for r in resources)


def test_analyze_impact_models_sorted(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test analyze_impact sorts affected models by distance."""
    assert jaffle_shop_server.manifest is not None
    result = jaffle_shop_server.manifest.analyze_impact("stg_customers", "model")

    models = result["impact"]["models_affected"]
    if len(models) > 1:
        # Verify sorted by distance
        distances = [m["distance"] for m in models]
        assert distances == sorted(distances)


def test_analyze_impact_multiple_matches(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test analyze_impact returns multiple_matches for ambiguous names."""
    assert jaffle_shop_server.manifest is not None
    result = jaffle_shop_server.manifest.analyze_impact("customers")  # Matches both model and source

    assert result["multiple_matches"] is True
    assert result["match_count"] == 2


def test_analyze_impact_not_found(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test analyze_impact raises ValueError when resource not found."""
    assert jaffle_shop_server.manifest is not None
    with pytest.raises(ValueError, match="not found"):
        jaffle_shop_server.manifest.analyze_impact("nonexistent", "model")


def test_analyze_impact_message_levels(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test analyze_impact provides appropriate impact level messages."""
    assert jaffle_shop_server.manifest is not None
    result = jaffle_shop_server.manifest.analyze_impact("customers", "model")

    # Should have a message field
    assert "message" in result
    # Message should mention impact level
    assert any(word in result["message"].lower() for word in ["no", "low", "medium", "high", "impact"])
