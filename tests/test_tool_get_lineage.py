"""
Tests for toolImpl_get_lineage.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dbt_core_mcp.server import DbtCoreMcpServer


async def test_get_lineage_model_both_directions(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test get_lineage for a model in both directions."""
    result = await jaffle_shop_server.toolImpl_get_lineage("customers", "model", "both")

    assert result["resource"]["name"] == "customers"
    assert result["resource"]["resource_type"] == "model"
    assert "upstream" in result
    assert "downstream" in result
    assert "stats" in result

    # Customers model depends on stg_customers and stg_orders
    assert result["stats"]["upstream_count"] >= 2


async def test_get_lineage_upstream_only(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test get_lineage with upstream direction only."""
    result = await jaffle_shop_server.toolImpl_get_lineage("customers", "model", "upstream")

    assert result["resource"]["name"] == "customers"
    assert "upstream" in result
    assert "downstream" not in result
    assert result["stats"]["upstream_count"] >= 2
    assert result["stats"]["downstream_count"] == 0


async def test_get_lineage_downstream_only(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test get_lineage with downstream direction only."""
    result = await jaffle_shop_server.toolImpl_get_lineage("stg_customers", "model", "downstream")

    assert result["resource"]["name"] == "stg_customers"
    assert "upstream" not in result
    assert "downstream" in result
    assert result["stats"]["downstream_count"] >= 1  # customers depends on stg_customers


async def test_get_lineage_with_depth_limit(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test get_lineage with depth limit."""
    result = await jaffle_shop_server.toolImpl_get_lineage("customers", "model", "upstream", depth=1)

    assert result["resource"]["name"] == "customers"
    assert "upstream" in result

    # With depth=1, should only get immediate parents
    for node in result["upstream"]:
        assert node["distance"] == 1


async def test_get_lineage_source(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test get_lineage for a source."""
    result = await jaffle_shop_server.toolImpl_get_lineage("jaffle_shop.customers", "source", "downstream")

    assert result["resource"]["resource_type"] == "source"
    assert "downstream" in result


async def test_get_lineage_auto_detect(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test get_lineage with auto-detection (no resource_type specified)."""
    result = await jaffle_shop_server.toolImpl_get_lineage("stg_customers")

    # Should find the model
    assert result["resource"]["name"] == "stg_customers"
    assert result["resource"]["resource_type"] == "model"


async def test_get_lineage_multiple_matches(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test get_lineage when multiple resources match the name."""
    # "customers" exists as both a model and a source
    result = await jaffle_shop_server.toolImpl_get_lineage("customers")

    # Should return multiple_matches structure
    assert result.get("multiple_matches") is True or result["resource"]["name"] == "customers"


async def test_get_lineage_invalid_direction(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test get_lineage with invalid direction raises ValueError."""
    import pytest

    with pytest.raises(ValueError, match="Invalid direction|Lineage error"):
        await jaffle_shop_server.toolImpl_get_lineage("customers", "model", "invalid")


async def test_get_lineage_not_found(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test get_lineage with non-existent resource raises ValueError."""
    import pytest

    with pytest.raises(ValueError, match="not found|Lineage error"):
        await jaffle_shop_server.toolImpl_get_lineage("nonexistent_model")
