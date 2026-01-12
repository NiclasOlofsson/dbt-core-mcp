"""
Tests for get_lineage tool.
"""

from typing import TYPE_CHECKING

import pytest

from dbt_core_mcp.tools.get_lineage import _implementation as get_lineage_impl

if TYPE_CHECKING:
    from dbt_core_mcp.server import DbtCoreMcpServer


@pytest.mark.asyncio
async def test_get_lineage_model_both_directions(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test get_lineage for a model in both directions."""
    result = await get_lineage_impl("customers", "model", "both", None, jaffle_shop_server.state)

    assert result["resource"]["name"] == "customers"
    assert result["resource"]["resource_type"] == "model"
    assert "upstream" in result
    assert "downstream" in result
    assert "stats" in result

    # Customers model depends on stg_customers and stg_orders
    assert result["stats"]["upstream_count"] >= 2


@pytest.mark.asyncio
async def test_get_lineage_upstream_only(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test get_lineage with upstream direction only."""
    result = await get_lineage_impl("customers", "model", "upstream", None, jaffle_shop_server.state)

    assert result["resource"]["name"] == "customers"
    assert "upstream" in result
    assert "downstream" not in result
    assert result["stats"]["upstream_count"] >= 2
    assert result["stats"]["downstream_count"] == 0


@pytest.mark.asyncio
async def test_get_lineage_downstream_only(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test get_lineage with downstream direction only."""
    result = await get_lineage_impl("stg_customers", "model", "downstream", None, jaffle_shop_server.state)

    assert result["resource"]["name"] == "stg_customers"
    assert "upstream" not in result
    assert "downstream" in result
    assert result["stats"]["downstream_count"] >= 1  # customers depends on stg_customers


@pytest.mark.asyncio
async def test_get_lineage_with_depth_limit(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test get_lineage with depth limit."""
    result = await get_lineage_impl("customers", "model", "upstream", 1, jaffle_shop_server.state)

    assert result["resource"]["name"] == "customers"
    assert "upstream" in result

    # With depth=1, should only get immediate parents
    for node in result["upstream"]:
        assert node["distance"] == 1


@pytest.mark.asyncio
async def test_get_lineage_source(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test get_lineage for a source."""
    result = await get_lineage_impl("jaffle_shop.customers", "source", "downstream", None, jaffle_shop_server.state)

    assert result["resource"]["resource_type"] == "source"
    assert "downstream" in result


@pytest.mark.asyncio
async def test_get_lineage_auto_detect(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test get_lineage with auto-detection (no resource_type specified)."""
    result = await get_lineage_impl("stg_customers", None, "both", None, jaffle_shop_server.state)

    # Should find the model
    assert result["resource"]["name"] == "stg_customers"
    assert result["resource"]["resource_type"] == "model"


@pytest.mark.asyncio
async def test_get_lineage_multiple_matches(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test get_lineage when multiple resources match the name."""
    # "customers" exists as both a model and a source
    result = await get_lineage_impl("customers", None, "both", None, jaffle_shop_server.state)

    # Should return multiple_matches structure
    assert result.get("multiple_matches") is True or result["resource"]["name"] == "customers"


@pytest.mark.asyncio
async def test_get_lineage_invalid_direction(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test get_lineage with invalid direction raises ValueError."""
    import pytest

    with pytest.raises(ValueError, match="Invalid direction|Lineage error"):
        await get_lineage_impl("customers", "model", "invalid", None, jaffle_shop_server.state)


@pytest.mark.asyncio
async def test_get_lineage_not_found(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test get_lineage with non-existent resource raises ValueError."""
    import pytest

    with pytest.raises(ValueError, match="not found|Lineage error"):
        await get_lineage_impl("nonexistent_model", None, "both", None, jaffle_shop_server.state)
