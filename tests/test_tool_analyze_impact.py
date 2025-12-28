"""
Tests for analyze_impact tool.
"""

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from dbt_core_mcp.server import DbtCoreMcpServer


@pytest.mark.asyncio
async def test_analyze_impact_model(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test analyze_impact for a model."""
    result = await jaffle_shop_server.toolImpl_analyze_impact("stg_customers", "model")

    assert result["resource"]["name"] == "stg_customers"
    assert result["resource"]["resource_type"] == "model"
    assert "impact" in result
    assert "affected_by_distance" in result
    assert "recommendation" in result
    assert "message" in result

    # stg_customers should have downstream dependencies (customers model)
    assert result["impact"]["models_affected_count"] >= 1
    assert result["impact"]["total_affected"] >= 1


@pytest.mark.asyncio
async def test_analyze_impact_source(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test analyze_impact for a source."""
    result = await jaffle_shop_server.toolImpl_analyze_impact("jaffle_shop.customers", "source")

    assert result["resource"]["resource_type"] == "source"
    assert "impact" in result

    # Source should have downstream models
    assert result["impact"]["models_affected_count"] >= 1
    assert "source:" in result["recommendation"] or "+" in result["recommendation"]


@pytest.mark.asyncio
async def test_analyze_impact_seed(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test analyze_impact for a seed."""
    result = await jaffle_shop_server.toolImpl_analyze_impact("raw_customers", "seed")

    assert result["resource"]["name"] == "raw_customers"
    assert result["resource"]["resource_type"] == "seed"
    assert "dbt seed" in result["recommendation"]


@pytest.mark.asyncio
async def test_analyze_impact_distance_grouping(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test analyze_impact groups affected resources by distance."""
    result = await jaffle_shop_server.toolImpl_analyze_impact("stg_customers", "model")

    assert "affected_by_distance" in result
    # Should have at least distance 1 (immediate dependents)
    assert len(result["affected_by_distance"]) >= 1
    assert "1" in result["affected_by_distance"]

    # Each distance group should have resources
    for _distance, resources in result["affected_by_distance"].items():
        assert len(resources) > 0
        assert all("distance" in r for r in resources)


@pytest.mark.asyncio
async def test_analyze_impact_models_sorted(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test analyze_impact sorts affected models by distance."""
    result = await jaffle_shop_server.toolImpl_analyze_impact("stg_customers", "model")

    models = result["impact"]["models_affected"]
    if len(models) > 1:
        # Verify sorted by distance
        distances = [m["distance"] for m in models]
        assert distances == sorted(distances)


@pytest.mark.asyncio
async def test_analyze_impact_multiple_matches(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test analyze_impact returns multiple_matches for ambiguous names."""
    result = await jaffle_shop_server.toolImpl_analyze_impact("customers")  # Matches both model and source

    assert result["multiple_matches"] is True
    assert result["match_count"] == 2


@pytest.mark.asyncio
async def test_analyze_impact_not_found(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test analyze_impact raises ValueError when resource not found."""
    with pytest.raises(ValueError, match="Impact analysis error"):
        await jaffle_shop_server.toolImpl_analyze_impact("nonexistent", "model")


@pytest.mark.asyncio
async def test_analyze_impact_message_levels(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test analyze_impact provides appropriate impact level messages."""
    result = await jaffle_shop_server.toolImpl_analyze_impact("customers", "model")

    # Should have a message field
    assert "message" in result
    # Message should mention impact level
    assert any(word in result["message"].lower() for word in ["no", "low", "medium", "high", "impact"])
