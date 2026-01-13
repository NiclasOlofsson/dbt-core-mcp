"""Tests for analyze_impact tool."""

from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest

from dbt_core_mcp.context import DbtCoreServerContext
from dbt_core_mcp.tools.analyze_impact import _implementation as analyze_impact_impl  # type: ignore[reportPrivateUsage]


@pytest.fixture
def mock_state() -> Mock:
    """Create a mock server state for testing."""
    state = Mock(spec=DbtCoreServerContext)
    state.ensure_initialized = AsyncMock()

    # Mock manifest with analyze_impact method
    mock_manifest = Mock()

    def mock_analyze_impact(name: str, resource_type: str | None = None) -> dict[str, Any]:
        # Handle multiple matches
        if name == "customers" and resource_type is None:
            return {
                "multiple_matches": True,
                "match_count": 2,
            }

        # Handle not found
        if name == "nonexistent":
            raise ValueError(f"Resource '{name}' not found")

        # Model impact
        if name == "stg_customers" and resource_type in ("model", None):
            return {
                "resource": {
                    "name": "stg_customers",
                    "resource_type": "model",
                },
                "impact": {
                    "models_affected": [{"name": "customers", "distance": 1, "resource_type": "model"}],
                    "models_affected_count": 1,
                    "tests_affected_count": 2,
                    "total_affected": 3,
                },
                "affected_by_distance": {
                    "1": [{"name": "customers", "distance": 1, "resource_type": "model"}],
                },
                "recommendation": "dbt build --select stg_customers+",
                "message": "Medium impact: 1 model, 2 tests affected",
            }

        # Source impact
        if name == "jaffle_shop.customers" and resource_type in ("source", None):
            return {
                "resource": {
                    "name": "customers",
                    "source_name": "jaffle_shop",
                    "resource_type": "source",
                },
                "impact": {
                    "models_affected": [{"name": "stg_customers", "distance": 1, "resource_type": "model"}],
                    "models_affected_count": 1,
                    "total_affected": 1,
                },
                "affected_by_distance": {
                    "1": [{"name": "stg_customers", "distance": 1, "resource_type": "model"}],
                },
                "recommendation": "dbt build --select source:jaffle_shop.customers+",
                "message": "Medium impact: 1 model affected",
            }

        # Seed impact
        if name == "raw_customers" and resource_type in ("seed", None):
            return {
                "resource": {
                    "name": "raw_customers",
                    "resource_type": "seed",
                },
                "impact": {
                    "models_affected": [],
                    "models_affected_count": 0,
                    "total_affected": 0,
                },
                "affected_by_distance": {},
                "recommendation": "dbt seed --select raw_customers",
                "message": "No impact: no downstream dependencies",
            }

        # Model impact (for customers)
        if name == "customers" and resource_type == "model":
            return {
                "resource": {
                    "name": "customers",
                    "resource_type": "model",
                },
                "impact": {
                    "models_affected": [],
                    "models_affected_count": 0,
                    "total_affected": 0,
                },
                "affected_by_distance": {},
                "recommendation": "dbt build --select customers",
                "message": "No impact: no downstream dependencies",
            }

        raise ValueError(f"Resource '{name}' not found")

    mock_manifest.analyze_impact = mock_analyze_impact
    state.manifest = mock_manifest

    return state


@pytest.mark.asyncio
async def test_analyze_impact_model(mock_state: Mock) -> None:
    """Test analyze_impact for a model."""
    result = await analyze_impact_impl(None, "stg_customers", "model", mock_state, force_parse=False)

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
async def test_analyze_impact_source(mock_state: Mock) -> None:
    """Test analyze_impact for a source."""
    result = await analyze_impact_impl(None, "jaffle_shop.customers", "source", mock_state, force_parse=False)

    assert result["resource"]["resource_type"] == "source"
    assert "impact" in result

    # Source should have downstream models
    assert result["impact"]["models_affected_count"] >= 1
    assert "source:" in result["recommendation"] or "+" in result["recommendation"]


@pytest.mark.asyncio
async def test_analyze_impact_seed(mock_state: Mock) -> None:
    """Test analyze_impact for a seed."""
    result = await analyze_impact_impl(None, "raw_customers", "seed", mock_state, force_parse=False)

    assert result["resource"]["name"] == "raw_customers"
    assert result["resource"]["resource_type"] == "seed"
    assert "dbt seed" in result["recommendation"]


@pytest.mark.asyncio
async def test_analyze_impact_distance_grouping(mock_state: Mock) -> None:
    """Test analyze_impact groups affected resources by distance."""
    result = await analyze_impact_impl(None, "stg_customers", "model", mock_state, force_parse=False)

    assert "affected_by_distance" in result
    # Should have at least distance 1 (immediate dependents)
    assert len(result["affected_by_distance"]) >= 1
    assert "1" in result["affected_by_distance"]

    # Each distance group should have resources
    for _distance, resources in result["affected_by_distance"].items():
        assert len(resources) > 0
        assert all("distance" in r for r in resources)


@pytest.mark.asyncio
async def test_analyze_impact_models_sorted(mock_state: Mock) -> None:
    """Test analyze_impact sorts affected models by distance."""
    result = await analyze_impact_impl(None, "stg_customers", "model", mock_state, force_parse=False)

    models = result["impact"]["models_affected"]
    if len(models) > 1:
        # Verify sorted by distance
        distances = [m["distance"] for m in models]
        assert distances == sorted(distances)


@pytest.mark.asyncio
async def test_analyze_impact_multiple_matches(mock_state: Mock) -> None:
    """Test analyze_impact returns multiple_matches for ambiguous names."""
    result = await analyze_impact_impl(None, "customers", None, mock_state, force_parse=False)  # Matches both model and source

    assert result["multiple_matches"] is True
    assert result["match_count"] == 2


@pytest.mark.asyncio
async def test_analyze_impact_not_found(mock_state: Mock) -> None:
    """Test analyze_impact raises ValueError when resource not found."""
    with pytest.raises(ValueError, match="Impact analysis error"):
        await analyze_impact_impl(None, "nonexistent", "model", mock_state, force_parse=False)


@pytest.mark.asyncio
async def test_analyze_impact_message_levels(mock_state: Mock) -> None:
    """Test analyze_impact provides appropriate impact level messages."""
    result = await analyze_impact_impl(None, "customers", "model", mock_state, force_parse=False)

    # Should have a message field
    assert "message" in result
    # Message should mention impact level
    assert any(word in result["message"].lower() for word in ["no", "low", "medium", "high", "impact"])
