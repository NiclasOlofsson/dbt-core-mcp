"""Tests for get_lineage tool."""

from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest

from dbt_core_mcp.context import DbtCoreServerContext
from dbt_core_mcp.tools.get_lineage import _implementation as get_lineage_impl  # type: ignore[reportPrivateUsage]


@pytest.fixture
def mock_state() -> Mock:
    """Create a mock server state for testing."""
    state = Mock(spec=DbtCoreServerContext)
    state.ensure_initialized = AsyncMock()

    # Mock manifest with get_lineage method
    mock_manifest = Mock()

    def mock_get_lineage(name: str, resource_type: str | None = None, direction: str = "both", depth: int | None = None) -> dict[str, Any]:
        # Validate direction
        if direction not in ("upstream", "downstream", "both"):
            raise ValueError(f"Invalid direction: {direction}")

        # Handle not found
        if name == "nonexistent_model":
            raise ValueError(f"Resource '{name}' not found")

        # Handle multiple matches
        if name == "customers" and resource_type is None:
            return {
                "multiple_matches": True,
                "resource": {"name": "customers"},
            }

        # customers model
        if name == "customers" and resource_type in ("model", None):
            result: dict[str, Any] = {
                "resource": {
                    "name": "customers",
                    "resource_type": "model",
                    "unique_id": "model.jaffle_shop.customers",
                },
                "stats": {
                    "upstream_count": 2 if direction in ("upstream", "both") else 0,
                    "downstream_count": 0 if direction in ("upstream", "both") else 0,
                },
            }
            if direction in ("upstream", "both"):
                result["upstream"] = [
                    {"name": "stg_customers", "distance": 1, "resource_type": "model"},
                    {"name": "stg_orders", "distance": 1, "resource_type": "model"},
                ]
            if direction in ("downstream", "both"):
                result["downstream"] = []
            return result

        # stg_customers model
        if name == "stg_customers" and resource_type in ("model", None):
            result: dict[str, Any] = {
                "resource": {
                    "name": "stg_customers",
                    "resource_type": "model",
                },
                "stats": {
                    "upstream_count": 0,
                    "downstream_count": 1 if direction in ("downstream", "both") else 0,
                },
            }
            if direction in ("upstream", "both"):
                result["upstream"] = []
            if direction in ("downstream", "both"):
                result["downstream"] = [{"name": "customers", "distance": 1, "resource_type": "model"}]
            return result

        # jaffle_shop.customers source
        if name == "jaffle_shop.customers" and resource_type in ("source", None):
            result: dict[str, Any] = {
                "resource": {
                    "name": "customers",
                    "source_name": "jaffle_shop",
                    "resource_type": "source",
                },
                "stats": {
                    "upstream_count": 0,
                    "downstream_count": 1 if direction in ("downstream", "both") else 0,
                },
            }
            if direction in ("downstream", "both"):
                result["downstream"] = [{"name": "stg_customers", "distance": 1, "resource_type": "model"}]

            # With depth limit
            if depth == 1:
                # Return only immediate dependencies
                if "upstream" in result:
                    result["upstream"] = [n for n in result["upstream"] if n["distance"] == 1]
                if "downstream" in result:
                    result["downstream"] = [n for n in result["downstream"] if n["distance"] == 1]

            return result

        raise ValueError(f"Resource '{name}' not found")

    mock_manifest.get_lineage = mock_get_lineage
    state.manifest = mock_manifest

    return state


@pytest.mark.asyncio
async def test_get_lineage_model_both_directions(mock_state: Mock) -> None:
    """Test get_lineage for a model in both directions."""
    result = await get_lineage_impl(None, "customers", "model", "both", None, mock_state, force_parse=False)

    assert result["resource"]["name"] == "customers"
    assert result["resource"]["resource_type"] == "model"
    assert "upstream" in result
    assert "downstream" in result
    assert "stats" in result

    # Customers model depends on stg_customers and stg_orders
    assert result["stats"]["upstream_count"] >= 2


@pytest.mark.asyncio
async def test_get_lineage_upstream_only(mock_state: Mock) -> None:
    """Test get_lineage with upstream direction only."""
    result = await get_lineage_impl(None, "customers", "model", "upstream", None, mock_state, force_parse=False)

    assert result["resource"]["name"] == "customers"
    assert "upstream" in result
    assert "downstream" not in result
    assert result["stats"]["upstream_count"] >= 2
    assert result["stats"]["downstream_count"] == 0


@pytest.mark.asyncio
async def test_get_lineage_downstream_only(mock_state: Mock) -> None:
    """Test get_lineage with downstream direction only."""
    result = await get_lineage_impl(None, "stg_customers", "model", "downstream", None, mock_state, force_parse=False)

    assert result["resource"]["name"] == "stg_customers"
    assert "upstream" not in result
    assert "downstream" in result
    assert result["stats"]["downstream_count"] >= 1  # customers depends on stg_customers


@pytest.mark.asyncio
async def test_get_lineage_with_depth_limit(mock_state: Mock) -> None:
    """Test get_lineage with depth limit."""
    result = await get_lineage_impl(None, "customers", "model", "upstream", 1, mock_state, force_parse=False)

    assert result["resource"]["name"] == "customers"
    assert "upstream" in result

    # With depth=1, should only get immediate parents
    for node in result["upstream"]:
        assert node["distance"] == 1


@pytest.mark.asyncio
async def test_get_lineage_source(mock_state: Mock) -> None:
    """Test get_lineage for a source."""
    result = await get_lineage_impl(None, "jaffle_shop.customers", "source", "downstream", None, mock_state, force_parse=False)

    assert result["resource"]["resource_type"] == "source"
    assert "downstream" in result


@pytest.mark.asyncio
async def test_get_lineage_auto_detect(mock_state: Mock) -> None:
    """Test get_lineage with auto-detection (no resource_type specified)."""
    result = await get_lineage_impl(None, "stg_customers", None, "both", None, mock_state, force_parse=False)

    # Should find the model
    assert result["resource"]["name"] == "stg_customers"
    assert result["resource"]["resource_type"] == "model"


@pytest.mark.asyncio
async def test_get_lineage_multiple_matches(mock_state: Mock) -> None:
    """Test get_lineage when multiple resources match the name."""
    # "customers" exists as both a model and a source
    result = await get_lineage_impl(None, "customers", None, "both", None, mock_state, force_parse=False)

    # Should return multiple_matches structure
    assert result.get("multiple_matches") is True or result["resource"]["name"] == "customers"


@pytest.mark.asyncio
async def test_get_lineage_invalid_direction(mock_state: Mock) -> None:
    """Test get_lineage with invalid direction raises ValueError."""
    with pytest.raises(ValueError, match="Invalid direction|Lineage error"):
        await get_lineage_impl(None, "customers", "model", "invalid", None, mock_state, force_parse=False)


@pytest.mark.asyncio
async def test_get_lineage_not_found(mock_state: Mock) -> None:
    """Test get_lineage with non-existent resource raises ValueError."""
    with pytest.raises(ValueError, match="not found|Lineage error"):
        await get_lineage_impl(None, "nonexistent_model", None, "both", None, mock_state, force_parse=False)
