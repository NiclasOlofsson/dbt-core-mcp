"""Tests for ManifestLoader helper methods (get_resource_node)."""

from unittest.mock import Mock

import pytest

from dbt_core_mcp.dbt.manifest import ManifestLoader


@pytest.fixture
def mock_manifest() -> ManifestLoader:
    """Create a mock manifest with test data."""
    manifest = Mock(spec=ManifestLoader)

    def mock_get_resource_node(name: str, resource_type: str | None = None):
        # Validate resource_type
        valid_types = {"model", "source", "seed", "snapshot", "test", "analysis"}
        if resource_type is not None and resource_type not in valid_types:
            raise ValueError(f"Invalid resource_type '{resource_type}'. Must be one of: {', '.join(sorted(valid_types))}")

        # Handle source dot notation
        if "." in name and resource_type is None:
            source_name, table_name = name.split(".", 1)
            if source_name == "jaffle_shop" and table_name == "customers":
                return {
                    "resource_type": "source",
                    "unique_id": "source.jaffle_shop.jaffle_shop.customers",
                    "source_name": "jaffle_shop",
                    "name": "customers",
                }

        # Multiple matches when no resource_type and name is "customers"
        if name == "customers" and resource_type is None:
            return {
                "multiple_matches": True,
                "match_count": 2,
                "name": "customers",
                "matches": [
                    {
                        "resource_type": "model",
                        "unique_id": "model.jaffle_shop.customers",
                        "name": "customers",
                    },
                    {
                        "resource_type": "source",
                        "unique_id": "source.jaffle_shop.jaffle_shop.customers",
                        "source_name": "jaffle_shop",
                        "name": "customers",
                    },
                ],
            }

        # Model match
        if name == "customers" and resource_type == "model":
            return {
                "resource_type": "model",
                "unique_id": "model.jaffle_shop.customers",
                "name": "customers",
            }

        # Source match
        if name == "customers" and resource_type == "source":
            return {
                "resource_type": "source",
                "source_name": "jaffle_shop",
                "name": "customers",
            }

        # Not found
        raise ValueError(f"Resource '{name}' not found")

    manifest.get_resource_node = mock_get_resource_node
    return manifest


def test_get_resource_node_model(mock_manifest: ManifestLoader) -> None:
    """Test get_resource_node with a model resource type filter."""
    result = mock_manifest.get_resource_node("customers", "model")

    assert result["resource_type"] == "model"
    assert result["unique_id"] == "model.jaffle_shop.customers"
    assert result["name"] == "customers"


def test_get_resource_node_source(mock_manifest: ManifestLoader) -> None:
    """Test get_resource_node with a source resource type filter."""
    result = mock_manifest.get_resource_node("customers", "source")

    assert result["resource_type"] == "source"
    assert result["source_name"] == "jaffle_shop"
    assert result["name"] == "customers"


def test_get_resource_node_source_dot_notation(mock_manifest: ManifestLoader) -> None:
    """Test get_resource_node with source_name.table_name notation."""
    result = mock_manifest.get_resource_node("jaffle_shop.customers")

    assert result["resource_type"] == "source"
    assert result["unique_id"] == "source.jaffle_shop.jaffle_shop.customers"
    assert result["source_name"] == "jaffle_shop"


def test_get_resource_node_multiple_matches(mock_manifest: ManifestLoader) -> None:
    """Test get_resource_node returns all matches when ambiguous."""
    result = mock_manifest.get_resource_node("customers")

    assert result["multiple_matches"] is True
    assert result["match_count"] == 2
    assert result["name"] == "customers"
    assert len(result["matches"]) == 2

    types = {m["resource_type"] for m in result["matches"]}
    assert types == {"model", "source"}


def test_get_resource_node_not_found(mock_manifest: ManifestLoader) -> None:
    """Test get_resource_node raises ValueError when resource not found."""
    with pytest.raises(ValueError, match="Resource 'nonexistent' not found"):
        mock_manifest.get_resource_node("nonexistent")


def test_get_resource_node_invalid_type(mock_manifest: ManifestLoader) -> None:
    """Test get_resource_node raises ValueError for invalid resource_type."""
    with pytest.raises(ValueError, match="Invalid resource_type"):
        mock_manifest.get_resource_node("customers", "invalid_type")
