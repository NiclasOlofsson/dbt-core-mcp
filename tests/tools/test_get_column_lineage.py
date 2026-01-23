"""Tests for get_column_lineage tool - refactored with testable helpers."""

from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest

from dbt_core_mcp.context import DbtCoreServerContext
from dbt_core_mcp.tools.get_column_lineage import (
    _build_schema_mapping,  # pyright: ignore[reportPrivateUsage]
    _extract_dependencies_from_lineage,  # pyright: ignore[reportPrivateUsage]
    _format_lineage_response,  # pyright: ignore[reportPrivateUsage]
    implementation,
)


@pytest.fixture
def mock_state() -> Mock:
    """Create a mock server state for column lineage testing."""
    state = Mock(spec=DbtCoreServerContext)
    state.ensure_initialized = AsyncMock()
    state.manifest = Mock()
    return state


# ========== Helper Function Tests (Pure Functions) ==========


def test_build_schema_mapping_with_upstream_models() -> None:
    """Test schema mapping construction with mocked manifest."""
    # Mock manifest that returns resource info
    mock_manifest = Mock()

    def mock_get_resource_info(unique_id: str, **kwargs: Any) -> dict[str, Any]:
        if "stg_customers" in unique_id:
            return {
                "database": "main",
                "schema": "main",
                "name": "stg_customers",
                "alias": None,
                "database_columns": {
                    "customer_id": {"type": "INTEGER"},
                    "first_name": {"type": "VARCHAR"},
                },
            }
        elif "stg_orders" in unique_id:
            return {
                "database": "main",
                "schema": "main",
                "name": "stg_orders",
                "alias": None,
                "database_columns": {
                    "order_id": {"type": "INTEGER"},
                    "customer_id": {"type": "INTEGER"},
                },
            }
        return {}

    mock_manifest.get_resource_info = mock_get_resource_info

    upstream_lineage = {
        "upstream": [
            {"unique_id": "model.jaffle_shop.stg_customers"},
            {"unique_id": "model.jaffle_shop.stg_orders"},
        ]
    }

    # Execute
    schema_mapping = _build_schema_mapping(mock_manifest, upstream_lineage)

    # Assert: Verify nested structure
    assert "main" in schema_mapping
    assert "main" in schema_mapping["main"]
    assert "stg_customers" in schema_mapping["main"]["main"]
    assert "stg_orders" in schema_mapping["main"]["main"]

    # Verify column types (should be lowercase)
    stg_customers_cols = schema_mapping["main"]["main"]["stg_customers"]
    assert stg_customers_cols["customer_id"] == "integer"
    assert stg_customers_cols["first_name"] == "varchar"

    stg_orders_cols = schema_mapping["main"]["main"]["stg_orders"]
    assert stg_orders_cols["customer_id"] == "integer"


def test_build_schema_mapping_empty_upstream() -> None:
    """Test schema mapping with no upstream models."""
    mock_manifest = Mock()
    empty_lineage = {"upstream": []}

    result = _build_schema_mapping(mock_manifest, empty_lineage)

    assert result == {}


def test_extract_dependencies_from_lineage() -> None:
    """Test dependency extraction from mocked sqlglot lineage node."""
    # Mock a simple lineage node structure
    mock_dep = Mock()
    mock_dep.source = Mock()
    mock_dep.source.this = "raw_customers"
    mock_dep.source.catalog = None
    mock_dep.source.db = Mock()
    mock_dep.source.db.__str__ = Mock(return_value="my_schema")
    mock_dep.name = "id"

    mock_lineage_node = Mock()
    mock_lineage_node.walk = Mock(return_value=[mock_dep])

    # Execute without manifest
    result = _extract_dependencies_from_lineage(mock_lineage_node, None, None)

    # Validate
    assert len(result) == 1
    assert result[0]["column"] == "id"
    assert result[0]["table"] == "raw_customers"
    assert result[0]["schema"] == "my_schema"


def test_extract_dependencies_respects_depth() -> None:
    """Test that depth parameter limits extraction."""
    # This would need more complex mocking of nested dependencies
    # For now, test that function accepts depth parameter
    mock_lineage_node = Mock()
    mock_lineage_node.walk = Mock(return_value=[])

    result = _extract_dependencies_from_lineage(mock_lineage_node, None, depth=1)

    assert isinstance(result, list)


def test_format_lineage_response_basic() -> None:
    """Test response formatting with basic inputs."""
    dependencies = [
        {"column": "id", "table": "raw_customers"},
        {"column": "order_id", "table": "raw_orders"},
    ]

    result = _format_lineage_response("customers", "customer_id", "upstream", dependencies)

    assert result["model"] == "customers"
    assert result["column"] == "customer_id"
    assert result["direction"] == "upstream"
    assert result["dependencies"] == dependencies
    assert result["dependency_count"] == 2
    assert "downstream_usage" not in result


def test_format_lineage_response_with_downstream() -> None:
    """Test response formatting with downstream usage."""
    dependencies = [{"column": "id", "table": "raw_customers"}]
    downstream_usage = [{"model": "orders", "unique_id": "model.test.orders", "references_column": True}]

    result = _format_lineage_response("customers", "customer_id", "both", dependencies, downstream_usage)

    assert result["downstream_usage"] == downstream_usage
    assert result["downstream_count"] == 1


# ========== Integration Tests (Error Paths) ==========


@pytest.mark.asyncio
async def test_model_not_found_error(mock_state: Mock) -> None:
    """Test error handling when model is not found."""
    # Setup: manifest returns error for unknown model
    mock_state.manifest.get_resource_info.side_effect = ValueError("Model 'nonexistent' not found")

    # Execute & Assert
    with pytest.raises(ValueError, match="Column lineage error|not found"):
        await implementation(
            ctx=None,
            model_name="nonexistent_model",
            column_name="some_column",
            direction="upstream",
            depth=None,
            state=mock_state,
            force_parse=False,
        )

    # Verify manifest was called
    mock_state.manifest.get_resource_info.assert_called_once()


@pytest.mark.asyncio
async def test_no_compiled_sql_error(mock_state: Mock) -> None:
    """Test error when model has no compiled SQL and compilation fails."""
    # Setup: model exists but no compiled SQL, and compilation fails
    mock_state.manifest.get_resource_info.return_value = {
        "unique_id": "model.test.customers",
        "name": "customers",
        "compiled_sql": None,
        "compiled_sql_cached": False,  # Not cached, will try to compile
    }

    # Mock runner that fails compilation
    mock_runner = Mock()
    mock_result = Mock()
    mock_result.success = False
    mock_runner.invoke_compile = AsyncMock(return_value=mock_result)
    mock_state.get_runner = AsyncMock(return_value=mock_runner)

    # Execute & Assert
    with pytest.raises(ValueError, match="Failed to compile model"):
        await implementation(
            ctx=None,
            model_name="customers",
            column_name="customer_id",
            direction="upstream",
            depth=None,
            state=mock_state,
            force_parse=False,
        )


@pytest.mark.asyncio
async def test_multiple_matches_error(mock_state: Mock) -> None:
    """Test error when multiple models match the name."""
    # Setup: multiple matches returned
    mock_state.manifest.get_resource_info.return_value = {
        "multiple_matches": True,
        "matches": [
            {"unique_id": "model.project1.customers"},
            {"unique_id": "model.project2.customers"},
        ],
    }

    # Execute & Assert
    with pytest.raises(ValueError, match="Multiple models found"):
        await implementation(
            ctx=None,
            model_name="customers",
            column_name="customer_id",
            direction="upstream",
            depth=None,
            state=mock_state,
            force_parse=False,
        )


@pytest.mark.asyncio
async def test_manifest_not_initialized_error(mock_state: Mock) -> None:
    """Test error when manifest is not initialized."""
    # Setup: manifest is None
    mock_state.manifest = None

    # Execute & Assert
    with pytest.raises(RuntimeError, match="Manifest not initialized"):
        await implementation(
            ctx=None,
            model_name="customers",
            column_name="customer_id",
            direction="upstream",
            depth=None,
            state=mock_state,
            force_parse=False,
        )


@pytest.mark.asyncio
async def test_downstream_not_implemented_error(mock_state: Mock) -> None:
    """Test that downstream direction raises NotImplementedError."""
    # Execute & Assert
    with pytest.raises(NotImplementedError, match="Downstream column lineage is not yet implemented"):
        await implementation(
            ctx=None,
            model_name="customers",
            column_name="customer_id",
            direction="downstream",
            depth=None,
            state=mock_state,
            force_parse=False,
        )


@pytest.mark.asyncio
async def test_both_direction_not_implemented_error(mock_state: Mock) -> None:
    """Test that 'both' direction raises NotImplementedError."""
    # Execute & Assert
    with pytest.raises(NotImplementedError, match="Downstream column lineage is not yet implemented"):
        await implementation(
            ctx=None,
            model_name="customers",
            column_name="customer_id",
            direction="both",
            depth=None,
            state=mock_state,
            force_parse=False,
        )
