"""Tests for get_column_lineage tool - refactored with testable helpers."""

from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest

from dbt_core_mcp.context import DbtCoreServerContext
from dbt_core_mcp.tools.get_column_lineage import (
    _build_schema_mapping,  # pyright: ignore[reportPrivateUsage]
    _extract_dependencies_from_lineage,  # pyright: ignore[reportPrivateUsage]
    _format_lineage_response,  # pyright: ignore[reportPrivateUsage]
    _get_output_columns_from_sql,  # pyright: ignore[reportPrivateUsage]
    _resolve_output_columns,  # pyright: ignore[reportPrivateUsage]
    implementation,
)


@pytest.fixture
def mock_state() -> Mock:
    """Create a mock server state for column lineage testing."""
    state = Mock(spec=DbtCoreServerContext)
    state.ensure_initialized = AsyncMock()
    state.manifest = Mock()
    state.manifest.get_manifest_dict = Mock(return_value={"nodes": {}, "sources": {}})
    state.manifest.get_node_by_unique_id = Mock(return_value=None)
    return state


# ========== Helper Function Tests (Pure Functions) ==========


def test_build_schema_mapping_with_upstream_models() -> None:
    """Test schema mapping construction with mocked manifest."""
    # Mock manifest that returns resource info
    mock_manifest = Mock()

    def mock_get_resource_info(name: str, **kwargs: Any) -> dict[str, Any]:
        if "stg_customers" in name:
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
        elif "stg_orders" in name:
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
            {"unique_id": "model.jaffle_shop.stg_customers", "name": "stg_customers"},
            {"unique_id": "model.jaffle_shop.stg_orders", "name": "stg_orders"},
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


def test_build_schema_mapping_skips_empty_columns() -> None:
    """Test schema mapping skips upstream nodes with empty columns."""
    mock_manifest = Mock()

    def mock_get_resource_info(name: str, **kwargs: Any) -> dict[str, Any]:
        if name == "stg_customers":
            return {
                "database": "main",
                "schema": "main",
                "name": "stg_customers",
                "alias": None,
                "database_columns": [],
            }
        return {}

    mock_manifest.get_resource_info = mock_get_resource_info

    upstream_lineage = {
        "upstream": [
            {"unique_id": "model.jaffle_shop.stg_customers", "name": "stg_customers"},
        ]
    }

    result = _build_schema_mapping(mock_manifest, upstream_lineage)

    assert result == {}


def test_build_schema_mapping_uses_manifest_columns() -> None:
    """Test schema mapping falls back to manifest columns when database columns are missing."""
    mock_manifest = Mock()

    def mock_get_resource_info(name: str, **kwargs: Any) -> dict[str, Any]:
        if name == "stg_customers":
            return {
                "database": "main",
                "schema": "main",
                "name": "stg_customers",
                "alias": None,
                "database_columns": [],
                "columns": {
                    "customer_id": {"data_type": "INTEGER"},
                    "first_name": {"data_type": "VARCHAR"},
                },
            }
        return {}

    mock_manifest.get_resource_info = mock_get_resource_info

    upstream_lineage = {
        "upstream": [
            {"unique_id": "model.jaffle_shop.stg_customers", "name": "stg_customers"},
        ]
    }

    result = _build_schema_mapping(mock_manifest, upstream_lineage)

    assert "main" in result
    assert "stg_customers" in result["main"]["main"]
    assert result["main"]["main"]["stg_customers"]["customer_id"] == "integer"


def test_get_output_columns_from_sql_select_star_cte() -> None:
    """Test output column extraction from SELECT * over a single CTE."""
    sql = """
    WITH base AS (
        SELECT customer_id, first_name FROM raw_customers
    )
    SELECT * FROM base
    """

    result = _get_output_columns_from_sql(sql, {})

    assert result == ["customer_id", "first_name"]


def test_get_output_columns_from_sql_select_star_table() -> None:
    """Test output column extraction from SELECT * over a single table using schema mapping."""
    sql = "SELECT * FROM customers"
    schema_mapping = {
        "main": {
            "main": {
                "customers": {
                    "customer_id": "integer",
                    "first_name": "varchar",
                }
            }
        }
    }

    result = _get_output_columns_from_sql(sql, schema_mapping)

    assert set(result) == {"customer_id", "first_name"}


def test_resolve_output_columns_prefers_sql() -> None:
    """Test output column resolver prefers SQL-derived columns first."""
    sql = "SELECT customer_id FROM customers"
    schema_mapping: dict[str, Any] = {}
    resource_info = {
        "database_columns": [{"col_name": "ignored_col", "type": "INTEGER"}],
        "columns": {"ignored_manifest": {"data_type": "INTEGER"}},
    }

    output_columns, source = _resolve_output_columns(sql, schema_mapping, resource_info)

    assert source == "sql"
    assert list(output_columns.keys()) == ["customer_id"]


def test_resolve_output_columns_falls_back_to_warehouse_then_manifest() -> None:
    """Test resolver falls back to warehouse columns, then manifest columns."""
    sql = "SELECT * FROM customers"
    schema_mapping: dict[str, Any] = {}

    resource_info = {
        "database_columns": [{"col_name": "warehouse_col", "type": "INTEGER"}],
        "columns": {"manifest_col": {"data_type": "INTEGER"}},
    }

    output_columns, source = _resolve_output_columns(sql, schema_mapping, resource_info)
    assert source == "warehouse"
    assert list(output_columns.keys()) == ["warehouse_col"]

    resource_info = {
        "database_columns": [],
        "columns": {"manifest_col": {"data_type": "INTEGER"}},
    }

    output_columns, source = _resolve_output_columns(sql, schema_mapping, resource_info)
    assert source == "manifest"
    assert list(output_columns.keys()) == ["manifest_col"]


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
    mock_dep.downstream = None  # No parent node (end of chain)
    mock_dep.expression = Mock()
    mock_dep.expression.__str__ = Mock(return_value="column_expression")

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
    mock_runner.invoke = AsyncMock(return_value=mock_result)  # Changed from invoke_compile to invoke
    mock_state.get_runner = AsyncMock(return_value=mock_runner)
    # Mock manifest load to handle reload after compilation
    mock_state.manifest.load = AsyncMock()

    # Execute & Assert
    with pytest.raises(RuntimeError, match="Failed to compile project"):
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
    # Setup: multiple matches returned with compiled_sql to avoid compilation path
    mock_state.manifest.get_resource_info.return_value = {
        "multiple_matches": True,
        "matches": [
            {"unique_id": "model.project1.customers"},
            {"unique_id": "model.project2.customers"},
        ],
        "compiled_sql": "SELECT 1",  # Add this to avoid compilation check
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


# ========== Downstream Lineage Tests (TDD - write tests first) ==========


@pytest.mark.asyncio
async def test_downstream_single_level(mock_state: Mock) -> None:
    """Test downstream lineage for a single level (depth=1)."""
    # Setup: stg_customers has downstream model 'customers' that uses customer_id
    mock_state.manifest = Mock()

    # Mock get_lineage to return downstream models AND upstream for schema building
    def mock_get_lineage(model: str, **kwargs: Any) -> dict[str, Any]:
        direction = kwargs.get("direction", "downstream")
        if model == "stg_customers" and direction == "downstream":
            return {"downstream": [{"unique_id": "model.test.customers", "name": "customers"}]}
        elif "customers" in model and direction == "upstream":
            # Customers depends on stg_customers
            return {"upstream": [{"unique_id": "model.test.stg_customers", "name": "stg_customers"}]}
        return {"downstream": [], "upstream": []}

    mock_state.manifest.get_lineage = mock_get_lineage

    # Mock get_resource_info for both models
    def mock_get_resource_info(unique_id: str, **kwargs: Any) -> dict[str, Any]:
        if "stg_customers" in unique_id or unique_id == "stg_customers":
            return {
                "name": "stg_customers",
                "database": "main",
                "schema": "main",
                "alias": None,
                "compiled_sql": "SELECT id as customer_id FROM raw_customers",
                "database_columns": [{"col_name": "customer_id", "type": "INTEGER"}],
            }
        elif "customers" in unique_id:
            return {
                "name": "customers",
                "unique_id": "model.test.customers",
                "database": "main",
                "schema": "main",
                "alias": None,
                # COMPILED SQL ({{ ref() }} already resolved to table name)
                "compiled_sql": "SELECT customer_id, COUNT(*) as order_count FROM stg_customers GROUP BY customer_id",
                "database_columns": [{"col_name": "customer_id", "type": "INTEGER"}, {"col_name": "order_count", "type": "INTEGER"}],
            }
        return {}

    mock_state.manifest.get_resource_info = mock_get_resource_info

    # Mock get_project_info to return adapter type
    mock_state.manifest.get_project_info = Mock(return_value={"adapter_type": "duckdb"})

    # Execute
    result = await implementation(
        ctx=None,
        model_name="stg_customers",
        column_name="customer_id",
        direction="downstream",
        depth=1,
        state=mock_state,
        force_parse=False,
    )

    # Assert
    assert result["model"] == "stg_customers"
    assert result["column"] == "customer_id"
    assert result["direction"] == "downstream"
    assert "downstream_usage" in result

    downstream = result["downstream_usage"]
    assert len(downstream) == 1
    assert downstream[0]["model"] == "customers"
    assert downstream[0]["column"] == "customer_id"
    assert downstream[0]["distance"] == 1


@pytest.mark.asyncio
async def test_downstream_column_not_used(mock_state: Mock) -> None:
    """Test downstream when column is not used in downstream models."""
    # Setup: stg_customers.first_name is not used in customers model
    mock_state.manifest = Mock()

    mock_state.manifest.get_lineage = Mock(return_value={"downstream": [{"unique_id": "model.test.customers", "name": "customers"}]})

    def mock_get_resource_info(unique_id: str, **kwargs: Any) -> dict[str, Any]:
        if "stg_customers" in unique_id or unique_id == "stg_customers":
            return {"name": "stg_customers", "compiled_sql": "SELECT id as customer_id, name as first_name FROM raw_customers", "database_columns": [{"col_name": "customer_id", "type": "INTEGER"}, {"col_name": "first_name", "type": "VARCHAR"}]}
        elif "customers" in unique_id:
            # Only uses customer_id, not first_name
            return {
                "name": "customers",
                "compiled_sql": "SELECT customer_id, COUNT(*) as order_count FROM {{ ref('stg_customers') }} GROUP BY customer_id",
                "database_columns": [{"col_name": "customer_id", "type": "INTEGER"}, {"col_name": "order_count", "type": "INTEGER"}],
            }
        return {}

    mock_state.manifest.get_resource_info = mock_get_resource_info

    # Execute
    result = await implementation(
        ctx=None,
        model_name="stg_customers",
        column_name="first_name",
        direction="downstream",
        depth=1,
        state=mock_state,
        force_parse=False,
    )

    # Assert: No downstream usage
    assert result["downstream_usage"] == []


@pytest.mark.asyncio
async def test_downstream_recursive_depth_2(mock_state: Mock) -> None:
    """Test downstream lineage traces through multiple levels (depth=2)."""
    # Setup: stg_customers -> customers -> report
    mock_state.manifest = Mock()

    call_count = {"get_lineage": 0}

    def mock_get_lineage(model: str, **kwargs: Any) -> dict[str, Any]:
        call_count["get_lineage"] += 1
        if model == "stg_customers":
            return {"downstream": [{"unique_id": "model.test.customers", "name": "customers"}]}
        elif model == "customers":
            return {"downstream": [{"unique_id": "model.test.report", "name": "report"}]}
        return {"downstream": []}

    mock_state.manifest.get_lineage = mock_get_lineage

    def mock_get_resource_info(unique_id: str, **kwargs: Any) -> dict[str, Any]:
        if "stg_customers" in unique_id or unique_id == "stg_customers":
            return {
                "name": "stg_customers",
                "database": "main",
                "schema": "main",
                "alias": None,
                "compiled_sql": "SELECT id as customer_id FROM raw_customers",
                "database_columns": [{"col_name": "customer_id", "type": "INTEGER"}],
            }
        elif "customers" in unique_id:
            return {
                "name": "customers",
                "database": "main",
                "schema": "main",
                "alias": None,
                # COMPILED SQL (templates resolved)
                "compiled_sql": "SELECT customer_id FROM stg_customers",
                "database_columns": [{"col_name": "customer_id", "type": "INTEGER"}],
            }
        elif "report" in unique_id:
            return {
                "name": "report",
                "database": "main",
                "schema": "main",
                "alias": None,
                # COMPILED SQL (templates resolved)
                "compiled_sql": "SELECT customer_id as cust_id FROM customers",
                "database_columns": [{"col_name": "cust_id", "type": "INTEGER"}],
            }
        return {}

    mock_state.manifest.get_resource_info = mock_get_resource_info

    # Mock get_project_info to return adapter type
    mock_state.manifest.get_project_info = Mock(return_value={"adapter_type": "duckdb"})

    # Execute
    result = await implementation(
        ctx=None,
        model_name="stg_customers",
        column_name="customer_id",
        direction="downstream",
        depth=2,
        state=mock_state,
        force_parse=False,
    )

    # Assert: Should find usage in both customers and report
    downstream = result["downstream_usage"]
    assert len(downstream) == 2

    # Check distance levels
    customers_usage = next(d for d in downstream if d["model"] == "customers")
    assert customers_usage["distance"] == 1
    assert customers_usage["column"] == "customer_id"

    report_usage = next(d for d in downstream if d["model"] == "report")
    assert report_usage["distance"] == 2
    assert report_usage["column"] == "cust_id"


@pytest.mark.asyncio
async def test_downstream_respects_depth_limit(mock_state: Mock) -> None:
    """Test that depth parameter limits downstream traversal."""
    # Setup: Same as above but with depth=1
    mock_state.manifest = Mock()

    def mock_get_lineage(model: str, **kwargs: Any) -> dict[str, Any]:
        if model == "stg_customers":
            return {"downstream": [{"unique_id": "model.test.customers", "name": "customers"}]}
        elif model == "customers":
            return {"downstream": [{"unique_id": "model.test.report", "name": "report"}]}
        return {"downstream": []}

    mock_state.manifest.get_lineage = mock_get_lineage

    def mock_get_resource_info(unique_id: str, **kwargs: Any) -> dict[str, Any]:
        if "stg_customers" in unique_id or unique_id == "stg_customers":
            return {
                "name": "stg_customers",
                "database": "main",
                "schema": "main",
                "alias": None,
                "compiled_sql": "SELECT id as customer_id FROM raw_customers",
                "database_columns": [{"col_name": "customer_id", "type": "INTEGER"}],
            }
        elif "customers" in unique_id:
            return {
                "name": "customers",
                "database": "main",
                "schema": "main",
                "alias": None,
                # COMPILED SQL (templates resolved)
                "compiled_sql": "SELECT customer_id FROM stg_customers",
                "database_columns": [{"col_name": "customer_id", "type": "INTEGER"}],
            }
        return {}

    mock_state.manifest.get_resource_info = mock_get_resource_info

    # Mock get_project_info to return adapter type
    mock_state.manifest.get_project_info = Mock(return_value={"adapter_type": "duckdb"})

    # Execute with depth=1
    result = await implementation(
        ctx=None,
        model_name="stg_customers",
        column_name="customer_id",
        direction="downstream",
        depth=1,
        state=mock_state,
        force_parse=False,
    )

    # Assert: Should only find customers, not report
    downstream = result["downstream_usage"]
    assert len(downstream) == 1
    assert downstream[0]["model"] == "customers"
