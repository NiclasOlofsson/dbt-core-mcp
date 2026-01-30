"""
Tests for query_database tool.
"""

import json
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest

from dbt_core_mcp.tools.query_database import (
    _implementation as query_database_impl,  # type: ignore[reportPrivateUsage]
)
from dbt_core_mcp.tools.query_database import (
    extract_cte_sql,
)


@pytest.fixture
def mock_state() -> Mock:
    """Create mock state for query_database tool testing."""
    state = Mock()
    state.ensure_initialized = AsyncMock()

    def compile_jinja(sql: str) -> str:
        return sql

    state.compile_jinja = AsyncMock(side_effect=compile_jinja)

    # Mock get_project_paths to return default paths
    def get_project_paths() -> dict[str, Any]:
        return {
            "model-paths": ["models"],
            "test-paths": ["tests"],
            "target-path": "target",
        }

    state.get_project_paths = get_project_paths

    # Mock runner with invoke_query method
    mock_runner = Mock()
    mock_runner.invoke_query = AsyncMock()

    # Configure default return value with elapsed_time
    default_result = Mock()
    default_result.elapsed_time = 1.23  # Default elapsed time for tests
    mock_runner.invoke_query.return_value = default_result

    state.get_runner = AsyncMock(return_value=mock_runner)

    return state


@pytest.mark.asyncio
async def test_query_database_simple_select(mock_state: Mock) -> None:
    """Test query_database with a simple SELECT query - command construction."""
    # Mock the query execution to return test data in dbt show format
    mock_result = Mock()
    mock_result.success = True
    mock_result.elapsed_time = 1.23
    mock_result.stdout = json.dumps({"show": [{"test_col": 1}]})

    mock_runner = await mock_state.get_runner()
    mock_runner.invoke_query.return_value = mock_result

    result = await query_database_impl(None, "SELECT 1 as test_col", None, "json", None, None, mock_state)

    assert result["status"] == "success"
    assert "rows" in result
    assert "row_count" in result
    assert result["row_count"] >= 1
    assert "elapsed_time" in result
    assert result["elapsed_time"] == 1.23


@pytest.mark.asyncio
async def test_query_database_with_ref(mock_state: Mock) -> None:
    """Test query_database with {{ ref() }} Jinja templating."""
    # Mock the query execution to return test data in dbt show format
    mock_result = Mock()
    mock_result.success = True
    mock_result.elapsed_time = 1.23
    mock_result.stdout = json.dumps(
        {
            "show": [
                {"customer_id": 1, "first_name": "Alice"},
                {"customer_id": 2, "first_name": "Bob"},
            ]
        }
    )

    mock_runner = await mock_state.get_runner()
    mock_runner.invoke_query.return_value = mock_result

    result = await query_database_impl(None, "SELECT * FROM {{ ref('customers') }} LIMIT 5", None, "json", None, None, mock_state)

    assert result["status"] == "success"
    assert "rows" in result
    assert result["row_count"] <= 5


@pytest.mark.asyncio
async def test_query_database_with_source(mock_state: Mock) -> None:
    """Test query_database with {{ source() }} Jinja templating."""
    # Mock the query execution to return test data in dbt show format
    mock_result = Mock()
    mock_result.success = True
    mock_result.elapsed_time = 1.23
    mock_result.stdout = json.dumps(
        {
            "show": [
                {"id": 1, "name": "Raw Customer 1"},
                {"id": 2, "name": "Raw Customer 2"},
                {"id": 3, "name": "Raw Customer 3"},
            ]
        }
    )

    mock_runner = await mock_state.get_runner()
    mock_runner.invoke_query.return_value = mock_result

    result = await query_database_impl(None, "SELECT * FROM {{ source('jaffle_shop', 'customers') }} LIMIT 3", None, "json", None, None, mock_state)

    assert result["status"] == "success"
    assert "rows" in result
    assert result["row_count"] <= 3


@pytest.mark.asyncio
async def test_query_database_with_limit_in_sql(mock_state: Mock) -> None:
    """Test query_database with LIMIT clause in SQL."""
    # Mock the query execution to return test data in dbt show format
    mock_result = Mock()
    mock_result.success = True
    mock_result.elapsed_time = 1.23
    mock_result.stdout = json.dumps(
        {
            "show": [
                {"customer_id": 1, "first_name": "Alice"},
                {"customer_id": 2, "first_name": "Bob"},
            ]
        }
    )

    mock_runner = await mock_state.get_runner()
    mock_runner.invoke_query.return_value = mock_result

    result = await query_database_impl(None, "SELECT * FROM {{ ref('customers') }} LIMIT 2", None, "json", None, None, mock_state)

    assert result["status"] == "success"
    assert "rows" in result
    assert result["row_count"] <= 2


@pytest.mark.asyncio
async def test_query_database_invalid_sql(mock_state: Mock) -> None:
    """Test query_database with invalid SQL raises RuntimeError."""
    # Mock invoke_query to raise an error (as the real implementation would)
    mock_result = Mock()
    mock_result.success = False
    mock_result.exception = RuntimeError("Parser error at line 1")
    mock_result.stdout = "Database Error: Syntax error"

    mock_runner = await mock_state.get_runner()
    mock_runner.invoke_query.return_value = mock_result

    with pytest.raises(RuntimeError, match="Query execution failed"):
        await query_database_impl(None, "INVALID SQL STATEMENT", None, "json", None, None, mock_state)


@pytest.mark.asyncio
async def test_query_database_cte_requires_model_name(mock_state: Mock) -> None:
    """Test that CTE querying requires model_name parameter."""
    with pytest.raises(ValueError, match="model_name is required"):
        await query_database_impl(
            None,
            "SELECT * FROM cte",
            None,
            "json",
            "customer_agg",  # cte_name provided
            None,  # model_name missing
            mock_state,
        )


@pytest.mark.asyncio
async def test_query_database_cte_extraction(mock_state: Mock, tmp_path: Path) -> None:
    """Test CTE extraction functionality with real file operations."""
    # Create a project directory structure
    project_dir = tmp_path / "test_project"
    models_dir = project_dir / "models"
    models_dir.mkdir(parents=True)

    # Create a test model with CTEs
    model_file = models_dir / "test_model.sql"
    model_file.write_text("""
with customer_orders as (
    select
        customer_id,
        count(*) as order_count
    from {{ ref('orders') }}
    group by customer_id
),

final as (
    select
        customer_id,
        order_count
    from customer_orders
    where order_count > 0
)

select * from final
""")

    # Configure mock state to use the test project directory
    mock_state.project_dir = project_dir

    # Mock the query execution to return test data
    mock_result = Mock()
    mock_result.success = True
    mock_result.elapsed_time = 1.23
    mock_result.stdout = json.dumps(
        {
            "show": [
                {"customer_id": 1, "order_count": 5},
                {"customer_id": 2, "order_count": 3},
            ]
        }
    )

    mock_runner = await mock_state.get_runner()
    mock_runner.invoke_query.return_value = mock_result

    # Call query_database with CTE extraction
    result = await query_database_impl(
        None,
        "SELECT * FROM __cte__ WHERE order_count > 2",
        None,
        "json",
        "customer_orders",  # cte_name
        "test_model",  # model_name
        mock_state,
    )

    # Verify the result
    assert result["status"] == "success"
    assert "rows" in result
    assert result["row_count"] == 2

    # Verify that invoke_query was called
    assert mock_runner.invoke_query.called
    call_args = mock_runner.invoke_query.call_args

    # The SQL should include the extracted CTE and the additional WHERE clause
    # (We can't check the exact SQL since it goes through a temp file,
    # but we can verify the query was executed)
    assert call_args is not None


@pytest.mark.asyncio
async def test_query_database_cte_model_not_found(mock_state: Mock, tmp_path: Path) -> None:
    """Test that CTE querying fails when model file doesn't exist."""
    # Create an empty project directory
    project_dir = tmp_path / "empty_project"
    models_dir = project_dir / "models"
    models_dir.mkdir(parents=True)

    mock_state.project_dir = project_dir

    # Try to query a CTE from a non-existent model
    with pytest.raises(ValueError, match="Model file .* not found"):
        await query_database_impl(
            None,
            "SELECT *",
            None,
            "json",
            "customer_agg",  # cte_name
            "nonexistent_model",  # model_name that doesn't exist
            mock_state,
        )


@pytest.mark.asyncio
async def test_query_database_cte_not_found_in_model(mock_state: Mock, tmp_path: Path) -> None:
    """Test that CTE extraction fails when CTE doesn't exist in model."""
    # Create a project directory structure
    project_dir = tmp_path / "test_project"
    models_dir = project_dir / "models"
    models_dir.mkdir(parents=True)

    # Create a model without the requested CTE
    model_file = models_dir / "test_model.sql"
    model_file.write_text("""
with some_other_cte as (
    select * from {{ ref('orders') }}
)

select * from some_other_cte
""")

    mock_state.project_dir = project_dir

    # Try to extract a CTE that doesn't exist
    with pytest.raises(Exception):  # generate_cte_model returns False, which causes failure
        await query_database_impl(
            None,
            "SELECT *",
            None,
            "json",
            "nonexistent_cte",  # CTE that doesn't exist in the model
            "test_model",
            mock_state,
        )


# ===== Unit Tests for extract_cte_sql =====


def test_extract_cte_sql_basic(tmp_path: Path) -> None:
    """Test basic CTE extraction without additional SQL."""
    # Create project structure
    project_dir = tmp_path / "project"
    models_dir = project_dir / "models"
    models_dir.mkdir(parents=True)

    # Create a model with a simple CTE
    model_file = models_dir / "test_model.sql"
    model_file.write_text("""
with customer_agg as (
    select
        customer_id,
        count(*) as order_count
    from {{ ref('orders') }}
    group by customer_id
)

select * from customer_agg
""")

    # Extract the CTE
    sql = extract_cte_sql(project_dir, "customer_agg", "test_model")

    # Verify the extracted SQL contains the CTE and final select
    assert "with customer_agg as (" in sql.lower()
    assert "select * from customer_agg" in sql.lower()
    assert "{{ ref('orders') }}" in sql
    # Verify sqlfluff comment is removed
    assert "sqlfluff:disable" not in sql


def test_extract_cte_sql_with_additional_sql(tmp_path: Path) -> None:
    """Test CTE extraction with full SELECT replacement."""
    # Create project structure
    project_dir = tmp_path / "project"
    models_dir = project_dir / "models"
    models_dir.mkdir(parents=True)

    # Create a model
    model_file = models_dir / "customers.sql"
    model_file.write_text("""
with customer_orders as (
    select
        customer_id,
        count(*) as order_count
    from {{ ref('orders') }}
    group by customer_id
)

select * from customer_orders
""")

    # Extract with full SELECT
    sql = extract_cte_sql(
        project_dir,
        "customer_orders",
        "customers",
        "SELECT * FROM __cte__ WHERE order_count > 5 LIMIT 10",
    )

    # Verify the SQL has the filtering applied to the final SELECT
    assert "with customer_orders as (" in sql.lower()
    assert "select * from customer_orders where order_count > 5 limit 10" in sql.lower()


def test_extract_cte_sql_rejects_partial_sql(tmp_path: Path) -> None:
    """Test that partial SQL clauses are rejected for CTE extraction."""
    project_dir = tmp_path / "project"
    models_dir = project_dir / "models"
    models_dir.mkdir(parents=True)

    model_file = models_dir / "customers.sql"
    model_file.write_text("""
with customer_orders as (
    select
        customer_id,
        count(*) as order_count
    from {{ ref('orders') }}
    group by customer_id
)

select * from customer_orders
""")

    with pytest.raises(ValueError, match="full SELECT/WITH query"):
        extract_cte_sql(project_dir, "customer_orders", "customers", "WHERE order_count > 5")


def test_extract_cte_sql_with_full_select(tmp_path: Path) -> None:
    """Test CTE extraction with full SELECT replacement for aggregation."""
    # Create project structure
    project_dir = tmp_path / "project"
    models_dir = project_dir / "models"
    models_dir.mkdir(parents=True)

    # Create a model
    model_file = models_dir / "customers.sql"
    model_file.write_text("""
with customer_orders as (
    select
        customer_id,
        count(*) as order_count
    from {{ ref('orders') }}
    group by customer_id
)

select * from customer_orders
""")

    # Extract with a full SELECT using the placeholder
    sql = extract_cte_sql(
        project_dir,
        "customer_orders",
        "customers",
        "SELECT customer_id, COUNT(*) AS order_count FROM __cte__ GROUP BY customer_id",
    )

    assert "with customer_orders as (" in sql.lower()
    assert "select customer_id, count(*) as order_count from customer_orders group by customer_id" in sql.lower()
    assert "__cte__" not in sql


def test_extract_cte_sql_model_not_found(tmp_path: Path) -> None:
    """Test that extraction fails when model doesn't exist."""
    project_dir = tmp_path / "project"
    (project_dir / "models").mkdir(parents=True)

    with pytest.raises(ValueError, match="Model file .* not found"):
        extract_cte_sql(project_dir, "some_cte", "nonexistent_model")


def test_extract_cte_sql_multiple_models_found(tmp_path: Path) -> None:
    """Test that extraction fails when multiple model files match."""
    project_dir = tmp_path / "project"
    models_dir = project_dir / "models"

    # Create multiple model files with same name in different subdirectories
    (models_dir / "staging").mkdir(parents=True)
    (models_dir / "marts").mkdir(parents=True)

    (models_dir / "staging" / "customers.sql").write_text("select 1")
    (models_dir / "marts" / "customers.sql").write_text("select 2")

    with pytest.raises(ValueError, match="Multiple model files found"):
        extract_cte_sql(project_dir, "some_cte", "customers")


def test_extract_cte_sql_cte_not_found(tmp_path: Path) -> None:
    """Test that extraction fails when CTE doesn't exist in model."""
    project_dir = tmp_path / "project"
    models_dir = project_dir / "models"
    models_dir.mkdir(parents=True)

    # Create a model without the requested CTE
    model_file = models_dir / "test_model.sql"
    model_file.write_text("""
with different_cte as (
    select * from {{ ref('orders') }}
)

select * from different_cte
""")

    with pytest.raises(ValueError, match="Failed to extract CTE"):
        extract_cte_sql(project_dir, "nonexistent_cte", "test_model")


def test_extract_cte_sql_with_upstream_ctes(tmp_path: Path) -> None:
    """Test that CTE extraction includes all upstream dependencies."""
    project_dir = tmp_path / "project"
    models_dir = project_dir / "models"
    models_dir.mkdir(parents=True)

    # Create a model with chained CTEs
    model_file = models_dir / "customers.sql"
    model_file.write_text("""
with orders as (
    select * from {{ ref('stg_orders') }}
),

customer_agg as (
    select
        customer_id,
        count(*) as order_count
    from orders
    group by customer_id
)

select * from customer_agg
""")

    # Extract the downstream CTE
    sql = extract_cte_sql(project_dir, "customer_agg", "customers")

    # Verify both CTEs are included
    assert "with orders as (" in sql.lower()
    assert "customer_agg as (" in sql.lower()
    assert "from orders" in sql.lower()
    assert "{{ ref('stg_orders') }}" in sql


@pytest.mark.asyncio
async def test_query_database_relative_path_normalization(mock_state: Mock, tmp_path: Path) -> None:
    """Test that relative output_file paths are normalized to workspace root."""
    # Set up mock state with a project directory
    mock_state.project_dir = tmp_path

    # Mock the query execution to return test data in dbt show format
    mock_result = Mock()
    mock_result.success = True
    mock_result.elapsed_time = 1.23
    mock_result.stdout = json.dumps({"show": [{"test_col": 1}, {"test_col": 2}]})

    mock_runner = await mock_state.get_runner()
    mock_runner.invoke_query.return_value = mock_result

    # Use a relative path
    relative_path = "temp_auto/test_output.json"

    result = await query_database_impl(None, "SELECT 1 as test_col", relative_path, "json", None, None, mock_state)

    assert result["status"] == "success"
    # Check that the file was saved with an absolute path relative to workspace root
    saved_path = Path(result["saved_to"])
    assert saved_path.is_absolute()
    assert saved_path.parent.parent == tmp_path  # temp_auto/test_output.json -> parent.parent should be workspace root
    assert saved_path.name == "test_output.json"
    assert saved_path.parent.name == "temp_auto"


@pytest.mark.asyncio
async def test_query_database_absolute_path_unchanged(mock_state: Mock, tmp_path: Path) -> None:
    """Test that absolute output_file paths are used as-is."""
    # Set up mock state with a project directory
    mock_state.project_dir = tmp_path / "workspace"

    # Mock the query execution to return test data in dbt show format
    mock_result = Mock()
    mock_result.success = True
    mock_result.elapsed_time = 1.23
    mock_result.stdout = json.dumps({"show": [{"test_col": 1}, {"test_col": 2}]})

    mock_runner = await mock_state.get_runner()
    mock_runner.invoke_query.return_value = mock_result

    # Use an absolute path
    absolute_path = tmp_path / "other_location" / "test_output.json"

    result = await query_database_impl(None, "SELECT 1 as test_col", str(absolute_path), "json", None, None, mock_state)

    assert result["status"] == "success"
    # Check that the file was saved at the exact absolute path provided
    saved_path = Path(result["saved_to"])
    assert saved_path == absolute_path
    assert saved_path.is_absolute()
