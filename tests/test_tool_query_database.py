"""
Tests for query_database tool.
"""

import json
from unittest.mock import AsyncMock, Mock

import pytest

from dbt_core_mcp.tools.query_database import _implementation as query_database_impl  # type: ignore[reportPrivateUsage]


@pytest.fixture
def mock_state() -> Mock:
    """Create mock state for query_database tool testing."""
    state = Mock()
    state.ensure_initialized = AsyncMock()

    def compile_jinja(sql: str) -> str:
        return sql

    state.compile_jinja = AsyncMock(side_effect=compile_jinja)

    # Mock runner with invoke_query method
    mock_runner = Mock()
    mock_runner.invoke_query = AsyncMock()
    state.get_runner = AsyncMock(return_value=mock_runner)

    return state


@pytest.mark.asyncio
async def test_query_database_simple_select(mock_state: Mock) -> None:
    """Test query_database with a simple SELECT query - command construction."""
    # Mock the query execution to return test data in dbt show format
    mock_result = Mock()
    mock_result.success = True
    mock_result.stdout = json.dumps({"show": [{"test_col": 1}]})

    mock_runner = await mock_state.get_runner()
    mock_runner.invoke_query.return_value = mock_result

    result = await query_database_impl(None, "SELECT 1 as test_col", None, "json", mock_state)

    assert result["status"] == "success"
    assert "rows" in result
    assert "row_count" in result
    assert result["row_count"] >= 1


@pytest.mark.asyncio
async def test_query_database_with_ref(mock_state: Mock) -> None:
    """Test query_database with {{ ref() }} Jinja templating."""
    # Mock the query execution to return test data in dbt show format
    mock_result = Mock()
    mock_result.success = True
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

    result = await query_database_impl(None, "SELECT * FROM {{ ref('customers') }} LIMIT 5", None, "json", mock_state)

    assert result["status"] == "success"
    assert "rows" in result
    assert result["row_count"] <= 5


@pytest.mark.asyncio
async def test_query_database_with_source(mock_state: Mock) -> None:
    """Test query_database with {{ source() }} Jinja templating."""
    # Mock the query execution to return test data in dbt show format
    mock_result = Mock()
    mock_result.success = True
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

    result = await query_database_impl(None, "SELECT * FROM {{ source('jaffle_shop', 'customers') }} LIMIT 3", None, "json", mock_state)

    assert result["status"] == "success"
    assert "rows" in result
    assert result["row_count"] <= 3


@pytest.mark.asyncio
async def test_query_database_with_limit_in_sql(mock_state: Mock) -> None:
    """Test query_database with LIMIT clause in SQL."""
    # Mock the query execution to return test data in dbt show format
    mock_result = Mock()
    mock_result.success = True
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

    result = await query_database_impl(None, "SELECT * FROM {{ ref('customers') }} LIMIT 2", None, "json", mock_state)

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
        await query_database_impl(None, "INVALID SQL STATEMENT", None, "json", mock_state)
