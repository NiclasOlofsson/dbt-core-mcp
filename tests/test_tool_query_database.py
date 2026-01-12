"""
Tests for query_database tool.
"""

from typing import TYPE_CHECKING

import pytest

from dbt_core_mcp.tools.query_database import _implementation as query_database_impl

if TYPE_CHECKING:
    from dbt_core_mcp.server import DbtCoreMcpServer


@pytest.mark.asyncio
async def test_query_database_simple_select(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test query_database with a simple SELECT query."""
    result = await query_database_impl(None, "SELECT 1 as test_col", None, "json", jaffle_shop_server.state)

    assert result["status"] == "success"
    assert "rows" in result
    assert "row_count" in result
    assert result["row_count"] >= 1


@pytest.mark.asyncio
async def test_query_database_with_ref(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test query_database with {{ ref() }} Jinja templating."""
    result = await query_database_impl(None, "SELECT * FROM {{ ref('customers') }} LIMIT 5", None, "json", jaffle_shop_server.state)

    assert result["status"] == "success"
    assert "rows" in result
    assert result["row_count"] <= 5


@pytest.mark.asyncio
async def test_query_database_with_source(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test query_database with {{ source() }} Jinja templating."""
    result = await query_database_impl(None, "SELECT * FROM {{ source('jaffle_shop', 'customers') }} LIMIT 3", None, "json", jaffle_shop_server.state)

    assert result["status"] == "success"
    assert "rows" in result
    assert result["row_count"] <= 3


@pytest.mark.asyncio
async def test_query_database_with_limit_in_sql(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test query_database with LIMIT clause in SQL."""
    result = await query_database_impl(None, "SELECT * FROM {{ ref('customers') }} LIMIT 2", None, "json", jaffle_shop_server.state)

    assert result["status"] == "success"
    assert "rows" in result
    assert result["row_count"] <= 2


@pytest.mark.asyncio
async def test_query_database_invalid_sql(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test query_database with invalid SQL raises RuntimeError."""
    with pytest.raises(RuntimeError, match="Query execution failed"):
        await query_database_impl(None, "INVALID SQL STATEMENT", None, "json", jaffle_shop_server.state)
