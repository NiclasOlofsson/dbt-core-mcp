"""
Tests for query_database tool.
"""

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from dbt_core_mcp.server import DbtCoreMcpServer


@pytest.mark.asyncio
async def test_query_database_simple_select(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test query_database with a simple SELECT query."""
    result = await jaffle_shop_server.toolImpl_query_database(ctx=None, sql="SELECT 1 as test_col")

    assert result["status"] == "success"
    assert "rows" in result
    assert "row_count" in result
    assert result["row_count"] >= 1


@pytest.mark.asyncio
async def test_query_database_with_ref(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test query_database with {{ ref() }} Jinja templating."""
    result = await jaffle_shop_server.toolImpl_query_database(ctx=None, sql="SELECT * FROM {{ ref('customers') }} LIMIT 5")

    assert result["status"] == "success"
    assert "rows" in result
    assert result["row_count"] <= 5


@pytest.mark.asyncio
async def test_query_database_with_source(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test query_database with {{ source() }} Jinja templating."""
    result = await jaffle_shop_server.toolImpl_query_database(ctx=None, sql="SELECT * FROM {{ source('jaffle_shop', 'customers') }} LIMIT 3")

    assert result["status"] == "success"
    assert "rows" in result
    assert result["row_count"] <= 3


@pytest.mark.asyncio
async def test_query_database_with_limit_in_sql(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test query_database with LIMIT clause in SQL."""
    result = await jaffle_shop_server.toolImpl_query_database(ctx=None, sql="SELECT * FROM {{ ref('customers') }} LIMIT 2")

    assert result["status"] == "success"
    assert "rows" in result
    assert result["row_count"] <= 2


@pytest.mark.asyncio
async def test_query_database_invalid_sql(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test query_database with invalid SQL returns error."""
    result = await jaffle_shop_server.toolImpl_query_database(ctx=None, sql="INVALID SQL STATEMENT")

    assert result["status"] in ["failed", "error"]
    assert "error" in result or "message" in result
