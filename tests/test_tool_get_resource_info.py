"""
Tests for toolImpl_get_resource_info.
"""

from typing import TYPE_CHECKING

import pytest
if TYPE_CHECKING:
    from dbt_core_mcp.server import DbtCoreMcpServer


@pytest.mark.asyncio
async def test_get_resource_info_with_compiled_sql(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test get_resource_info tool includes compiled SQL and triggers compilation if needed."""
    # Call the actual tool implementation (not just manifest method)
    result = await jaffle_shop_server.toolImpl_get_resource_info(name="customers", resource_type="model", include_compiled_sql=True)

    assert result["name"] == "customers"
    assert result["resource_type"] == "model"

    # Verify compilation was triggered and SQL is now available
    assert result["compiled_sql"] is not None, "Expected compiled SQL to be present"
    assert result["compiled_sql_cached"] is True, "Expected compiled SQL to be cached after compilation"

    # Verify it's actually compiled (no Jinja templates)
    assert "{{" not in result["compiled_sql"], "Expected no Jinja templates in compiled SQL"
    assert "jaffle_shop" in result["compiled_sql"] or "main" in result["compiled_sql"], "Expected schema reference in compiled SQL"


@pytest.mark.asyncio
async def test_get_resource_info_skip_compiled_sql(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test get_resource_info tool can skip compiled SQL with include_compiled_sql=False."""
    result = await jaffle_shop_server.toolImpl_get_resource_info(name="customers", resource_type="model", include_compiled_sql=False)

    assert result["name"] == "customers"
    assert result["resource_type"] == "model"
    assert "compiled_sql" not in result


@pytest.mark.asyncio
async def test_get_resource_info_compiled_sql_only_for_models(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test get_resource_info tool only includes compiled SQL for models, not sources/seeds."""
    # Test with source - should not have compiled_sql even if requested
    source_result = await jaffle_shop_server.toolImpl_get_resource_info(name="jaffle_shop.customers", resource_type="source", include_compiled_sql=True)
    assert source_result["resource_type"] == "source"
    assert "compiled_sql" not in source_result

    # Test with seed - should not have compiled_sql even if requested
    seed_result = await jaffle_shop_server.toolImpl_get_resource_info(name="raw_customers", resource_type="seed", include_compiled_sql=True)
    assert seed_result["resource_type"] == "seed"
    assert "compiled_sql" not in seed_result


@pytest.mark.asyncio
async def test_get_resource_info_uses_cached_compilation(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test that get_resource_info doesn't recompile when compiled SQL is already cached."""
    # First call - triggers compilation (manifest lacks compiled_code initially)
    result1 = await jaffle_shop_server.toolImpl_get_resource_info(name="customers", resource_type="model", include_compiled_sql=True)

    assert result1["compiled_sql"] is not None, "First call should return compiled SQL"
    assert result1["compiled_sql_cached"] is True, "First call should cache compiled SQL after compilation"
    compiled_sql_1 = result1["compiled_sql"]

    # Second call - should use cached compilation (no recompilation needed)
    result2 = await jaffle_shop_server.toolImpl_get_resource_info(name="customers", resource_type="model", include_compiled_sql=True)

    assert result2["compiled_sql"] is not None, "Second call should return compiled SQL"
    assert result2["compiled_sql_cached"] is True, "Second call should indicate SQL is cached"
    assert result2["compiled_sql"] == compiled_sql_1, "Second call should return identical SQL (cached, not recompiled)"


@pytest.mark.asyncio
async def test_get_resource_info_includes_database_schema_for_sources(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test get_resource_info includes database_columns for sources when include_database_schema=True."""
    result = await jaffle_shop_server.toolImpl_get_resource_info(
        name="jaffle_shop.customers",
        resource_type="source",
        include_database_schema=True,
    )

    assert result["resource_type"] == "source"
    assert result["source_name"] == "jaffle_shop"
    assert result["name"] == "customers"

    # Verify database_columns is present
    assert "database_columns" in result, "Expected database_columns to be present for source"
    assert isinstance(result["database_columns"], list), "Expected database_columns to be a list"
    assert len(result["database_columns"]) > 0, "Expected non-empty database_columns list"

    # Verify column structure
    first_col = result["database_columns"][0]
    assert "col_name" in first_col or "column_name" in first_col or "Field" in first_col, "Expected column name field in schema"


@pytest.mark.asyncio
async def test_get_resource_info_skips_database_schema_when_disabled(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test get_resource_info skips database_columns when include_database_schema=False."""
    result = await jaffle_shop_server.toolImpl_get_resource_info(
        name="jaffle_shop.customers",
        resource_type="source",
        include_database_schema=False,
    )

    assert result["resource_type"] == "source"
    assert "database_columns" not in result, "Expected no database_columns when include_database_schema=False"


@pytest.mark.asyncio
async def test_get_resource_info_multiple_matches_with_database_schema(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test get_resource_info enriches all matches with database_columns when multiple resources match."""
    # Query "customers" without resource_type - should match both source and model
    result = await jaffle_shop_server.toolImpl_get_resource_info(
        name="customers",
        resource_type=None,
        include_database_schema=True,
    )

    # Should have multiple matches
    assert result.get("multiple_matches") is True, "Expected multiple_matches=True for 'customers'"
    assert result["match_count"] >= 2, "Expected at least 2 matches (source + model)"

    matches = result["matches"]
    assert len(matches) >= 2, "Expected at least 2 match objects"

    # Find the source and model matches
    source_match = next((m for m in matches if m.get("resource_type") == "source"), None)
    model_match = next((m for m in matches if m.get("resource_type") == "model"), None)

    assert source_match is not None, "Expected to find source match"
    assert model_match is not None, "Expected to find model match"

    # Both should have database_columns enrichment
    assert "database_columns" in source_match, "Expected database_columns in source match"
    assert isinstance(source_match["database_columns"], list), "Expected list for source database_columns"
    assert len(source_match["database_columns"]) > 0, "Expected non-empty database_columns for source"

    assert "database_columns" in model_match, "Expected database_columns in model match"
    assert isinstance(model_match["database_columns"], list), "Expected list for model database_columns"
    assert len(model_match["database_columns"]) > 0, "Expected non-empty database_columns for model"
