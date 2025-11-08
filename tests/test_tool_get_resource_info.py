"""
Tests for toolImpl_get_resource_info.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dbt_core_mcp.server import DbtCoreMcpServer


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


async def test_get_resource_info_skip_compiled_sql(jaffle_shop_server: "DbtCoreMcpServer") -> None:
    """Test get_resource_info tool can skip compiled SQL with include_compiled_sql=False."""
    result = await jaffle_shop_server.toolImpl_get_resource_info(name="customers", resource_type="model", include_compiled_sql=False)

    assert result["name"] == "customers"
    assert result["resource_type"] == "model"
    assert "compiled_sql" not in result


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
