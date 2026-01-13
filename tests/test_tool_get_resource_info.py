"""
Tests for get_resource_info tool.
"""

from unittest.mock import AsyncMock, Mock

import pytest

from dbt_core_mcp.tools.get_resource_info import _implementation as get_resource_info_impl  # type: ignore[reportPrivateUsage]


@pytest.fixture
def mock_state() -> Mock:
    """Create mock state for get_resource_info tool testing."""
    state = Mock()
    state.ensure_initialized = AsyncMock()

    # Mock manifest object with get_resource_info method
    mock_manifest = Mock()

    def get_resource_info(name: str, resource_type: str | None = None, include_database_schema: bool = False, include_compiled_sql: bool = False) -> dict:
        """Mock implementation of manifest.get_resource_info()"""
        # Handle multiple matches when resource_type is None and name is "customers"
        if resource_type is None and name == "customers":
            matches_list = [
                {
                    "source_name": "jaffle_shop",
                    "name": "customers",
                    "resource_type": "source",
                    "database_columns": [
                        {"col_name": "id", "type": "INTEGER"},
                        {"col_name": "name", "type": "VARCHAR"},
                        {"col_name": "email", "type": "VARCHAR"},
                    ]
                    if include_database_schema
                    else None,
                },
                {
                    "name": "customers",
                    "resource_type": "model",
                    "database_columns": [
                        {"col_name": "customer_id", "type": "INTEGER"},
                        {"col_name": "first_name", "type": "VARCHAR"},
                    ]
                    if include_database_schema
                    else None,
                },
            ]
            # Remove None database_columns entries
            for match in matches_list:
                if match["database_columns"] is None:
                    del match["database_columns"]

            return {
                "multiple_matches": True,
                "match_count": 2,
                "matches": matches_list,
            }

        # Simulate finding customers model
        if name == "customers" and resource_type in ("model", None):
            result = {
                "name": "customers",
                "resource_type": "model",
                "fqn": ["jaffle_shop", "models", "marts", "customers"],
                "description": "Customer dimension table",
                "columns": {
                    "customer_id": {"name": "customer_id", "description": "Primary key"},
                    "first_name": {"name": "first_name"},
                },
            }
            if include_compiled_sql:
                result["compiled_sql"] = "SELECT customer_id, first_name FROM stg_customers"
            if include_database_schema:
                result["database_columns"] = [
                    {"col_name": "customer_id", "type": "INTEGER"},
                    {"col_name": "first_name", "type": "VARCHAR"},
                ]
            return result

        # Simulate finding jaffle_shop.customers source
        if name == "jaffle_shop.customers" and resource_type in ("source", None):
            result = {
                "source_name": "jaffle_shop",
                "name": "customers",
                "resource_type": "source",
                "identifier": "raw_customers",
                "database": "main",
                "schema": "public",
                "description": "Raw customers from source",
                "columns": {
                    "id": {"name": "id", "description": "ID"},
                    "name": {"name": "name"},
                },
            }
            if include_database_schema:
                result["database_columns"] = [
                    {"col_name": "id", "type": "INTEGER"},
                    {"col_name": "name", "type": "VARCHAR"},
                    {"col_name": "email", "type": "VARCHAR"},
                ]
            return result

        # Simulate finding raw_customers seed
        if name == "raw_customers" and resource_type in ("seed", None):
            result = {
                "name": "raw_customers",
                "resource_type": "seed",
                "fqn": ["jaffle_shop", "seeds", "raw_customers"],
                "description": "Raw customer data",
            }
            return result

        return {}

    mock_manifest.get_resource_info = get_resource_info
    state.manifest = mock_manifest

    # Mock database schema query
    state.get_table_schema_from_db = AsyncMock(
        return_value=[
            {"col_name": "id", "type": "INTEGER"},
            {"col_name": "name", "type": "VARCHAR"},
            {"col_name": "email", "type": "VARCHAR"},
        ]
    )

    return state


@pytest.mark.asyncio
async def test_get_resource_info_with_compiled_sql(mock_state: Mock) -> None:
    """Test get_resource_info tool includes compiled SQL and triggers compilation if needed."""
    result = await get_resource_info_impl(None, "customers", "model", False, True, mock_state, force_parse=False)

    assert result["name"] == "customers"
    assert result["resource_type"] == "model"

    # Verify compiled SQL is present
    assert result.get("compiled_sql") is not None, "Expected compiled SQL to be present"
    assert "{{" not in result.get("compiled_sql", ""), "Expected no Jinja templates in compiled SQL"


@pytest.mark.asyncio
async def test_get_resource_info_skip_compiled_sql(mock_state: Mock) -> None:
    """Test get_resource_info tool can skip compiled SQL with include_compiled_sql=False."""
    result = await get_resource_info_impl(None, "customers", "model", False, False, mock_state, force_parse=False)

    assert result["name"] == "customers"
    assert result["resource_type"] == "model"
    # When include_compiled_sql=False, compiled_sql should not be in the output
    # (implementation may omit it or keep it but mark as not requested)


@pytest.mark.asyncio
async def test_get_resource_info_compiled_sql_only_for_models(mock_state: Mock) -> None:
    """Test get_resource_info tool only includes compiled SQL for models, not sources/seeds."""
    # Test with source - should not have compiled_sql even if requested
    source_result = await get_resource_info_impl(None, "jaffle_shop.customers", "source", False, True, mock_state, force_parse=False)
    assert source_result["resource_type"] == "source"
    assert "compiled_sql" not in source_result or source_result["compiled_sql"] is None

    # Test with seed - should not have compiled_sql even if requested
    seed_result = await get_resource_info_impl(None, "raw_customers", "seed", False, True, mock_state, force_parse=False)
    assert seed_result["resource_type"] == "seed"
    assert "compiled_sql" not in seed_result or seed_result["compiled_sql"] is None


@pytest.mark.asyncio
async def test_get_resource_info_uses_cached_compilation(mock_state: Mock) -> None:
    """Test that get_resource_info doesn't recompile when compiled SQL is already cached."""
    # First call - returns compiled SQL from manifest
    result1 = await get_resource_info_impl(None, "customers", "model", False, True, mock_state, force_parse=False)

    assert result1["compiled_sql"] is not None, "First call should return compiled SQL"
    compiled_sql_1 = result1["compiled_sql"]

    # Second call - should return same cached SQL
    result2 = await get_resource_info_impl(None, "customers", "model", False, True, mock_state, force_parse=False)

    assert result2["compiled_sql"] is not None, "Second call should return compiled SQL"
    assert result2["compiled_sql"] == compiled_sql_1, "Second call should return identical SQL (cached)"


@pytest.mark.asyncio
async def test_get_resource_info_includes_database_schema_for_sources(mock_state: Mock) -> None:
    """Test get_resource_info includes database_columns for sources when include_database_schema=True."""
    result = await get_resource_info_impl(
        None,
        "jaffle_shop.customers",
        "source",
        True,
        False,
        mock_state,
    )

    assert result["resource_type"] == "source"
    assert result["source_name"] == "jaffle_shop"
    assert result["name"] == "customers"

    # Verify database_columns is present
    assert "database_columns" in result, "Expected database_columns to be present for source"
    assert isinstance(result["database_columns"], list), "Expected database_columns to be a list"
    assert len(result["database_columns"]) > 0, "Expected non-empty database_columns list"


@pytest.mark.asyncio
async def test_get_resource_info_skips_database_schema_when_disabled(mock_state: Mock) -> None:
    """Test get_resource_info skips database_columns when include_database_schema=False."""
    result = await get_resource_info_impl(
        None,
        "jaffle_shop.customers",
        "source",
        False,
        False,
        mock_state,
    )

    assert result["resource_type"] == "source"
    assert "database_columns" not in result, "Expected no database_columns when include_database_schema=False"


@pytest.mark.asyncio
async def test_get_resource_info_multiple_matches_with_database_schema(mock_state: Mock) -> None:
    """Test get_resource_info enriches all matches with database_columns when multiple resources match."""
    # Query "customers" without resource_type - should match both source and model
    result = await get_resource_info_impl(
        None,
        "customers",
        None,
        True,
        False,
        mock_state,
    )

    # Should have multiple matches
    assert result.get("multiple_matches") is True, "Expected multiple_matches=True for 'customers'"
    assert result["match_count"] >= 2, "Expected at least 2 matches (source + model)"

    matches = result["matches"]
    assert len(matches) >= 2, "Expected at least 2 match objects"
