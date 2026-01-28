"""Tests for downstream column lineage with unified structure."""

from unittest.mock import Mock

import pytest

from dbt_core_mcp.tools.get_column_lineage import implementation


@pytest.mark.asyncio
async def test_downstream_basic_structure(fixture_manifest: Mock):
    """Test downstream returns usages instead of dependencies."""
    result = await implementation(
        ctx=None,
        model_name="stg_customers",
        column_name="customer_id",
        direction="downstream",
        depth=None,
        state=fixture_manifest,
        force_parse=False,
    )

    # Should have usages, NOT dependencies
    assert "usages" in result
    assert "dependencies" not in result
    assert result["direction"] == "downstream"
    assert result["model"] == "stg_customers"
    assert result["column"] == "customer_id"


@pytest.mark.asyncio
async def test_downstream_no_root_transformations(fixture_manifest: Mock):
    """Test downstream doesn't include transformations at root (we're the source)."""
    result = await implementation(
        ctx=None,
        model_name="stg_customers",
        column_name="customer_id",
        direction="downstream",
        depth=None,
        state=fixture_manifest,
        force_parse=False,
    )

    # No transformations at root - we're the source
    assert "transformations" not in result


@pytest.mark.asyncio
async def test_downstream_usage_has_transformations(fixture_manifest: Mock):
    """Test each usage includes transformations showing how they use the column."""
    result = await implementation(
        ctx=None,
        model_name="stg_customers",
        column_name="customer_id",
        direction="downstream",
        depth=None,
        state=fixture_manifest,
        force_parse=False,
    )

    assert len(result["usages"]) > 0
    usage = result["usages"][0]

    # Each usage should have transformations
    assert "transformations" in usage
    assert isinstance(usage["transformations"], list)
    assert len(usage["transformations"]) > 0


@pytest.mark.asyncio
async def test_downstream_transformations_reversed(fixture_manifest: Mock):
    """Test transformations are in reverse order (input → output)."""
    result = await implementation(
        ctx=None,
        model_name="stg_customers",
        column_name="customer_id",
        direction="downstream",
        depth=None,
        state=fixture_manifest,
        force_parse=False,
    )

    # Find test_lineage usage (it has complex transformations)
    test_lineage_usage = next((u for u in result["usages"] if u["model"] == "test_lineage"), None)
    assert test_lineage_usage is not None

    transformations = test_lineage_usage["transformations"]

    # Transformations should be reversed (bottom-to-top: inputs → output)
    # First should NOT be outer query, last SHOULD be outer query
    assert transformations[0]["type"] != "outer_query"  # Not the query
    assert transformations[0]["id"].startswith(("table:", "cte:"))  # Input source
    assert transformations[-1]["type"] == "outer_query"


@pytest.mark.asyncio
async def test_downstream_includes_union_ctes(fixture_manifest: Mock):
    """Test downstream transformations include UNION CTEs with branches."""
    result = await implementation(
        ctx=None,
        model_name="stg_customers",
        column_name="customer_id",
        direction="downstream",
        depth=None,
        state=fixture_manifest,
        force_parse=False,
    )

    test_lineage_usage = next((u for u in result["usages"] if u["model"] == "test_lineage"), None)
    assert test_lineage_usage is not None

    # Should have UNION transformations
    union_transforms = [t for t in test_lineage_usage["transformations"] if t.get("type") == "union"]
    assert len(union_transforms) > 0

    # UNION should have branches
    union_transform = union_transforms[0]
    assert "branches" in union_transform
    assert isinstance(union_transform["branches"], list)


@pytest.mark.asyncio
async def test_downstream_includes_alias_notation(fixture_manifest: Mock):
    """Test downstream transformations include alias notation in sources."""
    result = await implementation(
        ctx=None,
        model_name="stg_customers",
        column_name="customer_id",
        direction="downstream",
        depth=None,
        state=fixture_manifest,
        force_parse=False,
    )

    test_lineage_usage = next((u for u in result["usages"] if u["model"] == "test_lineage"), None)
    assert test_lineage_usage is not None

    # Should have transformations with alias notation
    # e.g., "table:stg_customers[c]"
    transforms_with_aliases = [t for t in test_lineage_usage["transformations"] if "sources" in t and any("[" in s for s in t.get("sources", []))]
    assert len(transforms_with_aliases) > 0


@pytest.mark.asyncio
async def test_downstream_recursive_usages(fixture_manifest: Mock):
    """Test usages can have nested usages (recursive structure)."""
    result = await implementation(
        ctx=None,
        model_name="stg_customers",
        column_name="customer_id",
        direction="downstream",
        depth=None,  # unlimited depth
        state=fixture_manifest,
        force_parse=False,
    )

    # At least one usage should exist
    assert len(result["usages"]) > 0

    # Usages can have their own usages (recursive)
    usage = result["usages"][0]
    assert "model" in usage
    assert "column" in usage
    assert "distance" in usage
    # May or may not have nested usages depending on data
    # assert "usages" in usage  # optional


@pytest.mark.asyncio
async def test_downstream_distance_tracking(fixture_manifest: Mock):
    """Test distance field tracks hops from source."""
    result = await implementation(
        ctx=None,
        model_name="stg_customers",
        column_name="customer_id",
        direction="downstream",
        depth=None,
        state=fixture_manifest,
        force_parse=False,
    )

    # All direct usages should have distance=1
    for usage in result["usages"]:
        assert usage["distance"] == 1


@pytest.mark.asyncio
async def test_both_directions_complete_structure(fixture_manifest: Mock):
    """Test direction='both' includes transformations, dependencies, and usages."""
    result = await implementation(
        ctx=None,
        model_name="customers",
        column_name="customer_id",
        direction="both",
        depth=1,
        state=fixture_manifest,
        force_parse=False,
    )

    # Should have all three components
    assert result["direction"] == "both"
    assert result["model"] == "customers"
    assert result["column"] == "customer_id"

    # From upstream: transformations showing how customers builds customer_id
    assert "transformations" in result
    assert isinstance(result["transformations"], list)
    assert len(result["transformations"]) > 0
    # Should have recognizable transformation types
    transform_types = {t.get("type") for t in result["transformations"]}
    assert transform_types & {"outer_query", "cte", "table"}  # At least one of these

    # From upstream: dependencies showing where data comes from
    assert "dependencies" in result
    assert isinstance(result["dependencies"], list)
    assert result["dependency_count"] == len(result["dependencies"])
    # Should depend on stg_customers
    assert any("stg_customers" in dep.get("table", "").lower() for dep in result["dependencies"])

    # From downstream: usages showing who uses this column
    assert "usages" in result
    assert isinstance(result["usages"], list)
    # Each usage should have transformations (how they transform it)
    for usage in result["usages"]:
        assert "model" in usage
        assert "column" in usage
        assert "distance" in usage
        assert "transformations" in usage
        assert isinstance(usage["transformations"], list)
