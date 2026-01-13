"""Tests for dbt output parsing using real and synthetic fixtures.

This validates that our output parsing logic handles various scenarios:
- Successful builds (real fixture)
- Failed builds (synthetic)
- Mixed scenarios (synthetic)
- Different resource types
- Edge cases (empty results, skipped)
"""

import json
from pathlib import Path
from typing import Any, Dict

import pytest


@pytest.fixture
def fixtures_dir() -> Path:
    """Path to test fixtures directory."""
    return Path(__file__).parent / "fixtures"


@pytest.fixture
def real_run_results(fixtures_dir: Path) -> Dict[str, Any]:
    """Load real successful dbt run_results.json fixture."""
    run_results_path = fixtures_dir / "target" / "run_results.json"
    assert run_results_path.exists(), f"Fixture not found: {run_results_path}"

    with open(run_results_path) as f:
        return json.load(f)


@pytest.fixture
def failed_run_results() -> Dict[str, Any]:
    """Create synthetic failed run results for testing."""
    return {
        "metadata": {"dbt_schema_version": "https://schemas.getdbt.com/dbt/run-results/v6.json"},
        "results": [
            {
                "status": "error",
                "timing": [{"name": "compile", "started_at": "2026-01-13T13:11:56.602661Z", "completed_at": "2026-01-13T13:11:56.602661Z"}],
                "thread_id": "Thread-1",
                "execution_time": 0.5,
                "adapter_response": {},
                "message": "Compilation Error in model 'customers' (models/customers.sql)\n  column \"invalid_column\" must appear in the GROUP BY clause",
                "failures": ["compilation error"],
                "unique_id": "model.jaffle_shop.customers",
                "compiled": None,
                "compiled_code": None,
                "relation_name": None,
                "batch_results": None,
            }
        ],
        "elapsed_time": 0.8,
        "args": {"which": "build"},
    }


@pytest.fixture
def mixed_run_results() -> Dict[str, Any]:
    """Create synthetic mixed results (success, fail, skip) for testing."""
    return {
        "metadata": {"dbt_schema_version": "https://schemas.getdbt.com/dbt/run-results/v6.json"},
        "results": [
            {
                "status": "success",
                "timing": [],
                "thread_id": "Thread-1",
                "execution_time": 0.2,
                "adapter_response": {"_message": "CREATE TABLE", "code": "CREATE"},
                "message": "CREATE TABLE (5 rows, 0 processed)",
                "failures": None,
                "unique_id": "model.jaffle_shop.stg_customers",
                "compiled": None,
                "compiled_code": None,
                "relation_name": None,
                "batch_results": None,
            },
            {
                "status": "fail",
                "timing": [],
                "thread_id": "Thread-2",
                "execution_time": 0.1,
                "adapter_response": {},
                "message": "FAIL 1 assert count(*) > 0",
                "failures": ["assert count(*) > 0"],
                "unique_id": "test.jaffle_shop.assert_positive_order_amount",
                "compiled": None,
                "compiled_code": None,
                "relation_name": None,
                "batch_results": None,
            },
            {
                "status": "skipped",
                "timing": [],
                "thread_id": "Thread-3",
                "execution_time": 0.0,
                "adapter_response": {},
                "message": "SKIPPED",
                "failures": None,
                "unique_id": "model.jaffle_shop.orders",
                "compiled": None,
                "compiled_code": None,
                "relation_name": None,
                "batch_results": None,
            },
        ],
        "elapsed_time": 1.5,
        "args": {"which": "build"},
    }


@pytest.fixture
def empty_run_results() -> Dict[str, Any]:
    """Create empty results for testing no-match scenarios."""
    return {"metadata": {"dbt_schema_version": "https://schemas.getdbt.com/dbt/run-results/v6.json"}, "results": [], "elapsed_time": 0.1, "args": {"which": "build", "select": "nonexistent_model"}}


# Comprehensive Output Parsing Tests


def test_successful_results_parsing(real_run_results: Dict[str, Any]) -> None:
    """Test parsing successful dbt build output from real fixture."""
    results_list = real_run_results.get("results", [])

    # Assert basic structure
    assert len(results_list) > 0, "Should have results in successful fixture"
    assert "elapsed_time" in real_run_results, "Should have elapsed_time"
    assert real_run_results["elapsed_time"] >= 0, "Elapsed time should be non-negative"

    # Test counting logic with all successful results
    passed_count = sum(1 for r in results_list if r.get("status") in ("success", "pass"))
    failed_count = sum(1 for r in results_list if r.get("status") in ("error", "fail"))
    skip_count = sum(1 for r in results_list if r.get("status") == "skipped")

    assert passed_count == len(results_list), "All results should be successful in real fixture"
    assert failed_count == 0, "Should have no failures in successful fixture"
    assert skip_count == 0, "Should have no skips in successful fixture"


def test_failed_results_parsing(failed_run_results: Dict[str, Any]) -> None:
    """Test parsing failed dbt build output."""
    results_list = failed_run_results.get("results", [])

    # Test counting logic with failed results
    passed_count = sum(1 for r in results_list if r.get("status") in ("success", "pass"))
    failed_count = sum(1 for r in results_list if r.get("status") in ("error", "fail"))
    skip_count = sum(1 for r in results_list if r.get("status") == "skipped")
    total = len(results_list)

    assert total > 0, "Should have results"
    assert failed_count > 0, "Should have failures in failed fixture"
    assert passed_count == 0, "Should have no successes in failed fixture"
    assert passed_count + failed_count + skip_count == total, "Counts should add up"

    # Test message construction for failures
    parts = []
    if passed_count > 0:
        parts.append(f"✅ {passed_count} passed" if failed_count > 0 or skip_count > 0 else "✅ All passed")
    if failed_count > 0:
        parts.append(f"❌ {failed_count} failed")
    if skip_count > 0:
        parts.append(f"⏭️ {skip_count} skipped")

    summary = f"Build: {total}/{total} resources completed ({', '.join(parts)})"

    assert "❌" in summary, "Should have failure indicator"
    assert "failed" in summary, "Should mention failures"
    assert "✅" not in summary, "Should not have success indicator when all failed"


def test_mixed_results_parsing(mixed_run_results: Dict[str, Any]) -> None:
    """Test parsing mixed success/failure/skip results."""
    results_list = mixed_run_results.get("results", [])

    # Test counting logic with mixed results
    passed_count = sum(1 for r in results_list if r.get("status") in ("success", "pass"))
    failed_count = sum(1 for r in results_list if r.get("status") in ("error", "fail"))
    skip_count = sum(1 for r in results_list if r.get("status") == "skipped")
    total = len(results_list)

    assert total == 3, "Mixed fixture should have 3 results"
    assert passed_count == 1, "Should have 1 success"
    assert failed_count == 1, "Should have 1 failure"
    assert skip_count == 1, "Should have 1 skip"
    assert passed_count + failed_count + skip_count == total, "Counts should add up"

    # Test message construction for mixed results
    parts = []
    if passed_count > 0:
        parts.append(f"✅ {passed_count} passed" if failed_count > 0 or skip_count > 0 else "✅ All passed")
    if failed_count > 0:
        parts.append(f"❌ {failed_count} failed")
    if skip_count > 0:
        parts.append(f"⏭️ {skip_count} skipped")

    summary = f"Build: {total}/{total} resources completed ({', '.join(parts)})"

    # Should show all three indicators
    assert "✅ 1 passed" in summary, "Should show specific passed count"
    assert "❌ 1 failed" in summary, "Should show failed count"
    assert "⏭️ 1 skipped" in summary, "Should show skipped count"
    assert "All passed" not in summary, "Should not show 'All passed' when mixed"


def test_empty_results_parsing(empty_run_results: Dict[str, Any]) -> None:
    """Test parsing empty results (no matches)."""
    results_list = empty_run_results.get("results", [])

    # Test counting logic with empty results
    passed_count = sum(1 for r in results_list if r.get("status") in ("success", "pass"))
    failed_count = sum(1 for r in results_list if r.get("status") in ("error", "fail"))
    skip_count = sum(1 for r in results_list if r.get("status") == "skipped")
    total = len(results_list)

    assert total == 0, "Empty fixture should have no results"
    assert passed_count == 0, "Should have no passes"
    assert failed_count == 0, "Should have no failures"
    assert skip_count == 0, "Should have no skips"

    # Test message construction for empty results
    parts = []
    if passed_count > 0:
        parts.append(f"✅ {passed_count} passed" if failed_count > 0 or skip_count > 0 else "✅ All passed")
    if failed_count > 0:
        parts.append(f"❌ {failed_count} failed")
    if skip_count > 0:
        parts.append(f"⏭️ {skip_count} skipped")

    # Empty results should produce special message
    if not parts:
        summary = "Build: 0/0 resources completed (no resources matched selection)"
    else:
        summary = f"Build: {total}/{total} resources completed ({', '.join(parts)})"

    assert "0/0" in summary, "Should show zero progress"
    assert "no resources matched" in summary or len(parts) == 0, "Should handle empty case"


def test_resource_type_extraction(real_run_results: Dict[str, Any]) -> None:
    """Test extraction of resource types from unique_id."""
    results_list = real_run_results.get("results", [])

    # Extract resource types from unique_id (format: resource_type.package.name)
    resource_types = set()
    for result in results_list:
        unique_id = result.get("unique_id", "")
        if "." in unique_id:
            resource_type = unique_id.split(".")[0]
            resource_types.add(resource_type)

    print(f"Found resource types: {resource_types}")

    # Assert we found valid resource types
    assert len(resource_types) > 0, "Should extract at least one resource type"
    valid_types = {"model", "test", "seed", "snapshot", "analysis", "macro", "source", "unit_test"}
    invalid_types = resource_types - valid_types
    assert not invalid_types, f"Found invalid resource types: {invalid_types}"


def test_result_structure_validation(real_run_results: Dict[str, Any], failed_run_results: Dict[str, Any], mixed_run_results: Dict[str, Any]) -> None:
    """Test that all result structures have required fields."""
    all_fixtures = [("real", real_run_results), ("failed", failed_run_results), ("mixed", mixed_run_results)]

    for fixture_name, fixture_data in all_fixtures:
        print(f"\nValidating {fixture_name} fixture structure:")

        # Validate top-level structure
        assert "results" in fixture_data, f"{fixture_name}: Should have 'results' key"
        assert "elapsed_time" in fixture_data, f"{fixture_name}: Should have 'elapsed_time' key"
        assert isinstance(fixture_data["results"], list), f"{fixture_name}: results should be list"
        assert isinstance(fixture_data["elapsed_time"], (int, float)), f"{fixture_name}: elapsed_time should be numeric"

        # Validate each result structure
        results = fixture_data["results"]
        for i, result in enumerate(results):
            required_fields = ["status", "unique_id", "message", "timing", "execution_time"]
            for field in required_fields:
                assert field in result, f"{fixture_name} result {i}: Should have '{field}' field"

            # Validate status values
            status = result["status"]
            valid_statuses = {"success", "pass", "error", "fail", "skipped"}
            assert status in valid_statuses, f"{fixture_name} result {i}: Invalid status '{status}'"

            # Validate unique_id format
            unique_id = result["unique_id"]
            assert isinstance(unique_id, str), f"{fixture_name} result {i}: unique_id should be string"
            assert "." in unique_id, f"{fixture_name} result {i}: unique_id should have dots (resource_type.package.name)"


def test_fixture_structure_analysis(real_run_results: Dict[str, Any]) -> None:
    """Analyze the structure of real dbt output - kept for debugging purposes."""
    print("\n=== Real dbt build output structure ===")
    print(f"Top-level keys: {list(real_run_results.keys())}")
    print(f"Total results: {len(real_run_results.get('results', []))}")
    print(f"Elapsed time: {real_run_results.get('elapsed_time')} seconds")

    results = real_run_results.get("results", [])
    if results:
        print(f"Sample result keys: {list(results[0].keys())}")
        unique_id = results[0].get("unique_id", "")
        if "." in unique_id:
            resource_type = unique_id.split(".")[0]
            print(f"Sample resource type: {resource_type}")
