"""Tests for CTE test generator functionality."""

from pathlib import Path

from dbt_core_mcp.cte_generator import (
    generate_cte_tests,
    is_position_in_comment,
    parse_csv_fixture,
    rows_to_sql,
)


class TestRowsToSql:
    """Test SQL generation from fixture rows."""

    def test_empty_rows_with_columns(self) -> None:
        """Test generating SQL for empty fixture with known columns."""
        result = rows_to_sql([], columns=["id", "name"])
        assert "SELECT NULL as id, NULL as name WHERE 1=0" == result

    def test_empty_rows_no_columns(self) -> None:
        """Test generating SQL for completely empty fixture."""
        result = rows_to_sql([])
        assert "SELECT NULL WHERE FALSE" == result

    def test_single_row_dict(self) -> None:
        """Test generating SQL from single row."""
        rows = [{"id": 1, "name": "Alice"}]
        result = rows_to_sql(rows)
        assert "SELECT 1 as id, 'Alice' as name" == result

    def test_multiple_rows(self) -> None:
        """Test generating SQL with UNION ALL for multiple rows."""
        rows = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        result = rows_to_sql(rows)
        assert "SELECT 1 as id, 'Alice' as name\nUNION ALL\nSELECT 2 as id, 'Bob' as name" == result

    def test_numeric_string_detection(self) -> None:
        """Test that numeric strings from CSV are detected and not quoted."""
        rows = [{"id": "123", "amount": "45.67"}]
        result = rows_to_sql(rows)
        assert "SELECT 45.67 as amount, 123 as id" == result  # Sorted columns

    def test_sql_escaping(self) -> None:
        """Test that single quotes in strings are escaped."""
        rows = [{"name": "O'Brien"}]
        result = rows_to_sql(rows)
        assert "SELECT 'O''Brien' as name" == result

    def test_null_values(self) -> None:
        """Test NULL handling in fixtures."""
        rows = [{"id": 1, "name": None}]
        result = rows_to_sql(rows)
        assert "SELECT 1 as id, NULL as name" == result


class TestParseCsvFixture:
    """Test CSV fixture parsing."""

    def test_empty_csv(self) -> None:
        """Test parsing empty CSV."""
        columns, rows = parse_csv_fixture("")
        assert columns == []
        assert rows == []

    def test_csv_headers_only(self) -> None:
        """Test parsing CSV with only headers."""
        csv_text = "id,name\n"
        columns, rows = parse_csv_fixture(csv_text)
        assert columns == ["id", "name"]
        assert rows == []

    def test_csv_with_data(self) -> None:
        """Test parsing CSV with data rows."""
        csv_text = "id,name\n1,Alice\n2,Bob"
        columns, rows = parse_csv_fixture(csv_text)
        assert columns == ["id", "name"]
        assert len(rows) == 2
        assert rows[0] == {"id": "1", "name": "Alice"}
        assert rows[1] == {"id": "2", "name": "Bob"}


class TestIsPositionInComment:
    """Test comment detection in SQL."""

    def test_not_in_comment(self) -> None:
        """Test position outside any comment."""
        sql = "SELECT * FROM customers"
        assert not is_position_in_comment(sql, 7)  # Position of 'FROM'

    def test_in_line_comment(self) -> None:
        """Test position inside line comment."""
        sql = "SELECT * -- this is a comment\nFROM customers"
        assert is_position_in_comment(sql, 20)  # Inside comment

    def test_not_in_line_comment_next_line(self) -> None:
        """Test position on next line after line comment."""
        sql = "SELECT * -- comment\nFROM customers"
        assert not is_position_in_comment(sql, 25)  # 'FROM' on next line

    def test_in_block_comment(self) -> None:
        """Test position inside block comment."""
        sql = "SELECT * /* block comment */ FROM"
        assert is_position_in_comment(sql, 15)  # Inside block comment

    def test_after_block_comment(self) -> None:
        """Test position after closed block comment."""
        sql = "SELECT * /* comment */ FROM"
        assert not is_position_in_comment(sql, 24)  # After */

    def test_in_jinja_comment(self) -> None:
        """Test position inside Jinja comment."""
        sql = "SELECT * {# jinja comment #} FROM"
        assert is_position_in_comment(sql, 15)  # Inside {# #}

    def test_nested_block_comments(self) -> None:
        """Test nested block comments."""
        sql = "SELECT /* outer /* inner */ still in */ FROM"
        assert is_position_in_comment(sql, 20)  # Still inside after first */
        assert not is_position_in_comment(sql, 41)  # After final */


class TestGenerateCteTests:
    """Test full CTE test generation workflow."""

    def test_generate_with_no_cte_tests(self, tmp_path: Path) -> None:
        """Test generation with project that has no CTE tests."""
        # Create minimal project structure
        project_dir = tmp_path / "project"
        project_dir.mkdir()
        (project_dir / "models").mkdir()
        (project_dir / "unit_tests").mkdir()

        # Create a test file without CTE tests
        test_file = project_dir / "unit_tests" / "test_unit_tests.yml"
        test_file.write_text("""
version: 2
unit_tests:
  - name: regular_test
    model: customers
    given: []
    expect:
      rows: []
""")

        count = generate_cte_tests(project_dir)
        assert count == 0

    def test_generate_with_cte_test(self, tmp_path: Path) -> None:
        """Test generation with a simple CTE test."""
        # Create project structure
        project_dir = tmp_path / "project"
        (project_dir / "models" / "marts").mkdir(parents=True)
        (project_dir / "unit_tests" / "marts").mkdir(parents=True)

        # Create a model with a CTE
        model_file = project_dir / "models" / "marts" / "customers.sql"
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

        # Create a CTE test
        test_file = project_dir / "unit_tests" / "marts" / "customers_unit_tests.yml"
        test_file.write_text("""
version: 2
unit_tests:
  - name: test_customer_agg
    model: customers::customer_agg
    config:
      cte_test: true
    given:
      - input: ref('orders')
        rows:
          - {customer_id: 1, order_id: 100}
          - {customer_id: 1, order_id: 101}
    expect:
      rows:
        - {customer_id: 1, order_count: 2}
""")

        count = generate_cte_tests(project_dir)
        assert count == 1

        # Verify generated files exist
        gen_models_dir = project_dir / "models" / "marts" / "__cte_tests"
        gen_tests_dir = project_dir / "unit_tests" / "marts" / "__cte_tests"

        assert gen_models_dir.exists()
        assert gen_tests_dir.exists()

        # Check that model and test were generated
        generated_models = list(gen_models_dir.glob("*.sql"))
        generated_tests = list(gen_tests_dir.glob("*.yml"))

        assert len(generated_models) == 1
        assert len(generated_tests) == 1

        # Verify generated model content
        model_content = generated_models[0].read_text()
        assert "-- sqlfluff:disable" in model_content
        assert "customer_agg as (" in model_content
        assert "select * from customer_agg" in model_content
        assert "from {{ ref('orders') }}" in model_content

    def test_cleanup_old_generated_files(self, tmp_path: Path) -> None:
        """Test that old generated files are cleaned up before generation."""
        # Create project structure
        project_dir = tmp_path / "project"
        (project_dir / "models" / "marts" / "__cte_tests").mkdir(parents=True)
        (project_dir / "unit_tests" / "marts" / "__cte_tests").mkdir(parents=True)

        # Create old generated files
        old_model = project_dir / "models" / "marts" / "__cte_tests" / "old.sql"
        old_test = project_dir / "unit_tests" / "marts" / "__cte_tests" / "old.yml"
        old_model.write_text("OLD")
        old_test.write_text("OLD")

        # Create empty structure for test
        (project_dir / "unit_tests" / "marts").mkdir(parents=True, exist_ok=True)
        test_file = project_dir / "unit_tests" / "marts" / "test_unit_tests.yml"
        test_file.write_text("""
version: 2
unit_tests: []
""")

        generate_cte_tests(project_dir)

        # Old files should be deleted
        assert not old_model.exists()
        assert not old_test.exists()

    def test_multiple_cte_tests(self, tmp_path: Path) -> None:
        """Test generation with multiple CTE tests."""
        # Create project structure
        project_dir = tmp_path / "project"
        (project_dir / "models" / "marts").mkdir(parents=True)
        (project_dir / "unit_tests" / "marts").mkdir(parents=True)

        # Create a model with multiple CTEs
        model_file = project_dir / "models" / "marts" / "customers.sql"
        model_file.write_text("""
with orders as (
    select * from {{ ref('stg_orders') }}
),

customer_agg as (
    select customer_id, count(*) as order_count
    from orders
    group by customer_id
)

select * from customer_agg
""")

        # Create tests for both CTEs
        test_file = project_dir / "unit_tests" / "marts" / "customers_unit_tests.yml"
        test_file.write_text("""
version: 2
unit_tests:
  - name: test_orders
    model: customers::orders
    config:
      cte_test: true
    given:
      - input: ref('stg_orders')
        rows: []
    expect:
      rows: []

  - name: test_customer_agg
    model: customers::customer_agg
    config:
      cte_test: true
    given:
      - input: ref('stg_orders')
        rows: []
    expect:
      rows: []
""")

        count = generate_cte_tests(project_dir)
        assert count == 2

        # Verify both tests generated
        gen_models = list((project_dir / "models" / "marts" / "__cte_tests").glob("*.sql"))
        assert len(gen_models) == 2
