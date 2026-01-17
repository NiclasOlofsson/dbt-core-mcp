"""
CTE Test Generator for dbt unit tests.

Automatically generates isolated CTE test models and tests from unit tests
marked with `config: cte_test: true`.
"""

import csv
import hashlib
import logging
import re
import shutil
from io import StringIO
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)


def rows_to_sql(rows: list[dict[str, Any]], columns: list[str] | None = None) -> str:
    """Convert list of row dicts (or empty with known columns) to SQL SELECT statements joined by UNION ALL."""
    # Determine column order
    if columns is None:
        cols_union: set[str] = set()
        for row in rows:
            cols_union.update(row.keys())
        columns = sorted(cols_union)

    if not columns:
        return "SELECT NULL WHERE FALSE"  # no columns known

    if not rows:
        col_exprs = [f"NULL as {c}" for c in columns]
        return f"SELECT {', '.join(col_exprs)} WHERE 1=0"

    selects = []
    for row in rows:
        exprs = []
        for col in columns:
            v = row.get(col)
            if v is None:
                exprs.append(f"NULL as {col}")
            elif isinstance(v, str):
                # Try to detect numeric strings from CSV parsing
                # If it looks like a number, use it as-is; otherwise escape as string
                if v.isdigit() or (v.replace(".", "", 1).replace("-", "", 1).isdigit()):
                    # It's a number string from CSV - use as numeric literal
                    exprs.append(f"{v} as {col}")
                else:
                    # SQL standard: escape single quotes by doubling them
                    escaped = v.replace("'", "''")
                    exprs.append(f"'{escaped}' as {col}")
            else:
                exprs.append(f"{v} as {col}")
        selects.append(f"SELECT {', '.join(exprs)}")

    return "\nUNION ALL\n".join(selects)


def parse_csv_fixture(csv_text: str) -> tuple[list[str], list[dict[str, Any]]]:
    """Parse a csv fixture string into (columns, rows_as_dicts)."""
    sio = StringIO(csv_text.strip("\n"))
    reader = csv.DictReader(line for line in sio if line.strip() != "")
    columns = list(reader.fieldnames) if reader.fieldnames else []
    rows = [dict(row) for row in reader]
    return columns, rows


def is_position_in_comment(sql: str, pos: int) -> bool:
    """Check if a position in SQL is inside a comment (SQL or Jinja)."""
    # Check for line comment: is there a '--' before pos on the same line?
    line_start = sql.rfind("\n", 0, pos) + 1  # Start of current line
    line_content = sql[line_start:pos]
    if "--" in line_content:
        return True

    # Check for SQL block comment: count /* and */ before pos
    block_comment_depth = 0
    jinja_comment_depth = 0
    i = 0
    while i < pos:
        if i + 1 < len(sql):
            two_char = sql[i : i + 2]
            # SQL block comments
            if two_char == "/*":
                block_comment_depth += 1
                i += 2
                continue
            elif two_char == "*/":
                block_comment_depth -= 1
                i += 2
                continue
            # Jinja comments {# ... #}
            elif two_char == "{#":
                jinja_comment_depth += 1
                i += 2
                continue
            elif two_char == "#}":
                jinja_comment_depth -= 1
                i += 2
                continue
        i += 1

    return block_comment_depth > 0 or jinja_comment_depth > 0


def replace_cte_with_mock(sql: str, cte_name: str, rows: list[dict[str, Any]], columns: list[str] | None = None) -> str:
    """Replace a CTE definition with a mocked version from fixture rows."""

    # Find the CTE definition (skip commented matches)
    pattern = rf"\b{cte_name}\s+as\s*\("
    matches = list(re.finditer(pattern, sql, re.IGNORECASE))

    if not matches:
        logger.warning(f"Could not find CTE '{cte_name}' to mock")
        return sql

    # Find first match that's not in a comment
    match = None
    for m in matches:
        if not is_position_in_comment(sql, m.start()):
            match = m
            break

    if not match:
        logger.warning(f"CTE '{cte_name}' only found in comments")
        return sql

    # Find matching closing paren
    paren_pos = sql.index("(", match.start())
    paren_count = 1
    end_pos = paren_pos + 1
    in_string = False
    string_char = None
    in_line_comment = False
    in_block_comment = False

    while end_pos < len(sql) and paren_count > 0:
        char = sql[end_pos]
        next_char = sql[end_pos + 1] if end_pos + 1 < len(sql) else ""

        # Handle line comments
        if not in_string and not in_block_comment and char == "-" and next_char == "-":
            in_line_comment = True
            end_pos += 2
            continue

        if in_line_comment:
            if char == "\n":
                in_line_comment = False
            end_pos += 1
            continue

        # Handle block comments
        if not in_string and not in_line_comment and char == "/" and next_char == "*":
            in_block_comment = True
            end_pos += 2
            continue

        if in_block_comment:
            if char == "*" and next_char == "/":
                in_block_comment = False
                end_pos += 2
            else:
                end_pos += 1
            continue

        # Handle string literals (both single and double quotes)
        if char in ('"', "'"):
            if not in_string:
                in_string = True
                string_char = char
            elif char == string_char:
                in_string = False
                string_char = None

        # Count parens only outside strings and comments
        if not in_string and not in_line_comment and not in_block_comment:
            if char == "(":
                paren_count += 1
            elif char == ")":
                paren_count -= 1

        end_pos += 1

    # Replace with mocked CTE
    mock_sql = rows_to_sql(rows, columns=columns)
    mocked_cte = f"{cte_name} AS (\n    {mock_sql}\n)"

    # Replace in original SQL
    original_cte = sql[match.start() : end_pos]
    return sql.replace(original_cte, mocked_cte)


def generate_cte_model(base_model_path: Path, cte_name: str, test_given: list[dict[str, Any]], output_path: Path) -> bool:
    """Generate a truncated model that selects from the target CTE.

    Extracts from start of SQL through target CTE's closing paren,
    preserving all upstream CTEs and WITH clause.

    Also applies any CTE mocks from test fixtures.
    """

    # Read original model
    sql = base_model_path.read_text()

    # Find "cte_name AS (" (skip commented matches)
    pattern = rf"\b{cte_name}\s+AS\s*\("
    matches = list(re.finditer(pattern, sql, re.IGNORECASE))

    if not matches:
        logger.error(f"Could not find CTE '{cte_name}' in {base_model_path}")
        return False

    # Find first match that's not in a comment
    match = None
    for m in matches:
        if not is_position_in_comment(sql, m.start()):
            match = m
            break

    if not match:
        logger.error(f"CTE '{cte_name}' only found in comments in {base_model_path}")
        return False

    # Position of opening paren
    paren_pos = sql.index("(", match.start())

    # Count parens to find matching closing paren
    # Track strings and comments to avoid counting parens inside them
    paren_count = 1
    i = paren_pos + 1
    in_string = False
    string_char = None
    in_line_comment = False
    in_block_comment = False

    while i < len(sql) and paren_count > 0:
        char = sql[i]
        next_char = sql[i + 1] if i + 1 < len(sql) else ""

        # Handle line comments: -- until newline
        if not in_string and not in_block_comment and char == "-" and next_char == "-":
            in_line_comment = True
            i += 2  # Skip both dashes
            continue

        if in_line_comment:
            if char == "\n":
                in_line_comment = False
            i += 1
            continue

        # Handle block comments: /* until */
        if not in_string and not in_line_comment and char == "/" and next_char == "*":
            in_block_comment = True
            i += 2  # Skip /*
            continue

        if in_block_comment:
            if char == "*" and next_char == "/":
                in_block_comment = False
                i += 2  # Skip */
            else:
                i += 1
            continue

        # Handle string literals (both single and double quotes)
        if char in ('"', "'") and (i == 0 or sql[i - 1] != "\\"):
            if not in_string:
                in_string = True
                string_char = char
            elif char == string_char:
                in_string = False
                string_char = None

        # Count parens only outside strings and comments
        if not in_string and not in_line_comment and not in_block_comment:
            if char == "(":
                paren_count += 1
            elif char == ")":
                paren_count -= 1

        i += 1

    if paren_count != 0:
        logger.error(f"Could not find matching closing paren for CTE '{cte_name}'")
        return False

    # Extract from start of SQL to closing paren (includes everything automatically)
    upstream_sql = sql[:i].rstrip()

    # Apply CTE mocks from test fixtures
    # Convention: any given with input starting with '::' denotes a CTE mock
    for given in test_given:
        inp = given.get("input")
        if isinstance(inp, str) and inp.startswith("::"):
            mock_cte_name = inp.lstrip(":")
            fmt = given.get("format", "dict")
            if fmt == "csv":
                columns, mock_rows = parse_csv_fixture(given.get("rows", ""))
            else:
                columns, mock_rows = None, given.get("rows", [])
            logger.debug(f"Mocking CTE: {mock_cte_name}")
            upstream_sql = replace_cte_with_mock(upstream_sql, mock_cte_name, mock_rows, columns)

    # Add final SELECT
    generated_sql = f"{upstream_sql}\n\nselect * from {cte_name}"

    # Add SQLFluff disable directive at the top
    final_sql = f"-- sqlfluff:disable\n{generated_sql}"

    # Write generated model
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(final_sql)
    logger.debug(f"Generated CTE model: {output_path}")

    return True


def generate_cte_test(
    test_yaml_path: Path,
    test_name: str,
    base_model: str,
    cte_name: str,
    generated_model: str,
    gen_model_path: Path,
    output_path: Path,
) -> bool:
    """Generate an enabled test file targeting the generated model.

    Auto-detects missing inputs and adds empty fixtures for them.
    """

    # Parse original YAML
    with open(test_yaml_path) as f:
        test_data = yaml.safe_load(f)

    # Find the test
    target_test = None
    for test in test_data.get("unit_tests", []):
        if test["name"] == test_name:
            target_test = test.copy()
            break

    if not target_test:
        logger.error(f"Could not find test '{test_name}' in {test_yaml_path}")
        return False

    # Read generated model to find all refs/sources
    generated_sql = gen_model_path.read_text()

    # Find all ref() calls
    ref_pattern = r"ref\(['\"](\w+)['\"]\)"
    refs = re.findall(ref_pattern, generated_sql)

    # Find all source() calls
    source_pattern = r"source\(['\"](\w+)['\"],\s*['\"](\w+)['\"]\)"
    sources = re.findall(source_pattern, generated_sql)

    # Build set of actually used inputs from the final model
    actually_used = set()
    for ref_name in refs:
        actually_used.add(f"ref('{ref_name}')")
    for source_name, table_name in sources:
        actually_used.add(f"source('{source_name}', '{table_name}')")

    # Filter given to keep only actually used inputs
    clean_given = []
    for given in target_test.get("given", []):
        if "input" in given:
            input_ref = given["input"]
            if input_ref in actually_used:
                clean_given.append(given)

    target_test["given"] = clean_given
    existing_inputs = set()
    for given in target_test.get("given", []):
        existing_inputs.add(given.get("input", ""))

    # Auto-add missing ref inputs as empty fixtures
    for ref_name in refs:
        ref_input = f"ref('{ref_name}')"
        if ref_input not in existing_inputs:
            logger.debug(f"Auto-adding empty fixture: {ref_input}")
            target_test["given"].append({"input": ref_input, "rows": []})
            existing_inputs.add(ref_input)

    # Auto-add missing source inputs as empty fixtures
    for source_name, table_name in sources:
        source_input = f"source('{source_name}', '{table_name}')"
        if source_input not in existing_inputs:
            logger.debug(f"Auto-adding empty fixture: {source_input}")
            target_test["given"].append({"input": source_input, "rows": []})
            existing_inputs.add(source_input)

    # Modify: update model, and ensure generated test is enabled and schema-clean
    target_test["model"] = generated_model
    # Drop any config (enabled/cte_test) from the generated test to avoid skips and non-standard fields
    if "config" in target_test:
        del target_test["config"]

    # Build new YAML
    output_data = {"version": 2, "unit_tests": [target_test]}

    # Write YAML with proper indentation for lists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    class IndentDumper(yaml.SafeDumper):
        def increase_indent(self, flow: bool = False, indentless: bool = False) -> int | None:  # type: ignore[override]
            return super(IndentDumper, self).increase_indent(flow, False)

    with open(output_path, "w") as f:
        yaml.dump(output_data, f, Dumper=IndentDumper, default_flow_style=False, sort_keys=False, width=120, indent=2)

    logger.debug(f"Generated CTE test: {output_path}")
    return True


def _load_project_config(project_dir: Path) -> dict[str, Any]:
    """Load dbt_project.yml configuration.

    Args:
        project_dir: Path to dbt project root

    Returns:
        Dict with project configuration
    """
    project_file = project_dir / "dbt_project.yml"
    if not project_file.exists():
        logger.warning(f"dbt_project.yml not found at {project_file}, using defaults")
        return {}

    with open(project_file) as f:
        config = yaml.safe_load(f)

    return config or {}


def generate_cte_tests(project_dir: Path) -> int:
    """Scan project and generate all CTE tests.

    Args:
        project_dir: Path to dbt project root

    Returns:
        Number of CTE tests generated
    """
    logger.info("Generating CTE tests...")

    # Load project configuration
    config = _load_project_config(project_dir)

    # Get configured paths (use first element if multiple)
    test_paths = config.get("test-paths", ["tests"])
    model_paths = config.get("model-paths", ["models"])

    # For unit tests, check both test-paths and unit_tests directory
    # (unit_tests is a common convention even if not in test-paths)
    unit_tests_dirs = []
    for test_path in test_paths:
        unit_tests_dirs.append(project_dir / test_path)
    # Also check for unit_tests directory as fallback
    if (project_dir / "unit_tests").exists():
        unit_tests_dirs.append(project_dir / "unit_tests")

    # Use first model path for generated models
    models_dir = project_dir / model_paths[0]

    # Output directories (generated files go in first model path and preferred test path)
    gen_models_dir = project_dir / model_paths[0] / "marts" / "__cte_tests"

    # Determine output test directory:
    # - If unit_tests exists, use it (common convention)
    # - Otherwise use first configured test path
    if (project_dir / "unit_tests").exists():
        gen_tests_dir = project_dir / "unit_tests" / "marts" / "__cte_tests"
    else:
        gen_tests_dir = project_dir / test_paths[0] / "marts" / "__cte_tests"

    # Clean up old generated files
    if gen_models_dir.exists():
        shutil.rmtree(gen_models_dir)
        logger.debug(f"Cleaned up {gen_models_dir}")
    if gen_tests_dir.exists():
        shutil.rmtree(gen_tests_dir)
        logger.debug(f"Cleaned up {gen_tests_dir}")

    # Discover all unit test YAML files from all test directories
    test_files = []
    for unit_tests_dir in unit_tests_dirs:
        if unit_tests_dir.exists():
            test_files.extend(list(unit_tests_dir.rglob("*_unit_tests.yml")))
    logger.debug(f"Found {len(test_files)} unit test files")

    cte_tests_found = 0

    # Process each test file
    for test_file in test_files:
        # Read test YAML
        with open(test_file) as f:
            test_yaml = yaml.safe_load(f)

        # Find CTE tests (marked with cte_test: true config)
        for test in test_yaml.get("unit_tests", []):
            config = test.get("config", {})
            if config.get("cte_test") is True:
                cte_tests_found += 1
                test_name = test["name"]
                model_spec = test["model"]
                base_model, cte_name = model_spec.split("::")

                logger.debug(f"Found CTE test: {test_name} (model: {base_model}, CTE: {cte_name})")

                # Generate short hash from test name for unique filenames
                test_hash = hashlib.md5(test_name.encode()).hexdigest()[:6]

                # Determine model file path from test file structure
                # Find which test directory this file belongs to
                test_dir = None
                for candidate_dir in unit_tests_dirs:
                    try:
                        # Check if test_file is relative to this directory
                        relative_path = test_file.relative_to(candidate_dir)
                        test_dir = candidate_dir
                        break
                    except ValueError:
                        # Not relative to this directory, try next
                        continue

                if not test_dir:
                    logger.warning(f"Could not determine test directory for {test_file}")
                    continue

                # Assume tests mirror model structure: <test_dir>/marts/X.yml -> models/marts/X.sql
                relative_path = test_file.relative_to(test_dir)
                model_subdir = relative_path.parent
                model_file = models_dir / model_subdir / f"{base_model}.sql"

                if not model_file.exists():
                    logger.warning(f"Model file not found: {model_file}")
                    continue

                # Generate model name with hash suffix
                gen_model_name = f"{base_model}__{cte_name}__{test_hash}"
                gen_model_path = gen_models_dir / f"{gen_model_name}.sql"
                gen_test_path = gen_tests_dir / f"{gen_model_name}_unit_tests.yml"

                # Generate files
                if generate_cte_model(model_file, cte_name, test.get("given", []), gen_model_path):
                    if generate_cte_test(test_file, test_name, base_model, cte_name, gen_model_name, gen_model_path, gen_test_path):
                        logger.debug(f"Generated CTE test: {gen_model_name}")
                    else:
                        logger.error(f"Failed to generate CTE test YAML for {test_name}")
                else:
                    logger.error(f"Failed to generate CTE model for {test_name}")

    if cte_tests_found > 0:
        logger.info(f"Generated {cte_tests_found} CTE test(s)")
    else:
        logger.debug("No CTE tests found (tests with config: cte_test: true)")

    return cte_tests_found
