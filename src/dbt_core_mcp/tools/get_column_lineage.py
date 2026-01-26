"""Get column-level lineage through SQL transformations.

This module implements the get_column_lineage tool for dbt Core MCP.
Uses sqlglot to parse SQL and trace column dependencies through CTEs and transformations.

Architecture:
- Unified SQL parsing approach for both upstream and downstream directions
- Two-stage process: (1) resolve output columns, (2) analyze lineage per column
- Direction-agnostic helpers: _prepare_model_analysis, _analyze_column_lineage
- Consistent fallback order: SQL-derived → warehouse → manifest → none
"""

import logging
from typing import Any

from fastmcp.dependencies import Depends  # type: ignore[reportAttributeAccessIssue]
from fastmcp.server.context import Context
from sqlglot import exp, parse_one
from sqlglot.errors import SqlglotError
from sqlglot.lineage import lineage
from sqlglot.optimizer.scope import build_scope

from ..context import DbtCoreServerContext
from ..dbt.manifest import ManifestLoader
from ..dependencies import get_state
from . import dbtTool

logger = logging.getLogger(__name__)


def _wrap_final_select(sql: str, column_name: str, dialect: str = "databricks") -> exp.Expression:
    """Wrap the final SELECT in a CTE to enable lineage tracing through SELECT *.

    Transforms:
        SELECT * FROM final
    Into:
        WITH __lineage_final__ AS (SELECT * FROM final)
        SELECT column_name FROM __lineage_final__

    This allows lineage() to trace specific columns through schemaless queries.

    Args:
        sql: SQL query to wrap
        column_name: Column to trace
        dialect: SQL dialect

    Returns:
        Modified AST with wrapped final SELECT
    """
    ast = parse_one(sql, dialect=dialect)

    # Get the root SELECT
    root_select = ast if isinstance(ast, exp.Select) else ast.find(exp.Select)

    if not root_select:
        return ast

    # Save the WITH clause before clearing
    with_clause = root_select.args.get("with_")

    # Copy entire root SELECT and strip its WITH
    wrapper_select = root_select.copy()
    wrapper_select.set("with_", None)

    wrapper_cte = exp.CTE(this=wrapper_select, alias=exp.TableAlias(this=exp.Identifier(this="__lineage_final__")))

    # Clear ALL args from root
    for key in list(root_select.args.keys()):
        root_select.set(key, None)

    # Add wrapper to with_clause or create new one
    if with_clause:
        with_clause.expressions.append(wrapper_cte)
    else:
        with_clause = exp.With(expressions=[wrapper_cte])

    # Rebuild root SELECT with only what we need
    root_select.set("with_", with_clause)
    root_select.set("expressions", [exp.Column(this=exp.Identifier(this=column_name))])
    root_select.set("from_", exp.From(this=exp.Table(this=exp.Identifier(this="__lineage_final__"))))

    return ast


# Export for testing
__all__ = [
    "implementation",
    "get_column_lineage",
    "_prepare_model_analysis",
    "_analyze_column_lineage",
    "_build_schema_mapping",
    "_resolve_output_columns",
    "_resolve_wildcard_column_in_table",
    "_resolve_unresolved_table_reference",
    "_extract_dependencies_from_lineage",
    "_format_lineage_response",
    "_map_dbt_adapter_to_sqlglot_dialect",
]


def _map_dbt_adapter_to_sqlglot_dialect(adapter_type: str) -> str:
    """Map dbt adapter type to sqlglot dialect.

    Args:
        adapter_type: The dbt adapter type from manifest metadata

    Returns:
        sqlglot dialect name
    """
    # Direct matches between dbt adapter and sqlglot dialect
    ADAPTER_TO_DIALECT = {
        "athena": "athena",
        "bigquery": "bigquery",
        "clickhouse": "clickhouse",
        "databricks": "databricks",
        "doris": "doris",
        "dremio": "dremio",
        "duckdb": "duckdb",
        "fabric": "fabric",
        "hive": "hive",
        "materialize": "materialize",
        "mysql": "mysql",
        "oracle": "oracle",
        "postgres": "postgres",
        "postgresql": "postgres",  # Some adapters use postgresql
        "redshift": "redshift",
        "risingwave": "risingwave",
        "singlestore": "singlestore",
        "snowflake": "snowflake",
        "spark": "spark",
        "sqlite": "sqlite",
        "starrocks": "starrocks",
        "teradata": "teradata",
        "trino": "trino",
        # Adapters that need dialect mapping
        "synapse": "tsql",  # Azure Synapse uses T-SQL
        "sqlserver": "tsql",  # SQL Server uses T-SQL
        "glue": "spark",  # AWS Glue uses Spark
        "fabricspark": "spark",  # Fabric Lakehouse uses Spark
    }

    adapter_lower = adapter_type.lower()
    dialect = ADAPTER_TO_DIALECT.get(adapter_lower)

    if dialect:
        logger.debug(f"Mapped dbt adapter '{adapter_type}' to sqlglot dialect '{dialect}'")
        return dialect

    # Fallback: use adapter type as-is (might work for some cases)
    logger.warning(f"No explicit mapping for dbt adapter '{adapter_type}', using as-is for sqlglot")
    return adapter_lower


# ========== Unified Helpers (Direction-Agnostic) ==========
# These helpers are used by both upstream and downstream lineage analysis,
# ensuring consistent behavior regardless of direction:
#
# 1. _prepare_model_analysis(): Gets resource_info + compiled_sql + schema_mapping
# 2. _analyze_column_lineage(): Runs sqlglot lineage with consistent error handling
# 3. _resolve_output_columns(): Resolves output columns with fallback order
#
# This unified approach ensures:
# - Same SQL parsing logic for both directions
# - Consistent error handling and logging
# - No code duplication
# - Easier to maintain and test


def _build_schema_mapping(manifest: ManifestLoader, upstream_lineage: dict[str, Any]) -> dict[str, Any]:
    """Build schema mapping from upstream models for sqlglot.

    Args:
        manifest: ManifestLoader instance
        upstream_lineage: Upstream lineage dict from manifest.get_lineage()

    Returns:
        Schema mapping in format: {database: {schema: {table: {column: type}}}}
    """
    schema_mapping: dict[str, Any] = {}

    if "upstream" not in upstream_lineage:
        return schema_mapping

    for upstream_node in upstream_lineage["upstream"]:
        try:
            # Use node name, not unique_id (get_resource_info expects name)
            node_name = upstream_node.get("name")
            unique_id = upstream_node.get("unique_id")
            if not node_name or not unique_id:
                continue

            # Extract resource type from unique_id (e.g. "seed.project.name" -> "seed")
            resource_type = unique_id.split(".", 1)[0] if "." in unique_id else "model"

            node_info = manifest.get_resource_info(node_name, resource_type=resource_type, include_database_schema=True, include_compiled_sql=False)

            database = node_info.get("database", "").lower()
            schema = node_info.get("schema", "").lower()
            # For sources, use identifier; for models/seeds, use alias or name
            table = node_info.get("identifier") or node_info.get("alias") or node_info.get("name", "").lower()

            logger.info(f"[DEBUG] Processing {unique_id}: db={database}, schema={schema}, table={table} (identifier={node_info.get('identifier')}, alias={node_info.get('alias')}, name={node_info.get('name')})")

            if database and schema and table:
                # Add columns with their types
                columns = node_info.get("database_columns", [])
                logger.info(f"[DEBUG]   database_columns: {columns if columns else 'EMPTY'}")

                if not columns:
                    manifest_columns = node_info.get("columns", {})
                    logger.info(f"[DEBUG]   manifest columns keys: {list(manifest_columns.keys()) if manifest_columns else 'EMPTY'}")
                    if manifest_columns:
                        columns = {col_name: {"type": (col_info.get("data_type") or col_info.get("type") or "string")} for col_name, col_info in manifest_columns.items()}
                        logger.info(f"[DEBUG]   converted to: {columns}")

                if not columns:
                    logger.warning(f"[DEBUG]   SKIPPING {unique_id} - no columns found!")
                    continue

                # Handle both list format (from database) and dict format (from manifest)
                if isinstance(columns, list):
                    # List format: [{"col_name": "customer_id", "type": "INTEGER"}]
                    column_map = {col_info.get("col_name", "").lower(): col_info.get("type", "string").lower() for col_info in columns}
                else:
                    # Dict format: {"customer_id": {"type": "INTEGER"}}
                    column_map = {col_name.lower(): col_info.get("type", "string").lower() for col_name, col_info in columns.items()}

                if not column_map:
                    continue

                if database not in schema_mapping:
                    schema_mapping[database] = {}
                if schema not in schema_mapping[database]:
                    schema_mapping[database][schema] = {}

                schema_mapping[database][schema][table] = column_map
        except Exception as e:
            logger.warning(f"Could not load schema for upstream node {upstream_node.get('unique_id')}: {e}")
            continue

    return schema_mapping


def _find_table_columns(schema_mapping: dict[str, Any], table_name: str) -> list[str]:
    """Find column names for a table in the schema mapping.

    Args:
        schema_mapping: Schema mapping in format {db: {schema: {table: {column: type}}}}
        table_name: Table name to look up

    Returns:
        List of column names for the table
    """
    table_lower = table_name.lower()
    for database_mapping in schema_mapping.values():
        for schema_mapping_for_db in database_mapping.values():
            if table_lower in schema_mapping_for_db:
                return list(schema_mapping_for_db[table_lower].keys())
    return []


def _normalize_relation_name(value: str) -> str:
    """Normalize relation names for matching."""
    return value.replace('"', "").replace("`", "").replace("[", "").replace("]", "").strip().lower()


def _add_relation_keys(lookup: dict[str, str], database: str | None, schema: str | None, identifier: str | None, unique_id: str) -> None:
    if not identifier:
        return

    if database and schema:
        lookup[_normalize_relation_name(f"{database}.{schema}.{identifier}")] = unique_id

    if schema:
        lookup[_normalize_relation_name(f"{schema}.{identifier}")] = unique_id

    lookup[_normalize_relation_name(identifier)] = unique_id


def _build_relation_lookup(manifest: ManifestLoader) -> dict[str, str]:
    """Build relation name -> unique_id lookup from the manifest."""
    lookup: dict[str, str] = {}
    manifest_dict = manifest.get_manifest_dict()

    for node in manifest_dict.get("nodes", {}).values():
        if not isinstance(node, dict):
            continue

        unique_id = node.get("unique_id")
        if not unique_id:
            continue

        relation_name = node.get("relation_name")
        if relation_name:
            lookup[_normalize_relation_name(relation_name)] = unique_id

        database = node.get("database")
        schema = node.get("schema")
        identifier = node.get("alias") or node.get("name")
        _add_relation_keys(lookup, database, schema, identifier, unique_id)

    for source in manifest_dict.get("sources", {}).values():
        if not isinstance(source, dict):
            continue

        unique_id = source.get("unique_id")
        if not unique_id:
            continue

        relation_name = source.get("relation_name")
        if relation_name:
            lookup[_normalize_relation_name(relation_name)] = unique_id

        database = source.get("database")
        schema = source.get("schema")
        identifier = source.get("identifier") or source.get("name")
        _add_relation_keys(lookup, database, schema, identifier, unique_id)

    return lookup


def _get_output_columns_from_sql(compiled_sql: str, schema_mapping: dict[str, Any], dialect: str = "databricks") -> list[str]:
    """Extract output column names from compiled SQL.

    Handles SELECT * from a single CTE or table by expanding from schema mapping
    or the CTE's projections.

    Args:
        compiled_sql: Compiled SQL string
        schema_mapping: Schema mapping for table expansion
        dialect: SQL dialect for parsing (default: databricks)

    Returns:
        List of output column names
    """
    try:
        ast = parse_one(compiled_sql, dialect=dialect)
    except Exception:
        return []

    root_scope = build_scope(ast)
    if not root_scope:
        return []

    select = root_scope.expression if isinstance(root_scope.expression, exp.Select) else root_scope.expression.find(exp.Select)
    if not select:
        return []

    projections = list(select.expressions)
    if projections and all(isinstance(p, exp.Star) for p in projections):
        if len(root_scope.selected_sources) == 1:
            _, (_, source) = next(iter(root_scope.selected_sources.items()))
            if isinstance(source, exp.Table):
                return _find_table_columns(schema_mapping, source.name)

            # CTE or subquery scope
            if hasattr(source, "expression"):
                cte_select = source.expression if isinstance(source.expression, exp.Select) else source.expression.find(exp.Select)
                if cte_select:
                    return [proj.alias_or_name for proj in cte_select.expressions if proj.alias_or_name]
        return []

    return [proj.alias_or_name for proj in projections if proj.alias_or_name]


def _resolve_output_columns(
    compiled_sql: str,
    schema_mapping: dict[str, Any],
    resource_info: dict[str, Any],
    dialect: str = "databricks",
) -> tuple[dict[str, Any], str]:
    """Resolve output columns for a model using resource-specific strategies.

    For Sources & Seeds (Database Tables):
    1) Warehouse columns (database_columns) - database is truth
    2) Manifest columns (schema.yml) - fallback
    3) Wildcard "*" - table not built yet, but table reference known

    For Models (SQL Transformations):
    1) SQL-derived projections ONLY - compiled SQL is truth
       (No fallbacks - if SQL parsing fails, no columns available)

    Args:
        compiled_sql: Compiled SQL string
        schema_mapping: Schema mapping for table expansion
        resource_info: Resource information dict
        dialect: SQL dialect for parsing (default: databricks)

    Returns:
        (output_columns_dict, source_label)
    """
    output_columns_dict: dict[str, Any] = {}

    # For sources AND seeds: database-first with wildcard fallback
    if resource_info.get("resource_type") in ["source", "seed"]:
        database_columns = resource_info.get("database_columns", [])
        if isinstance(database_columns, list) and database_columns:
            output_columns_dict = {col.get("col_name", ""): {} for col in database_columns if col.get("col_name")}
            if output_columns_dict:
                return output_columns_dict, "warehouse"
        elif isinstance(database_columns, dict) and database_columns:
            return {col_name: {} for col_name in database_columns.keys()}, "warehouse"

        # Fallback to manifest columns
        manifest_columns = resource_info.get("columns", {})
        output_columns_dict = {col_name: {} for col_name in manifest_columns.keys()}
        if output_columns_dict:
            return output_columns_dict, "manifest"

        # Final fallback: table not built yet, return wildcard
        return {"*": {}}, "wildcard"

    # For models/seeds: SQL parsing ONLY (compiled SQL is truth)
    output_columns = _get_output_columns_from_sql(compiled_sql, schema_mapping, dialect)
    if output_columns:
        return {col: {} for col in output_columns}, "sql"

    return {}, "none"


def _resolve_wildcard_column_in_table(table_name: str, schema_mapping: dict[str, Any]) -> str | None:
    """Resolve wildcard (*) column to specific column name within a known table.

    When we have a resolved table but wildcard column (e.g., "column": "*", "table": "stg_customers"),
    this function attempts to find the specific column being traced in that table.

    Args:
        table_name: Name of the table (without quotes)
        schema_mapping: Schema mapping with table and column information

    Returns:
        Resolved column name if found, None otherwise
    """
    if table_name not in schema_mapping:
        return None

    table_info = schema_mapping[table_name]
    resource_type = table_info.get("resource_type", "")

    # For sources and seeds: use database-first approach
    if resource_type in ["source", "seed"]:
        # Check database columns first
        database_columns = table_info.get("database_columns", [])
        if isinstance(database_columns, list) and database_columns:
            # Return first database column as representative
            first_col = database_columns[0]
            if isinstance(first_col, dict):
                return first_col.get("col_name")
        elif isinstance(database_columns, dict) and database_columns:
            # Return first key from database columns
            return next(iter(database_columns.keys()))

        # Fallback to manifest columns
        manifest_columns = table_info.get("columns", {})
        if manifest_columns:
            return next(iter(manifest_columns.keys()))

    # For models: would use SQL parsing (handled elsewhere)
    return None


def _resolve_unresolved_table_reference(col_name: str, schema_mapping: dict[str, Any]) -> dict[str, Any] | None:
    """Resolve unresolved table references using schema mapping and database-first approach.

    When sqlglot can't resolve a table reference (returns None), this function
    attempts to find which table in the schema mapping contains the requested column,
    using database-first resolution for sources and seeds.

    Args:
        col_name: Column name to search for
        schema_mapping: Schema mapping with table and column information

    Returns:
        Dictionary with resolved table info if found, None otherwise:
        {
            "table_name": str,
            "database": str | None,
            "schema": str | None,
            "resolved_column": str | None  # If resolved from wildcard
        }
    """
    for table_name, table_info in schema_mapping.items():
        resource_type = table_info.get("resource_type", "")

        # For sources and seeds: use database-first approach
        if resource_type in ["source", "seed"]:
            # Check database columns first
            database_columns = table_info.get("database_columns", [])
            if isinstance(database_columns, list):
                for db_col in database_columns:
                    if isinstance(db_col, dict) and db_col.get("col_name", "").lower() == col_name.lower():
                        return {"table_name": f'"{table_name}"', "database": table_info.get("database"), "schema": table_info.get("schema"), "resolved_column": db_col.get("col_name")}
            elif isinstance(database_columns, dict):
                if col_name.lower() in [k.lower() for k in database_columns.keys()]:
                    # Find the actual key with proper case
                    actual_col = next(k for k in database_columns.keys() if k.lower() == col_name.lower())
                    return {"table_name": f'"{table_name}"', "database": table_info.get("database"), "schema": table_info.get("schema"), "resolved_column": actual_col}

            # Fallback to manifest columns for sources/seeds
            manifest_columns = table_info.get("columns", {})
            if col_name.lower() in [k.lower() for k in manifest_columns.keys()]:
                actual_col = next(k for k in manifest_columns.keys() if k.lower() == col_name.lower())
                return {"table_name": f'"{table_name}"', "database": table_info.get("database"), "schema": table_info.get("schema"), "resolved_column": actual_col}

    return None


def _extract_dependencies_from_lineage(
    lineage_node: Any,
    manifest: ManifestLoader | None,
    schema_mapping: dict[str, Any],
    depth: int | None,
) -> list[dict[str, Any]]:
    """Extract column dependencies from sqlglot lineage node.

    Args:
        lineage_node: Result from sqlglot.lineage()
        manifest: Optional ManifestLoader for dbt resource lookup
        schema_mapping: Schema mapping for table and column resolution
        depth: Maximum depth to traverse

    Returns:
        List of dependency dicts with column, table, optional dbt_resource,
        and internal CTE transformation path (via_ctes, transformations)
    """
    dependencies: list[dict[str, Any]] = []

    # DEBUGGING: Let's understand the lineage node structure first
    logger.info(f"[DEBUG] Root lineage node: name='{lineage_node.name}'")
    logger.info(f"[DEBUG] Root node source_name: '{lineage_node.source_name}'")
    logger.info(f"[DEBUG] Root node has {len(lineage_node.downstream)} downstream nodes")

    # DEBUGGING: Check schema mapping
    logger.info(f"[DEBUG] Schema mapping has {len(schema_mapping)} entries")
    for table_name, table_schema in schema_mapping.items():
        logger.info(f"[DEBUG] Schema mapping table: {table_name}")
        if isinstance(table_schema, dict):
            logger.info(f"[DEBUG]   Columns: {list(table_schema.keys())}")
        else:
            logger.info(f"[DEBUG]   Type: {type(table_schema)}")

    # Walk through ALL nodes in the lineage tree using .walk()
    all_nodes = list(lineage_node.walk())
    logger.info(f"[DEBUG] Total nodes in lineage tree: {len(all_nodes)}")

    for i, node in enumerate(all_nodes):
        logger.info(f"[DEBUG] Node {i}: name='{node.name}', source_name='{node.source_name}'")
        if hasattr(node.source, "this"):
            logger.info(f"[DEBUG] Node {i} source.this: '{node.source.this}'")
        logger.info(f"[DEBUG] Node {i} source type: {type(node.source).__name__}")
        # Check if this is a table reference
        if type(node.source).__name__ == "Table":
            logger.info(f"[DEBUG] FOUND TABLE: Node {i} is a table reference!")
        elif type(node.source).__name__ == "Placeholder":
            logger.info(f"[DEBUG] PLACEHOLDER: Node {i} is unresolved - schema mapping issue?")

    def walk_dependencies(node: Any, depth_current: int = 0) -> None:
        """Recursively walk lineage tree."""
        if depth is not None and depth_current >= depth:
            return

        for dep in node.walk():
            # Check if this is a table reference (external dependency)
            if hasattr(dep, "source") and hasattr(dep.source, "this"):
                table_name = str(dep.source.this)
                col_name = dep.name

                logger.info(f"[DEBUG] Dependency: col='{col_name}', table_name='{table_name}', source={type(dep.source).__name__}")

                # For Placeholder objects, the table name is unknown
                if type(dep.source).__name__ == "Placeholder" and table_name == "None":
                    logger.info(f"[DEBUG] Placeholder object with unknown table for column: {col_name}")
                    # Keep None as table name for now

                # Skip our artificial wrapper CTE
                if table_name == "__lineage_final__":
                    continue

                # # Try to resolve unresolved table references using schema mapping
                # if table_name == "None" and schema_mapping:
                #     # Attempt to resolve using database-first approach for sources/seeds
                #     resolved_table = _resolve_unresolved_table_reference(col_name, schema_mapping)
                #     if resolved_table:
                #         table_name = resolved_table["table_name"]
                #         db = resolved_table.get("database")
                #         schema_name = resolved_table.get("schema")
                #         # Update column name if resolved from wildcard
                #         if resolved_table.get("resolved_column"):
                #             col_name = resolved_table["resolved_column"]
                #     else:
                #         # Keep original values if resolution fails
                #         db = getattr(dep.source, "catalog", None)
                #         schema_name = getattr(dep.source, "db", None)
                # # Handle wildcard column resolution for resolved tables
                # elif col_name == "*" and table_name != "None" and schema_mapping:
                #     # Look for the target column in the resolved table
                #     resolved_column = _resolve_wildcard_column_in_table(table_name.strip('"'), schema_mapping)
                #     if resolved_column:
                #         col_name = resolved_column
                #     db = getattr(dep.source, "catalog", None)
                #     schema_name = getattr(dep.source, "db", None)
                # else:
                #     # Try to resolve to dbt model (existing logic)
                #     db = getattr(dep.source, "catalog", None)
                #     schema_name = getattr(dep.source, "db", None)

                dependency_info: dict[str, Any] = {
                    "column": col_name,
                    "table": table_name,
                }

                # Always extract db/schema metadata if available
                db = getattr(dep.source, "catalog", None)
                schema_name = getattr(dep.source, "db", None)

                if db:
                    dependency_info["database"] = str(db)
                if schema_name:
                    dependency_info["schema"] = str(schema_name)

                # Extract CTE transformation path
                cte_path = _extract_cte_path(dep)

                # Clean wrapper CTE from paths (unwrap __lineage_final__)
                if cte_path["via_ctes"]:
                    cte_path["via_ctes"] = [c for c in cte_path["via_ctes"] if c != "__lineage_final__"]
                if cte_path["transformations"]:
                    cleaned_transforms = []
                    for t in cte_path["transformations"]:
                        if t.get("cte") == "__lineage_final__":
                            # Skip wrapper CTE transformations
                            continue
                        cleaned_transforms.append(t)
                    cte_path["transformations"] = cleaned_transforms

                # Temporarily commented out: internal CTE details
                # if cte_path["via_ctes"]:
                #     dependency_info["via_ctes"] = cte_path["via_ctes"]
                #     dependency_info["table"] = "__via_ctes__"  # CTE references without transformation details

                # if cte_path["transformations"]:
                #     dependency_info["transformations"] = cte_path["transformations"]
                #     dependency_info["table"] = "__transformations__"  # CTE transformations with expression details

                # Replace None table references for better clarity
                # if dependency_info["table"] == "None":
                #     dependency_info["table"] = "__parsing__"  # Intermediate parsing steps during SQL analysis

                # Try to find the corresponding dbt resource
                if manifest:
                    try:
                        matching_node = manifest.get_resource_node(table_name)
                        if not matching_node.get("multiple_matches"):
                            dependency_info["dbt_resource"] = matching_node.get("unique_id", "")
                    except Exception:
                        pass  # Resource lookup failed, continue with table name

                dependencies.append(dependency_info)

    walk_dependencies(lineage_node)
    return dependencies


def _extract_cte_path(lineage_node: Any) -> dict[str, Any]:
    """Extract the CTE transformation path from a lineage node.

    Sqlglot lineage nodes have names like:
    - "final.tier" (CTE reference)
    - "aggregated.order_count" (CTE reference)
    - "customer_id" (direct column, no CTE)
    - "base.customer_id" (CTE reference)

    The part before the dot is the CTE name. We walk up the lineage to collect
    all CTEs in the transformation chain.

    Args:
        lineage_node: A single dependency node from sqlglot lineage walk

    Returns:
        Dictionary with:
        - via_ctes: List of CTE names in transformation order (root to leaf)
        - transformations: List of transformation details per step
    """
    via_ctes: list[str] = []
    transformations: list[dict[str, str]] = []
    visited: set[int] = set()  # Track visited nodes by id to prevent cycles

    # Walk up the lineage chain (using downstream references)
    current = lineage_node
    max_iterations = 100  # Hard limit to prevent infinite loops
    iteration = 0

    while current is not None and iteration < max_iterations:
        iteration += 1

        # Prevent cycles by tracking visited nodes
        node_id = id(current)
        if node_id in visited:
            break
        visited.add(node_id)

        # Extract CTE name from node name (format: "cte_name.column_name" or "column_name")
        if hasattr(current, "name") and current.name:
            node_name = current.name

            # Check if this is a CTE reference (has a dot separator)
            if "." in node_name:
                parts = node_name.split(".", 1)
                cte_name = parts[0]
                column_name = parts[1] if len(parts) > 1 else node_name

                # Only add if it's not already in the list (dedup) and looks like a CTE
                # (CTEs typically don't have schema qualifiers like "database.schema.table")
                if cte_name and cte_name not in via_ctes:
                    # Skip if it looks like a database or schema qualifier
                    # (these typically have catalog/db attributes on source)
                    is_table_ref = False
                    if hasattr(current, "source"):
                        source = current.source
                        is_table_ref = hasattr(source, "catalog") or getattr(source, "db", None) is not None

                    if not is_table_ref:
                        via_ctes.append(cte_name)

                        # Capture transformation info
                        transform_info: dict[str, str] = {
                            "cte": cte_name,
                            "column": column_name,
                        }

                        # Try to get expression if available
                        if hasattr(current, "expression") and current.expression is not None:
                            expr_sql = str(current.expression)
                            # Only include if it's not just a simple column reference
                            if expr_sql and expr_sql.strip() != column_name:
                                # Limit expression length to avoid huge outputs
                                if len(expr_sql) > 200:
                                    expr_sql = expr_sql[:197] + "..."
                                transform_info["expression"] = expr_sql

                        transformations.append(transform_info)

        # Move up the lineage chain (downstream attribute points to parent)
        current = getattr(current, "downstream", None) if hasattr(current, "downstream") else None

    return {
        "via_ctes": via_ctes,
        "transformations": transformations,
    }


def _resolve_dependency_resource(
    dependency: dict[str, Any],
    relation_lookup: dict[str, str],
) -> str | None:
    table_name = dependency.get("table")
    if not table_name:
        return None

    database = dependency.get("database")
    schema = dependency.get("schema")

    if database and schema:
        key = _normalize_relation_name(f"{database}.{schema}.{table_name}")
        if key in relation_lookup:
            return relation_lookup[key]

    if schema:
        key = _normalize_relation_name(f"{schema}.{table_name}")
        if key in relation_lookup:
            return relation_lookup[key]

    key = _normalize_relation_name(table_name)
    return relation_lookup.get(key)


def _check_column_in_lineage(lineage_node: Any, source_model: str, source_column: str) -> bool:
    """Check if a source column appears in the lineage tree.

    Args:
        lineage_node: Result from sqlglot.lineage()
        source_model: Model name to look for
        source_column: Column name to look for

    Returns:
        True if the column appears in the lineage
    """
    logger.debug(f"[LINEAGE_CHECK] Looking for {source_model}.{source_column}")

    for dep in lineage_node.walk():
        if hasattr(dep, "name"):
            # dep.name can be either "column_name" or "table.column_name"
            name_parts = dep.name.lower().split(".")
            col_name = name_parts[-1]  # Get the last part (column name)
            table_name = name_parts[0] if len(name_parts) > 1 else None

            logger.debug(f"[LINEAGE_CHECK]   Checking dep.name='{dep.name}' -> col='{col_name}', table='{table_name}'")

            # Check if column matches
            if col_name == source_column.lower():
                logger.debug("[LINEAGE_CHECK]   Column matches! Checking table...")
                # If table name in dep.name, check it matches source_model
                if table_name and source_model.lower() in table_name:
                    logger.debug(f"[LINEAGE_CHECK]   ✓ Table in dep.name matches: {table_name} contains {source_model}")
                    return True
                # If no table name in dep.name, check source attribute
                elif hasattr(dep, "source") and hasattr(dep.source, "this"):
                    source_table = str(dep.source.this).strip('"').lower()
                    logger.debug(f"[LINEAGE_CHECK]   Checking dep.source.this='{source_table}'")
                    if source_model.lower() in source_table:
                        logger.debug(f"[LINEAGE_CHECK]   ✓ Source attribute matches: {source_table} contains {source_model}")
                        return True
                else:
                    logger.debug("[LINEAGE_CHECK]   ✗ Column matches but no table info found")

    logger.debug(f"[LINEAGE_CHECK] ✗ No match found for {source_model}.{source_column}")
    return False


async def _trace_downstream_column(
    manifest: ManifestLoader,
    model_name: str,
    column_name: str,
    depth: int | None,
    dialect: str = "databricks",
    current_depth: int = 0,
) -> list[dict[str, Any]]:
    """Recursively trace where a column is used downstream.

    Args:
        manifest: ManifestLoader instance
        model_name: Model name to start from
        column_name: Column name to trace
        depth: Maximum depth to traverse (None for unlimited)
        dialect: SQL dialect for parsing (default: databricks)
        current_depth: Current recursion depth

    Returns:
        List of downstream usage dictionaries
    """
    logger.info(f"[DOWNSTREAM] Tracing {model_name}.{column_name} at depth {current_depth}")

    if depth is not None and current_depth >= depth:
        return []

    results: list[dict[str, Any]] = []

    # Get downstream models (distance 1)
    try:
        lineage_data = manifest.get_lineage(model_name, resource_type="model", direction="downstream", depth=1)
    except Exception as e:
        logger.warning(f"Could not get downstream lineage for {model_name}: {e}")
        return []

    downstream_models = lineage_data.get("downstream", [])
    logger.info(f"[DOWNSTREAM] Found {len(downstream_models)} downstream models: {[m.get('name') for m in downstream_models]}")

    for downstream_model in downstream_models:
        # Only process models (skip tests, snapshots, etc.)
        if not downstream_model.get("unique_id", "").startswith("model."):
            continue

        # Skip CTE unit test helper models (auto-generated by CTE test generator)
        # Pattern: ends with __<6-char-hash> like customers_enriched__customer_agg__259035
        model_name_check = downstream_model.get("name", "")
        if model_name_check and len(model_name_check) > 8:
            # Check if ends with __<6 hex chars>
            suffix = model_name_check[-8:]
            if suffix[:2] == "__" and all(c in "0123456789abcdef" for c in suffix[2:]):
                logger.debug(f"Skipping CTE test model: {model_name_check}")
                continue

        try:
            # Get downstream model info (schema + SQL) using unified helper
            model_name_downstream = downstream_model["name"]
            downstream_info, compiled_sql, schema_mapping = _prepare_model_analysis(
                manifest,
                model_name_downstream,
            )

            logger.info(f"[DOWNSTREAM] {model_name_downstream}: has_sql={compiled_sql is not None}")

            # Resolve output columns using centralized logic
            output_columns_dict, output_source = _resolve_output_columns(compiled_sql, schema_mapping, downstream_info, dialect)
            if output_columns_dict:
                logger.info(f"[DOWNSTREAM] {model_name_downstream}: using {len(output_columns_dict)} columns from {output_source}")
            else:
                # Fallback: use string search if no column metadata available
                logger.debug(f"[DOWNSTREAM] {model_name_downstream}: No column metadata, using string search")
                source_column_ref_patterns = [
                    f"{model_name}.{column_name}",
                    f".{column_name}",
                    f" {column_name} ",
                    f" {column_name},",
                    f"({column_name}",
                ]
                sql_lower = compiled_sql.lower()
                found_reference = any(pattern.lower() in sql_lower for pattern in source_column_ref_patterns)

                if found_reference:
                    logger.info(f"[DOWNSTREAM] ✓ {model_name_downstream} references {model_name}.{column_name} (string search)")
                    results.append({"model": model_name_downstream, "column": column_name, "distance": current_depth + 1})
                    further_downstream = await _trace_downstream_column(manifest, model_name_downstream, column_name, depth, dialect, current_depth + 1)
                    results.extend(further_downstream)
                continue

            # Check each output column to see if it uses our source column
            logger.debug(f"[DOWNSTREAM] {model_name_downstream}: checking {len(output_columns_dict)} output columns")
            sql_lower = compiled_sql.lower()
            source_column_ref_patterns = [
                f"{model_name}.{column_name}",
                f".{column_name}",
                f" {column_name} ",
                f" {column_name},",
                f"({column_name}",
            ]

            for output_col_name in output_columns_dict.keys():
                try:
                    # Trace this output column's lineage using unified helper
                    column_lineage_result = _analyze_column_lineage(
                        output_col_name,
                        compiled_sql,
                        schema_mapping,
                        model_name_downstream,
                        dialect,
                    )

                    logger.debug(f"[DOWNSTREAM] Check if {output_col_name} uses {model_name}.{column_name}")

                    # Check if our source column appears in the dependencies
                    if _check_column_in_lineage(column_lineage_result, model_name, column_name):
                        logger.info(f"[DOWNSTREAM] ✓ {model_name_downstream}.{output_col_name} USES {model_name}.{column_name}")
                        # This output column uses our source column!
                        results.append({"model": model_name_downstream, "column": output_col_name, "distance": current_depth + 1})

                        # Recurse: trace this column further downstream
                        further_downstream = await _trace_downstream_column(manifest, model_name_downstream, output_col_name, depth, dialect, current_depth + 1)
                        results.extend(further_downstream)
                    else:
                        # Heuristic fallback when lineage doesn't resolve dependencies
                        if output_col_name.lower() == column_name.lower() and any(pattern.lower() in sql_lower for pattern in source_column_ref_patterns):
                            logger.info(f"[DOWNSTREAM] ✓ {model_name_downstream}.{output_col_name} USES {model_name}.{column_name} (heuristic)")
                            results.append({"model": model_name_downstream, "column": output_col_name, "distance": current_depth + 1})

                            further_downstream = await _trace_downstream_column(manifest, model_name_downstream, output_col_name, depth, dialect, current_depth + 1)
                            results.extend(further_downstream)
                        else:
                            logger.debug(f"[DOWNSTREAM] ✗ {model_name_downstream}.{output_col_name} does NOT use {model_name}.{column_name}")

                except SqlglotError as e:
                    # Heuristic fallback when sqlglot fails
                    if output_col_name.lower() == column_name.lower() and any(pattern.lower() in sql_lower for pattern in source_column_ref_patterns):
                        logger.info(f"[DOWNSTREAM] ✓ {model_name_downstream}.{output_col_name} USES {model_name}.{column_name} (heuristic)")
                        results.append({"model": model_name_downstream, "column": output_col_name, "distance": current_depth + 1})

                        further_downstream = await _trace_downstream_column(manifest, model_name_downstream, output_col_name, depth, dialect, current_depth + 1)
                        results.extend(further_downstream)
                    else:
                        logger.warning(f"Could not trace {model_name_downstream}.{output_col_name}: {e}")
                    continue

        except Exception as e:
            # Use get() to avoid UnboundLocalError if exception occurs before model_name_downstream is set
            model_name_for_log = downstream_model.get("name", "unknown")
            logger.warning(f"Error analyzing downstream model {model_name_for_log}: {e}")
            continue

    return results


def _prepare_model_analysis(
    manifest: ManifestLoader,
    model_name: str,
) -> tuple[dict[str, Any], str, dict[str, Any]]:
    """Prepare model for column lineage analysis (unified helper).

    Gets all necessary data in one call:
    - Resource info (metadata, columns)
    - Compiled SQL
    - Schema mapping for sqlglot context

    Args:
        manifest: ManifestLoader instance
        model_name: Model name to analyze

    Returns:
        Tuple of (resource_info, compiled_sql, schema_mapping)

    Raises:
        ValueError: If model not found or has no compiled SQL
    """
    # Get resource info with compiled SQL and database schema
    resource_info = manifest.get_resource_info(
        model_name,
        resource_type="model",
        include_compiled_sql=True,
        include_database_schema=True,
    )

    compiled_sql = resource_info.get("compiled_sql")
    if not compiled_sql:
        raise ValueError(f"No compiled SQL found for model '{model_name}'")

    # DEBUG: Print compiled SQL to see what table names are actually there
    logger.info(f"[DEBUG] Compiled SQL for {model_name}:")
    logger.info(f"[DEBUG] {compiled_sql}")

    # Build schema mapping from upstream models
    try:
        upstream_lineage = manifest.get_lineage(
            model_name,
            resource_type="model",
            direction="upstream",
            depth=1,
        )
        schema_mapping = _build_schema_mapping(manifest, upstream_lineage)
    except (ValueError, KeyError, AttributeError) as e:
        logger.warning(f"Could not build schema mapping for {model_name}: {e}")
        schema_mapping = {}

    return resource_info, compiled_sql, schema_mapping


def _analyze_column_lineage(
    column_name: str,
    compiled_sql: str,
    schema_mapping: dict[str, Any],
    model_name: str,
    dialect: str = "databricks",
) -> Any:
    """Run sqlglot column lineage analysis (unified helper).

    Consistent error handling for both upstream and downstream directions.

    Args:
        column_name: Column to analyze
        compiled_sql: Compiled SQL
        schema_mapping: Schema context for sqlglot
        model_name: Model name (for error messages)
        dialect: SQL dialect for parsing (default: databricks)

    Returns:
        sqlglot lineage node

    Raises:
        ValueError: If SQL parsing fails
    """
    try:
        # Wrap the SQL to enable lineage tracing through SELECT *
        wrapped_ast = _wrap_final_select(compiled_sql, column_name, dialect)

        return lineage(
            column=column_name,
            sql=wrapped_ast,
            schema=schema_mapping,
            dialect=dialect,
        )
    except SqlglotError as e:
        raise ValueError(f"Failed to parse SQL for column lineage: {e}\nModel: {model_name}, Column: {column_name}")
    except Exception as e:
        logger.exception("Unexpected error in column lineage analysis")
        raise ValueError(f"Column lineage analysis failed: {e}")


def _trace_upstream_recursive(
    manifest: ManifestLoader,
    model_name: str,
    column_name: str,
    depth: int | None,
    dialect: str = "databricks",
    current_depth: int = 0,
    relation_lookup: dict[str, str] | None = None,
    visited: set[str] | None = None,
) -> list[dict[str, Any]]:
    """Recursively trace upstream dependencies for a column.

    Uses unified SQL parsing approach (same as downstream):
    1. Prepare model (get resource_info + compiled_sql + schema_mapping)
    2. Resolve output columns (SQL → warehouse → manifest)
    3. Run lineage per resolved column
    4. Recurse on dependencies

    Args:
        manifest: ManifestLoader instance
        model_name: Model name to analyze
        column_name: Column name to trace
        depth: Maximum depth (None for unlimited)
        dialect: SQL dialect for parsing (default: databricks)
        current_depth: Current recursion depth
        relation_lookup: FQN → unique_id mapping (built once, reused)
        visited: Set of visited unique_ids (prevent cycles)

    Returns:
        List of upstream dependencies with dbt resource mapping
    """
    # Skip wildcards
    if column_name.strip() == "*":
        return []

    # Check depth limit
    if depth is not None and current_depth >= depth:
        return []

    # Initialize tracking on first call
    if relation_lookup is None:
        relation_lookup = _build_relation_lookup(manifest)

    if visited is None:
        visited = set()

    # Prepare model for analysis (unified helper - Phase 1)
    try:
        _, compiled_sql, schema_mapping = _prepare_model_analysis(
            manifest,
            model_name,
        )
    except ValueError as e:
        logger.warning(f"Could not prepare model {model_name}: {e}")
        return []

    # Run lineage analysis (unified helper - Phase 1)
    try:
        column_lineage_result = _analyze_column_lineage(
            column_name,
            compiled_sql,
            schema_mapping,
            model_name,
            dialect,
        )
    except ValueError as e:
        logger.warning(f"Could not analyze column {model_name}.{column_name}: {e}")
        return []

    # Extract dependencies and resolve to dbt resources
    dependencies = _extract_dependencies_from_lineage(
        column_lineage_result,
        manifest,
        schema_mapping,
        depth,
    )

    # DEBUG: Add compiled SQL to first dependency for inspection
    if dependencies and len(dependencies) > 0:
        dependencies[0]["debug_compiled_sql"] = compiled_sql[:500] + ("..." if len(compiled_sql) > 500 else "")

    # Resolve dependency FQNs to dbt resources
    for dependency in dependencies:
        if dependency.get("dbt_resource"):
            continue

        resolved = _resolve_dependency_resource(dependency, relation_lookup)
        if resolved:
            dependency["dbt_resource"] = resolved

    results = list(dependencies)

    # Recurse on each dependency
    for i, dependency in enumerate(dependencies):
        dbt_resource = dependency.get("dbt_resource")
        if not dbt_resource or dbt_resource in visited:
            continue

        node = manifest.get_node_by_unique_id(dbt_resource)
        if not node:
            continue

        if node.get("resource_type") != "model":
            # This is a source/seed - add it to results but don't recurse further
            results.append(dependency)
            continue

        next_model = node.get("name")
        if not next_model:
            continue

        visited.add(dbt_resource)

        # Extract column name from dependency
        dep_column = dependency.get("column", "")

        # Handle wildcards: resolve upstream model's output columns
        if dep_column.endswith(".*") or dep_column.strip() == "*":
            # Use unified helper to prepare upstream model
            try:
                upstream_info, upstream_sql, upstream_schema_mapping = _prepare_model_analysis(
                    manifest,
                    next_model,
                )
            except ValueError:
                continue

            # Resolve output columns using unified logic
            output_columns_dict, _ = _resolve_output_columns(
                upstream_sql,
                upstream_schema_mapping,
                upstream_info,
                dialect,
            )

            # Determine which column to trace through the wildcard
            # Check previous dependency for the actual column name
            if i > 0:
                prev_dep = dependencies[i - 1]
                # Try transformations first, then fall back to column field
                if prev_dep.get("transformations"):
                    column_to_trace = prev_dep["transformations"][0].get("column", column_name)
                else:
                    # Strip CTE prefix if present (e.g., "customers.customer_id" -> "customer_id")
                    prev_column = prev_dep.get("column", "")
                    column_to_trace = prev_column.split(".")[-1] if prev_column else column_name
            else:
                # No previous dependency - use original target column
                column_to_trace = column_name

            # Only trace the specific column we're looking for, not all columns
            if column_to_trace in output_columns_dict:
                results.extend(
                    _trace_upstream_recursive(
                        manifest,
                        next_model,
                        column_to_trace,
                        depth,
                        dialect,
                        current_depth + 1,
                        relation_lookup,
                        visited,
                    )
                )
            continue

        # Regular column: extract name and recurse
        next_column = dep_column.split(".")[-1] if dep_column else column_name

        results.extend(
            _trace_upstream_recursive(
                manifest,
                next_model,
                next_column,
                depth,
                dialect,
                current_depth + 1,
                relation_lookup,
                visited,
            )
        )

    return results

    return results


def _format_lineage_response(model_name: str, column_name: str, direction: str, dependencies: list[dict[str, Any]], downstream_usage: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    """Format the final lineage response.

    Args:
        model_name: Model name
        column_name: Column name
        direction: Direction of lineage
        dependencies: Upstream dependencies
        downstream_usage: Optional downstream usage info

    Returns:
        Formatted response dict
    """
    result: dict[str, Any] = {
        "model": model_name,
        "column": column_name,
        "direction": direction,
        "dependencies": dependencies,
        "dependency_count": len(dependencies),
    }

    if downstream_usage is not None:
        result["downstream_usage"] = downstream_usage
        result["downstream_count"] = len(downstream_usage)

    return result


async def implementation(
    ctx: Context | None,
    model_name: str,
    column_name: str,
    direction: str,
    depth: int | None,
    state: DbtCoreServerContext,
    force_parse: bool = True,
) -> dict[str, Any]:
    """Implementation function for get_column_lineage tool.

    Separated for testing purposes - tests call this directly with explicit state.
    The @tool() decorated get_column_lineage() function calls this with injected dependencies.
    """
    # Initialize state if needed
    await state.ensure_initialized(ctx, force_parse)

    # Verify manifest is available
    if state.manifest is None:
        raise RuntimeError("Manifest not initialized")

    # FIRST: Check if we have compiled code, compile ALL if needed
    # Test source model to see if compilation is needed
    resouce_info = state.manifest.get_resource_info(model_name, resource_type="model", include_compiled_sql=True)
    compiled_sql = resouce_info.get("compiled_sql")
    if not compiled_sql:
        logger.info("No compiled SQL found - compiling entire project")
        runner = await state.get_runner()
        # Compile ALL models (dbt compile with no selector)
        compile_result = await runner.invoke(["compile"])

        if compile_result.success:
            # Reload manifest to get compiled code
            await state.manifest.load()
            # Re-fetch the resource to get updated compiled_code
            resouce_info = state.manifest.get_resource_info(
                model_name,
                resource_type="model",
                include_database_schema=False,
                include_compiled_sql=True,
            )
            compiled_sql = resouce_info.get("compiled_sql")
        else:
            raise RuntimeError(f"Failed to compile project: {compile_result}")

        logger.info("Project compiled successfully")

    # We always need compiled SQL from this point.
    if not compiled_sql:
        raise ValueError(f"No compiled SQL found for model '{model_name}'. Model may not contain SQL code.")

    # Handle multiple matches (check AFTER compilation like get_resource_info does)
    if resouce_info.get("multiple_matches"):
        raise ValueError(f"Multiple models found matching '{model_name}'. Please use unique_id: {[m['unique_id'] for m in resouce_info['matches']]}")

    # Get SQL dialect from manifest metadata
    project_info = state.manifest.get_project_info()
    adapter_type = project_info.get("adapter_type", "databricks")
    dialect = _map_dbt_adapter_to_sqlglot_dialect(adapter_type)
    logger.info(f"Using SQL dialect '{dialect}' for adapter type '{adapter_type}'")

    # Validate that the requested column exists in the model's output
    # Build schema mapping for column resolution
    upstream_lineage = state.manifest.get_lineage(
        model_name,
        resource_type="model",
        direction="upstream",
        depth=1,
    )
    schema_mapping = _build_schema_mapping(state.manifest, upstream_lineage)

    # Resolve output columns
    output_columns, _ = _resolve_output_columns(
        compiled_sql=compiled_sql,
        schema_mapping=schema_mapping,
        resource_info=resouce_info,
        dialect=dialect,
    )

    # Check if requested column exists
    if column_name not in output_columns:
        available_columns = ", ".join(f"'{col}'" for col in sorted(output_columns.keys()))
        raise ValueError(f"Column '{column_name}' not found in output of model '{model_name}'. Available columns: {available_columns}")

    if direction == "downstream":
        # Downstream only
        downstream_usage = await _trace_downstream_column(state.manifest, model_name, column_name, depth, dialect)
        return _format_lineage_response(model_name, column_name, direction, [], downstream_usage)

    if direction == "upstream":
        # Upstream only
        dependencies = _trace_upstream_recursive(state.manifest, model_name, column_name, depth, dialect)
        return _format_lineage_response(model_name, column_name, direction, dependencies, None)

    else:  # direction == "both"
        # Both upstream and downstream
        dependencies = _trace_upstream_recursive(state.manifest, model_name, column_name, depth, dialect)
        downstream_usage = await _trace_downstream_column(state.manifest, model_name, column_name, depth, dialect)
        return _format_lineage_response(model_name, column_name, direction, dependencies, downstream_usage)


@dbtTool()
async def get_column_lineage(
    ctx: Context,
    model_name: str,
    column_name: str,
    direction: str = "upstream",
    depth: int | None = None,
    state: DbtCoreServerContext = Depends(get_state),
) -> dict[str, Any]:
    """Trace column-level lineage through SQL transformations.

    Uses sqlglot to parse compiled SQL and track how columns flow through:
    - CTEs and subqueries
    - JOINs and aggregations
    - Transformations (calculations, CASE statements, etc.)
    - Window functions

    This provides detailed column-to-column dependencies that model-level
    lineage cannot capture.

    Args:
        model_name: Name or unique_id of the dbt model to analyze
        column_name: Name of the column to trace
        direction: Direction to trace lineage:
            - "upstream": Which source columns feed into this column
            - "downstream": Which downstream columns use this column
            - "both": Full bidirectional column lineage
        depth: Maximum levels to traverse (None for unlimited)
            - depth=1: Immediate column dependencies only
            - depth=2: Dependencies + their dependencies
            - None: Full dependency tree

    Returns:
        Column lineage information including:
        - Source columns this column depends on (upstream)
        - Downstream columns that depend on this column
        - Transformations and derivations
        - CTE transformation paths (via_ctes, transformations)
        - dbt resource mapping where available

        Each dependency includes:
        - column: Column name
        - table: Source table name
        - schema: Source schema (if available)
        - database: Source database (if available)
        - via_ctes: List of CTE names in transformation order
        - transformations: Transformation details per CTE step
          - cte: CTE name
          - column: Column name at this step
          - expression: SQL expression (truncated to 200 chars)

    Raises:
        ValueError: If model not found, column not found, or SQL parse fails
        RuntimeError: If sqlglot is not installed

    Examples:
        # Find which source columns feed into revenue
        get_column_lineage("fct_sales", "revenue", "upstream")

        # See what downstream models use customer_id
        get_column_lineage("dim_customers", "customer_id", "downstream")

        # Full bidirectional lineage for a column
        get_column_lineage("fct_orders", "order_total", "both")

    Note:
        Requires sqlglot package. Install with: pip install sqlglot
        The model must be compiled (run 'dbt compile' first).
    """
    return await implementation(ctx, model_name, column_name, direction, depth, state)
