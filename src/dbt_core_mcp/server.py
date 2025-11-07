"""
DBT Core MCP Server Implementation.

This server provides tools for interacting with DBT projects via the Model Context Protocol.
"""

import json
import logging
import os
import shutil
from pathlib import Path
from typing import Any, Optional

import yaml
from fastmcp import FastMCP
from fastmcp.server.middleware.error_handling import ErrorHandlingMiddleware
from fastmcp.server.middleware.rate_limiting import RateLimitingMiddleware

from .dbt.bridge_runner import BridgeRunner
from .dbt.manifest import ManifestLoader
from .utils.env_detector import detect_dbt_adapter, detect_python_command

logger = logging.getLogger(__name__)


class DBTCoreMCPServer:
    """
    DBT Core MCP Server.

    Provides tools for interacting with DBT projects.
    """

    def __init__(self, project_dir: Optional[str] = None) -> None:
        """Initialize the server.

        Args:
            project_dir: Optional path to DBT project directory for testing.
                         If not provided, uses MCP workspace roots.

        DBT project directories will be detected from MCP workspace roots during initialization.
        """
        # FastMCP initialization with recommended arguments
        from . import __version__

        self.app = FastMCP(
            version=__version__,
            name="DBT Core MCP",
            instructions="""DBT Core MCP Server for interacting with DBT projects.

            This server provides tools to:
            - Query DBT project metadata
            - Run DBT commands
            - Inspect models, sources, and tests
            - View compiled SQL
            - Access DBT documentation

            Usage:
            - Use the tools to interact with your DBT project
            - Query model lineage and dependencies
            - Run and test DBT models
            """,
            on_duplicate_resources="warn",
            on_duplicate_prompts="replace",
            include_fastmcp_meta=True,  # Include FastMCP metadata for clients
        )

        # DBT project directories will be set from workspace roots during MCP initialization
        # or from the optional project_dir argument for testing
        self.project_dir = Path(project_dir) if project_dir else None
        self.profiles_dir = os.path.expanduser("~/.dbt")

        # Initialize DBT components (lazy-loaded)
        self.runner: BridgeRunner | None = None
        self.manifest: ManifestLoader | None = None
        self.adapter_type: str | None = None
        self._initialized: bool = False

        # Add built-in FastMCP middleware (2.11.0)
        self.app.add_middleware(ErrorHandlingMiddleware())  # Handle errors first
        self.app.add_middleware(RateLimitingMiddleware(max_requests_per_second=50))
        # TimingMiddleware and LoggingMiddleware removed - they use structlog with column alignment
        # which causes formatting issues in VS Code's output panel

        # Register tools
        self._register_tools()

        logger.info("DBT Core MCP Server initialized")
        if self.project_dir:
            logger.info(f"Project directory: {self.project_dir}")
            logger.info(f"Adapter type: {self.adapter_type}")
        else:
            logger.info("Project directory will be set from MCP workspace roots")
        logger.info(f"Profiles directory: {self.profiles_dir}")

    def _get_project_paths(self) -> dict[str, list[str]]:
        """Read configured paths from dbt_project.yml.

        Returns:
            Dictionary with path types as keys and lists of paths as values
        """
        if not self.project_dir:
            return {}

        project_file = self.project_dir / "dbt_project.yml"
        if not project_file.exists():
            return {}

        try:
            with open(project_file) as f:
                config = yaml.safe_load(f)

            return {
                "model-paths": config.get("model-paths", ["models"]),
                "seed-paths": config.get("seed-paths", ["seeds"]),
                "snapshot-paths": config.get("snapshot-paths", ["snapshots"]),
                "analysis-paths": config.get("analysis-paths", ["analyses"]),
                "macro-paths": config.get("macro-paths", ["macros"]),
                "test-paths": config.get("test-paths", ["tests"]),
            }
        except Exception as e:
            logger.warning(f"Failed to parse dbt_project.yml: {e}")
            return {}

    def _is_manifest_stale(self) -> bool:
        """Check if manifest needs regeneration by comparing timestamps.

        Returns:
            True if manifest is missing or older than any source files
        """
        if not self.project_dir or not self.runner:
            return True

        manifest_path = self.project_dir / "target" / "manifest.json"
        if not manifest_path.exists():
            logger.debug("Manifest does not exist")
            return True

        manifest_mtime = manifest_path.stat().st_mtime

        # Check dbt_project.yml
        project_file = self.project_dir / "dbt_project.yml"
        if project_file.exists() and project_file.stat().st_mtime > manifest_mtime:
            logger.debug("dbt_project.yml is newer than manifest")
            return True

        # Get configured paths from project
        project_paths = self._get_project_paths()

        # Check all configured source directories
        for path_type, paths in project_paths.items():
            for path_str in paths:
                source_dir = self.project_dir / path_str
                if source_dir.exists():
                    # Check .sql files
                    for sql_file in source_dir.rglob("*.sql"):
                        if sql_file.stat().st_mtime > manifest_mtime:
                            logger.debug(f"{path_type}: {sql_file.name} is newer than manifest")
                            return True
                    # Check .yml and .yaml files
                    for yml_file in source_dir.rglob("*.yml"):
                        if yml_file.stat().st_mtime > manifest_mtime:
                            logger.debug(f"{path_type}: {yml_file.name} is newer than manifest")
                            return True
                    for yaml_file in source_dir.rglob("*.yaml"):
                        if yaml_file.stat().st_mtime > manifest_mtime:
                            logger.debug(f"{path_type}: {yaml_file.name} is newer than manifest")
                            return True

        return False

    def _initialize_dbt_components(self, force: bool = False) -> None:
        """Initialize DBT runner and manifest loader.

        Args:
            force: If True, always re-parse. If False, only parse if stale.
        """
        if not self.project_dir:
            raise RuntimeError("Project directory not set")

        # Only initialize runner once
        if not self.runner:
            # Detect Python command for user's environment
            python_cmd = detect_python_command(self.project_dir)
            logger.info(f"Detected Python command: {python_cmd}")

            # Detect DBT adapter type
            self.adapter_type = detect_dbt_adapter(self.project_dir)
            logger.info(f"Detected adapter: {self.adapter_type}")

            # Create bridge runner
            self.runner = BridgeRunner(self.project_dir, python_cmd)

        # Check if we need to parse
        should_parse = force or self._is_manifest_stale()

        if should_parse:
            # Run parse to generate/update manifest
            logger.info("Running dbt parse to generate manifest...")
            result = self.runner.invoke(["parse"])
            if not result.success:
                error_msg = str(result.exception) if result.exception else "Unknown error"
                raise RuntimeError(f"Failed to parse DBT project: {error_msg}")

        # Initialize or reload manifest loader
        manifest_path = self.runner.get_manifest_path()
        if not self.manifest:
            self.manifest = ManifestLoader(manifest_path)
        self.manifest.load()

        self._initialized = True
        logger.info("DBT components initialized successfully")

    def _ensure_initialized(self) -> None:
        """Ensure DBT components are initialized before use."""
        if not self._initialized:
            if not self.project_dir:
                raise RuntimeError("DBT project directory not set. The MCP server requires a workspace with a dbt_project.yml file.")
            self._initialize_dbt_components()

    def _parse_run_results(self) -> dict[str, object]:
        """Parse target/run_results.json after dbt run/test/build.

        Returns:
            Dictionary with results array and metadata
        """
        if not self.project_dir:
            return {"results": [], "elapsed_time": 0}

        run_results_path = self.project_dir / "target" / "run_results.json"
        if not run_results_path.exists():
            return {"results": [], "elapsed_time": 0}

        try:
            with open(run_results_path) as f:
                data = json.load(f)

            # Simplify results for output
            simplified_results = []
            for result in data.get("results", []):
                simplified_results.append(
                    {
                        "unique_id": result.get("unique_id"),
                        "status": result.get("status"),
                        "message": result.get("message"),
                        "execution_time": result.get("execution_time"),
                        "failures": result.get("failures"),
                    }
                )

            return {
                "results": simplified_results,
                "elapsed_time": data.get("elapsed_time", 0),
            }
        except Exception as e:
            logger.warning(f"Failed to parse run_results.json: {e}")
            return {"results": [], "elapsed_time": 0}

    def _compare_model_schemas(self, model_unique_ids: list[str], state_manifest_path: Path) -> dict[str, Any]:
        """Compare schemas of models before and after run.

        Args:
            model_unique_ids: List of model unique IDs that were run
            state_manifest_path: Path to the saved state manifest.json

        Returns:
            Dictionary with schema changes per model
        """
        if not state_manifest_path.exists():
            return {}

        try:
            # Load state (before) manifest
            with open(state_manifest_path) as f:
                state_manifest = json.load(f)

            # Load current (after) manifest
            if not self.manifest:
                return {}

            current_manifest_data = self.manifest.get_manifest_dict()  # type: ignore

            schema_changes: dict[str, dict[str, object]] = {}

            for unique_id in model_unique_ids:
                # Skip non-model nodes (like tests)
                if not unique_id.startswith("model."):
                    continue

                # Get before and after column definitions
                before_node = state_manifest.get("nodes", {}).get(unique_id, {})
                after_node = current_manifest_data.get("nodes", {}).get(unique_id, {})

                before_columns = before_node.get("columns", {})
                after_columns = after_node.get("columns", {})

                # Skip if no column definitions exist (not in schema.yml)
                if not before_columns and not after_columns:
                    continue

                # Compare columns
                before_names = set(before_columns.keys())
                after_names = set(after_columns.keys())

                added = sorted(after_names - before_names)
                removed = sorted(before_names - after_names)

                # Check for type changes in common columns
                changed_types = {}
                for col in before_names & after_names:
                    before_type = before_columns[col].get("data_type")
                    after_type = after_columns[col].get("data_type")
                    if before_type != after_type and before_type is not None and after_type is not None:
                        changed_types[col] = {"from": before_type, "to": after_type}

                # Only record if there are actual changes
                if added or removed or changed_types:
                    model_name = after_node.get("name", unique_id.split(".")[-1])
                    schema_changes[model_name] = {
                        "changed": True,
                        "added_columns": added,
                        "removed_columns": removed,
                        "changed_types": changed_types,
                    }

            return schema_changes

        except Exception as e:
            logger.warning(f"Failed to compare schemas: {e}")
            return {}

    def _get_table_schema_from_db(self, model_name: str) -> list[dict[str, object]]:
        """Get full table schema from database using DESCRIBE.

        Args:
            model_name: Name of the model

        Returns:
            List of column dictionaries with details (column_name, column_type, null, etc.)
            Empty list if query fails or table doesn't exist
        """
        try:
            sql = f"DESCRIBE {{{{ ref('{model_name}') }}}}"
            result = self.runner.invoke_query(sql, limit=None)  # type: ignore

            if not result.success or not result.stdout:
                return []

            # Parse JSON output using robust regex + JSONDecoder
            import json
            import re

            json_match = re.search(r'\{\s*"show"\s*:\s*\[', result.stdout)
            if not json_match:
                return []

            decoder = json.JSONDecoder()
            data, _ = decoder.raw_decode(result.stdout, json_match.start())

            if "show" in data:
                return data["show"]  # type: ignore[no-any-return]

            return []
        except Exception as e:
            logger.warning(f"Failed to query table schema for {model_name}: {e}")
            return []

    def _get_table_columns_from_db(self, model_name: str) -> list[str]:
        """Get actual column names from database table.

        Args:
            model_name: Name of the model

        Returns:
            List of column names from the actual table
        """
        schema = self._get_table_schema_from_db(model_name)
        if not schema:
            return []

        # Extract column names from schema
        columns: list[str] = []
        for row in schema:
            # Try common column name fields
            col_name = row.get("column_name") or row.get("Field") or row.get("name") or row.get("COLUMN_NAME")
            if col_name and isinstance(col_name, str):
                columns.append(col_name)

        logger.info(f"Extracted {len(columns)} columns for {model_name}: {columns}")
        return sorted(columns)

    def _register_tools(self) -> None:
        """Register all DBT tools."""

        @self.app.tool()
        def get_project_info() -> dict[str, object]:
            """Get information about the DBT project.

            Returns:
                Dictionary with project information
            """
            self._ensure_initialized()

            # Get project info from manifest
            info = self.manifest.get_project_info()  # type: ignore
            info["project_dir"] = str(self.project_dir)
            info["profiles_dir"] = self.profiles_dir
            info["adapter_type"] = self.adapter_type
            info["status"] = "ready"

            return info

        @self.app.tool()
        def list_models() -> list[dict[str, object]]:
            """List all models in the DBT project.

            Returns:
                List of model information dictionaries
            """
            self._ensure_initialized()

            models = self.manifest.get_models()  # type: ignore
            return [
                {
                    "name": m.name,
                    "unique_id": m.unique_id,
                    "schema": m.schema,
                    "database": m.database,
                    "alias": m.alias,
                    "description": m.description,
                    "materialization": m.materialization,
                    "tags": m.tags,
                    "package_name": m.package_name,
                    "file_path": m.original_file_path,
                    "depends_on": m.depends_on,
                }
                for m in models
            ]

        @self.app.tool()
        def get_model_info(name: str, include_database_schema: bool = True) -> dict[str, object]:
            """Get detailed information about a specific DBT model.

            Returns the complete manifest node for a model, including all metadata,
            columns, configuration, dependencies, and more. Excludes raw_code to keep
            context lightweight (use file path to read SQL when needed).

            Args:
                name: The name of the model
                include_database_schema: If True (default), queries the actual database table
                    schema using DESCRIBE and adds it as 'database_columns' field. This provides
                    the actual runtime schema vs. the manifest definition.

            Returns:
                Complete model information dictionary from the manifest (without raw_code),
                optionally enriched with actual database schema
            """
            self._ensure_initialized()

            try:
                node = self.manifest.get_model_node(name)  # type: ignore
                # Remove heavy fields to keep context lightweight
                node_copy = dict(node)
                node_copy.pop("raw_code", None)
                node_copy.pop("compiled_code", None)

                # Optionally query actual database schema
                if include_database_schema:
                    schema = self._get_table_schema_from_db(name)
                    if schema:
                        node_copy["database_columns"] = schema

                return node_copy
            except ValueError as e:
                raise ValueError(f"Model not found: {e}")

        @self.app.tool()
        def list_sources() -> list[dict[str, object]]:
            """List all sources in the DBT project.

            Returns:
                List of source information dictionaries
            """
            self._ensure_initialized()

            sources = self.manifest.get_sources()  # type: ignore
            return [
                {
                    "name": s.name,
                    "unique_id": s.unique_id,
                    "source_name": s.source_name,
                    "schema": s.schema,
                    "database": s.database,
                    "identifier": s.identifier,
                    "description": s.description,
                    "tags": s.tags,
                    "package_name": s.package_name,
                }
                for s in sources
            ]

        @self.app.tool()
        def get_source_info(source_name: str, table_name: str) -> dict[str, object]:
            """Get detailed information about a specific DBT source.

            Returns the complete manifest source node, including all metadata,
            columns, freshness configuration, etc.

            Args:
                source_name: The source name (e.g., 'jaffle_shop')
                table_name: The table name within the source (e.g., 'customers')

            Returns:
                Complete source information dictionary from the manifest
            """
            self._ensure_initialized()

            try:
                source = self.manifest.get_source_node(source_name, table_name)  # type: ignore
                return source
            except ValueError as e:
                raise ValueError(f"Source not found: {e}")

        @self.app.tool()
        def get_compiled_sql(name: str, force: bool = False) -> dict[str, object]:
            """Get the compiled SQL for a specific DBT model.

            Returns the fully compiled SQL with all Jinja templating rendered
            ({{ ref() }}, {{ source() }}, etc. resolved to actual table names).

            Args:
                name: Model name (e.g., 'customers' or 'staging.stg_orders')
                force: If True, force recompilation even if already compiled

            Returns:
                Dictionary with compiled SQL and metadata
            """
            self._ensure_initialized()

            try:
                # Check if already compiled
                compiled_code = self.manifest.get_compiled_code(name)  # type: ignore

                if compiled_code and not force:
                    return {
                        "model_name": name,
                        "compiled_sql": compiled_code,
                        "status": "success",
                        "cached": True,
                    }

                # Need to compile
                logger.info(f"Compiling model: {name}")
                result = self.runner.invoke_compile(name, force=force)  # type: ignore

                if not result.success:
                    error_msg = str(result.exception) if result.exception else "Compilation failed"
                    raise RuntimeError(f"Failed to compile model '{name}': {error_msg}")

                # Reload manifest to get compiled code
                self.manifest.load()  # type: ignore
                compiled_code = self.manifest.get_compiled_code(name)  # type: ignore

                if not compiled_code:
                    raise RuntimeError(f"Model '{name}' compiled but no compiled_code found in manifest")

                return {
                    "model_name": name,
                    "compiled_sql": compiled_code,
                    "status": "success",
                    "cached": False,
                }

            except ValueError as e:
                raise ValueError(f"Model not found: {e}")
            except Exception as e:
                raise RuntimeError(f"Failed to get compiled SQL: {e}")

        @self.app.tool()
        def refresh_manifest(force: bool = True) -> dict[str, object]:
            """Refresh the DBT manifest by running dbt parse.

            Args:
                force: If True, always re-parse. If False, only parse if stale.

            Returns:
                Status of the refresh operation
            """
            if not self.project_dir:
                raise RuntimeError("DBT project directory not set")

            try:
                self._initialize_dbt_components(force=force)
                return {
                    "status": "success",
                    "message": "Manifest refreshed successfully",
                    "project_name": self.manifest.get_project_info()["project_name"] if self.manifest else None,
                    "model_count": len(self.manifest.get_models()) if self.manifest else 0,
                    "source_count": len(self.manifest.get_sources()) if self.manifest else 0,
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to refresh manifest: {str(e)}",
                }

        @self.app.tool()
        def query_database(sql: str, limit: int | None = None) -> dict[str, object]:
            """Execute a SQL query against the DBT project's database.

            Uses dbt show --inline to execute queries with full Jinja templating support.
            Supports {{ ref('model_name') }} and {{ source('source_name', 'table_name') }}.

            Args:
                sql: SQL query to execute. Supports Jinja: {{ ref('model') }}, {{ source('src', 'table') }}
                     Can be SELECT, DESCRIBE, EXPLAIN, aggregations, JOINs, etc.
                limit: Optional maximum number of rows to return. If None (default), returns all rows.
                       If specified, limits the result set to that number of rows.

            Returns:
                Query results with rows in JSON format
            """
            self._ensure_initialized()

            if not self.adapter_type:
                raise RuntimeError("Adapter type not detected")

            # Execute query using dbt show --inline
            result = self.runner.invoke_query(sql, limit)  # type: ignore

            if not result.success:
                error_msg = str(result.exception) if result.exception else "Unknown error"
                return {
                    "error": error_msg,
                    "status": "failed",
                }

            # Parse JSON output from dbt show
            import json
            import re

            output = result.stdout if hasattr(result, "stdout") else ""

            try:
                # dbt show --output json returns: {"show": [...rows...]}
                # Find the JSON object (look for {"show": pattern)
                json_match = re.search(r'\{\s*"show"\s*:\s*\[', output)
                if not json_match:
                    return {
                        "error": "No JSON output found in dbt show response",
                        "status": "failed",
                    }

                # Use JSONDecoder to parse just the first complete JSON object
                # This handles extra data after the JSON (like log lines)
                decoder = json.JSONDecoder()
                data, idx = decoder.raw_decode(output, json_match.start())

                if "show" in data:
                    return {
                        "rows": data["show"],
                        "row_count": len(data["show"]),
                        "status": "success",
                    }
                else:
                    return {
                        "error": "Unexpected JSON format from dbt show",
                        "status": "failed",
                        "data": data,
                    }

            except json.JSONDecodeError as e:
                return {
                    "status": "error",
                    "message": f"Failed to parse query results: {e}",
                    "raw_output": output[:500],
                }

        @self.app.tool()
        def run_models(
            select: str | None = None,
            exclude: str | None = None,
            modified_only: bool = False,
            modified_downstream: bool = False,
            full_refresh: bool = False,
            fail_fast: bool = False,
            check_schema_changes: bool = False,
        ) -> dict[str, object]:
            """Run DBT models (compile SQL and execute against database).

            Smart selection modes for developers:
            - modified_only: Run only models that changed since last successful run
            - modified_downstream: Run changed models + all downstream dependencies

            Manual selection (if not using smart modes):
            - select: DBT selector syntax (e.g., "customers", "tag:mart", "stg_*")
            - exclude: Exclude specific models

            Args:
                select: Manual selector (e.g., "customers", "tag:mart", "path:marts/*")
                exclude: Exclude selector (e.g., "tag:deprecated")
                modified_only: Only run models modified since last successful run
                modified_downstream: Run modified models + downstream dependencies
                full_refresh: Force full refresh of incremental models
                fail_fast: Stop execution on first failure
                check_schema_changes: Detect schema changes and recommend downstream runs

            Returns:
                Execution results with status, models run, timing info, and optional schema_changes

            Examples:
                - run_models(select="customers") - Run specific model
                - run_models(modified_only=True) - Run only what changed
                - run_models(modified_downstream=True) - Run changed + downstream
                - run_models(select="tag:mart", full_refresh=True) - Full refresh marts
                - run_models(modified_only=True, check_schema_changes=True) - Detect schema changes
            """
            self._ensure_initialized()

            # Validate: can't use both smart and manual selection
            if (modified_only or modified_downstream) and select:
                raise ValueError("Cannot use both modified_* flags and select parameter")

            # Build command args
            args = ["run"]

            # Handle smart selection
            # Use relative path since DBT runs from project_dir
            state_dir = self.project_dir / "target" / "state_last_run"  # type: ignore

            if modified_only or modified_downstream:
                if not state_dir.exists():
                    return {
                        "status": "error",
                        "message": "No previous run state found. Run without modified_* flags first to establish baseline.",
                    }

                selector = "state:modified+" if modified_downstream else "state:modified"
                # Use relative path for --state since cwd=project_dir
                args.extend(["-s", selector, "--state", "target/state_last_run"])

            # Manual selection
            elif select:
                args.extend(["-s", select])

            if exclude:
                args.extend(["--exclude", exclude])

            if full_refresh:
                args.append("--full-refresh")

            if fail_fast:
                args.append("--fail-fast")

            # Capture pre-run table columns for schema change detection
            pre_run_columns: dict[str, list[str]] = {}
            if check_schema_changes:
                # Use dbt list to get models that will be run (without actually running them)
                list_args = ["list", "--resource-type", "model", "--output", "name"]

                if modified_only or modified_downstream:
                    selector = "state:modified+" if modified_downstream else "state:modified"
                    list_args.extend(["-s", selector, "--state", "target/state_last_run"])
                elif select:
                    list_args.extend(["-s", select])

                if exclude:
                    list_args.extend(["--exclude", exclude])

                # Get list of models
                logger.info(f"Getting model list for schema change detection: {list_args}")
                list_result = self.runner.invoke(list_args)  # type: ignore

                if list_result.success and list_result.stdout:
                    # Parse model names from output (one per line with --output name)
                    for line in list_result.stdout.strip().split("\n"):
                        line = line.strip()
                        # Skip log lines, timestamps, empty lines, and JSON output
                        if (
                            not line
                            or line.startswith("{")
                            or ":" in line[:10]  # Timestamp like "07:39:44"
                            or "Running with dbt=" in line
                            or "Registered adapter:" in line
                        ):
                            continue
                        # With --output name, each line is just the model name
                        model_name = line
                        # Query pre-run columns
                        logger.info(f"Querying pre-run columns for {model_name}")
                        cols = self._get_table_columns_from_db(model_name)
                        if cols:
                            pre_run_columns[model_name] = cols
                        else:
                            # Table doesn't exist yet - mark as new
                            pre_run_columns[model_name] = []

            # Execute
            logger.info(f"Running DBT models with args: {args}")
            result = self.runner.invoke(args)  # type: ignore

            if not result.success:
                error_msg = str(result.exception) if result.exception else "Run failed"
                return {
                    "status": "error",
                    "message": error_msg,
                    "command": " ".join(args),
                }

            # Parse run_results.json for details
            run_results = self._parse_run_results()

            # Check for schema changes if requested
            schema_changes: dict[str, dict[str, list[str]]] = {}
            if check_schema_changes and pre_run_columns:
                logger.info("Detecting schema changes by comparing pre/post-run database columns")

                for model_name, old_columns in pre_run_columns.items():
                    # Query post-run columns from database
                    new_columns = self._get_table_columns_from_db(model_name)

                    if not new_columns:
                        # Model failed to build or was skipped
                        continue

                    # Compare columns
                    added = [c for c in new_columns if c not in old_columns]
                    removed = [c for c in old_columns if c not in new_columns] if old_columns else []

                    if added or removed:
                        schema_changes[model_name] = {}
                        if added:
                            schema_changes[model_name]["added"] = added
                        if removed:
                            schema_changes[model_name]["removed"] = removed

            # Save state on success for next modified run
            if result.success and self.project_dir:
                state_dir.mkdir(parents=True, exist_ok=True)
                manifest_path = self.runner.get_manifest_path()  # type: ignore
                shutil.copy(manifest_path, state_dir / "manifest.json")

            response: dict[str, object] = {
                "status": "success",
                "command": " ".join(args),
                "results": run_results.get("results", []),
                "elapsed_time": run_results.get("elapsed_time"),
            }

            if schema_changes:
                response["schema_changes"] = schema_changes
                response["recommendation"] = "Schema changes detected. Consider running downstream models with modified_downstream=True to propagate changes."

            return response

        @self.app.tool()
        def test_models(
            select: str | None = None,
            exclude: str | None = None,
            modified_only: bool = False,
            modified_downstream: bool = False,
            fail_fast: bool = False,
        ) -> dict[str, object]:
            """Run DBT tests on models and sources.

            Smart selection modes for developers:
            - modified_only: Test only models that changed since last successful run
            - modified_downstream: Test changed models + all downstream dependencies

            Manual selection (if not using smart modes):
            - select: DBT selector syntax (e.g., "customers", "tag:mart", "test_type:generic")
            - exclude: Exclude specific tests

            Args:
                select: Manual selector for tests/models to test
                exclude: Exclude selector
                modified_only: Only test models modified since last successful run
                modified_downstream: Test modified models + downstream dependencies
                fail_fast: Stop execution on first failure

            Returns:
                Test results with status and failures
            """
            self._ensure_initialized()

            # Validate: can't use both smart and manual selection
            if (modified_only or modified_downstream) and select:
                raise ValueError("Cannot use both modified_* flags and select parameter")

            # Build command args
            args = ["test"]

            # Handle smart selection
            # Use relative path since DBT runs from project_dir
            state_dir = self.project_dir / "target" / "state_last_run"  # type: ignore

            if modified_only or modified_downstream:
                if not state_dir.exists():
                    return {
                        "status": "error",
                        "message": "No previous run state found. Run without modified_* flags first to establish baseline.",
                    }

                selector = "state:modified+" if modified_downstream else "state:modified"
                # Use relative path for --state since cwd=project_dir
                args.extend(["-s", selector, "--state", "target/state_last_run"])

            # Manual selection
            elif select:
                args.extend(["-s", select])

            if exclude:
                args.extend(["--exclude", exclude])

            if fail_fast:
                args.append("--fail-fast")

            # Execute
            logger.info(f"Running DBT tests with args: {args}")
            result = self.runner.invoke(args)  # type: ignore

            if not result.success:
                error_msg = str(result.exception) if result.exception else "Tests failed"
                return {
                    "status": "error",
                    "message": error_msg,
                    "command": " ".join(args),
                }

            # Parse run_results.json for details
            run_results = self._parse_run_results()

            return {
                "status": "success",
                "command": " ".join(args),
                "results": run_results.get("results", []),
                "elapsed_time": run_results.get("elapsed_time"),
            }

        @self.app.tool()
        def build_models(
            select: str | None = None,
            exclude: str | None = None,
            modified_only: bool = False,
            modified_downstream: bool = False,
            full_refresh: bool = False,
            fail_fast: bool = False,
        ) -> dict[str, object]:
            """Run DBT build (run + test in DAG order).

            Smart selection modes for developers:
            - modified_only: Build only models that changed since last successful run
            - modified_downstream: Build changed models + all downstream dependencies

            Manual selection (if not using smart modes):
            - select: DBT selector syntax (e.g., "customers", "tag:mart", "stg_*")
            - exclude: Exclude specific models

            Args:
                select: Manual selector
                exclude: Exclude selector
                modified_only: Only build models modified since last successful run
                modified_downstream: Build modified models + downstream dependencies
                full_refresh: Force full refresh of incremental models
                fail_fast: Stop execution on first failure

            Returns:
                Build results with status, models run/tested, and timing info
            """
            self._ensure_initialized()

            # Validate: can't use both smart and manual selection
            if (modified_only or modified_downstream) and select:
                raise ValueError("Cannot use both modified_* flags and select parameter")

            # Build command args
            args = ["build"]

            # Handle smart selection
            # Use relative path since DBT runs from project_dir
            state_dir = self.project_dir / "target" / "state_last_run"  # type: ignore

            if modified_only or modified_downstream:
                if not state_dir.exists():
                    return {
                        "status": "error",
                        "message": "No previous run state found. Run without modified_* flags first to establish baseline.",
                    }

                selector = "state:modified+" if modified_downstream else "state:modified"
                # Use relative path for --state since cwd=project_dir
                args.extend(["-s", selector, "--state", "target/state_last_run"])

            # Manual selection
            elif select:
                args.extend(["-s", select])

            if exclude:
                args.extend(["--exclude", exclude])

            if full_refresh:
                args.append("--full-refresh")

            if fail_fast:
                args.append("--fail-fast")

            # Execute
            logger.info(f"Running DBT build with args: {args}")
            result = self.runner.invoke(args)  # type: ignore

            if not result.success:
                error_msg = str(result.exception) if result.exception else "Build failed"
                return {
                    "status": "error",
                    "message": error_msg,
                    "command": " ".join(args),
                }

            # Save state on success for next modified run
            if result.success and self.project_dir:
                state_dir.mkdir(parents=True, exist_ok=True)
                manifest_path = self.runner.get_manifest_path()  # type: ignore
                shutil.copy(manifest_path, state_dir / "manifest.json")

            # Parse run_results.json for details
            run_results = self._parse_run_results()

            return {
                "status": "success",
                "command": " ".join(args),
                "results": run_results.get("results", []),
                "elapsed_time": run_results.get("elapsed_time"),
            }

        @self.app.tool()
        def seed_data(
            select: str | None = None,
            exclude: str | None = None,
            modified_only: bool = False,
            modified_downstream: bool = False,
            full_refresh: bool = False,
            show: bool = False,
        ) -> dict[str, object]:
            """Load seed data (CSV files) from seeds/ directory into database tables.

            Seeds are typically used for reference data like country codes, product categories, etc.

            Smart selection modes detect changed CSV files:
            - modified_only: Load only seeds that changed since last successful run
            - modified_downstream: Load changed seeds + all downstream dependencies

            Manual selection (if not using smart modes):
            - select: DBT selector syntax (e.g., "raw_customers", "tag:lookup")
            - exclude: Exclude specific seeds

            Important: Change detection for seeds works via file hash comparison:
            - Seeds < 1 MiB: Content hash is compared (recommended)
            - Seeds >= 1 MiB: Only file path changes are detected (content changes ignored)
            For large seeds, use manual selection or run all seeds.

            Args:
                select: Manual selector for seeds
                exclude: Exclude selector
                modified_only: Only load seeds modified since last successful run
                modified_downstream: Load modified seeds + downstream dependencies
                full_refresh: Truncate and reload seed tables (default behavior)
                show: Show preview of loaded data

            Returns:
                Seed results with status and loaded seed info

            Examples:
                seed_data()  # Load all seeds
                seed_data(modified_only=True)  # Load only changed CSVs
                seed_data(select="raw_customers")  # Load specific seed
            """
            self._ensure_initialized()

            # Validate: can't use both smart and manual selection
            if (modified_only or modified_downstream) and select:
                raise ValueError("Cannot use both modified_* flags and select parameter")

            # Build command args
            args = ["seed"]

            # Handle smart selection
            state_dir = self.project_dir / "target" / "state_last_run"  # type: ignore

            if modified_only or modified_downstream:
                if not state_dir.exists():
                    return {
                        "status": "error",
                        "message": "No previous seed state found. Run without modified_* flags first to establish baseline.",
                    }

                selector = "state:modified+" if modified_downstream else "state:modified"
                args.extend(["-s", selector, "--state", "target/state_last_run"])

            # Manual selection
            elif select:
                args.extend(["-s", select])

            if exclude:
                args.extend(["--exclude", exclude])

            if full_refresh:
                args.append("--full-refresh")

            if show:
                args.append("--show")

            # Execute
            logger.info(f"Running DBT seed with args: {args}")
            result = self.runner.invoke(args)  # type: ignore

            if not result.success:
                error_msg = str(result.exception) if result.exception else "Seed failed"
                return {
                    "status": "error",
                    "message": error_msg,
                    "command": " ".join(args),
                }

            # Save state on success for next modified run
            if result.success and self.project_dir:
                state_dir.mkdir(parents=True, exist_ok=True)
                manifest_path = self.runner.get_manifest_path()  # type: ignore
                shutil.copy(manifest_path, state_dir / "manifest.json")

            # Parse run_results.json for details
            run_results = self._parse_run_results()

            return {
                "status": "success",
                "command": " ".join(args),
                "results": run_results.get("results", []),
                "elapsed_time": run_results.get("elapsed_time"),
            }

        @self.app.tool()
        def snapshot_models(
            select: str | None = None,
            exclude: str | None = None,
        ) -> dict[str, object]:
            """Execute DBT snapshots to capture slowly changing dimensions (SCD Type 2).

            Snapshots track historical changes over time by recording:
            - When records were first seen (valid_from)
            - When records changed or were deleted (valid_to)
            - The state of records at each point in time

            Unlike models and seeds, snapshots are time-based and should be run on a schedule
            (e.g., daily or hourly), not during interactive development.

            Args:
                select: DBT selector syntax (e.g., "snapshot_name", "tag:daily")
                exclude: Exclude specific snapshots

            Returns:
                Snapshot results with status and captured changes

            Examples:
                snapshot_models()  # Run all snapshots
                snapshot_models(select="customer_history")  # Run specific snapshot
                snapshot_models(select="tag:hourly")  # Run snapshots tagged 'hourly'

            Note: Snapshots do not support smart selection (modified_only/modified_downstream)
            because they are time-dependent, not change-dependent.
            """
            self._ensure_initialized()

            # Build command args
            args = ["snapshot"]

            if select:
                args.extend(["-s", select])

            if exclude:
                args.extend(["--exclude", exclude])

            # Execute
            logger.info(f"Running DBT snapshot with args: {args}")
            result = self.runner.invoke(args)  # type: ignore

            if not result.success:
                error_msg = str(result.exception) if result.exception else "Snapshot failed"
                return {
                    "status": "error",
                    "message": error_msg,
                    "command": " ".join(args),
                }

            # Parse run_results.json for details
            run_results = self._parse_run_results()

            return {
                "status": "success",
                "command": " ".join(args),
                "results": run_results.get("results", []),
                "elapsed_time": run_results.get("elapsed_time"),
            }

        logger.info("Registered DBT tools")

    def run(self) -> None:
        """Run the MCP server."""
        self.app.run(show_banner=False)


def create_server(project_dir: Optional[str] = None) -> DBTCoreMCPServer:
    """Create a new DBT Core MCP server instance.

    Args:
        project_dir: Optional path to DBT project directory for testing.
                     If not provided, uses MCP workspace roots.

    Returns:
        DBTCoreMCPServer instance
    """
    return DBTCoreMCPServer(project_dir=project_dir)
