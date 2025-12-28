"""
dbt Core MCP Server Implementation.

This server provides tools for interacting with dbt projects via the Model Context Protocol.
"""

import asyncio
import json
import logging
import os
import shutil
from pathlib import Path
from typing import Any, Optional
from urllib.parse import unquote
from urllib.request import url2pathname

import yaml
from fastmcp import FastMCP
from fastmcp.server.context import Context
from fastmcp.server.middleware.error_handling import ErrorHandlingMiddleware
from fastmcp.server.middleware.rate_limiting import RateLimitingMiddleware

from .dbt.bridge_runner import BridgeRunner
from .dbt.manifest import ManifestLoader
from .utils.env_detector import detect_python_command

logger = logging.getLogger(__name__)


class DbtCoreMcpServer:
    """
    dbt Core MCP Server.

    Provides tools for interacting with dbt projects.
    """

    def __init__(self, project_dir: Optional[str] = None, timeout: Optional[float] = None) -> None:
        """Initialize the server.

        Args:
            project_dir: Optional path to dbt project directory. If not provided,
                        automatically detects from MCP workspace roots or falls back to cwd.
            timeout: Optional timeout in seconds for dbt commands (default: None for no timeout).
        """
        # FastMCP initialization with recommended arguments
        from . import __version__

        self.app = FastMCP(
            version=__version__,
            name="dbt Core MCP",
            instructions="""dbt Core MCP Server for interacting with dbt projects.

            This server provides tools to:
            - Query dbt project metadata
            - Run dbt commands
            - Inspect models, sources, and tests
            - View compiled SQL
            - Access dbt documentation

            Usage:
            - Use the tools to interact with your dbt project
            - Query model lineage and dependencies
            - Run and test dbt models
            """,
            on_duplicate_resources="warn",
            on_duplicate_prompts="replace",
            include_fastmcp_meta=True,  # Include FastMCP metadata for clients
        )

        # Store the explicit project_dir if provided, otherwise will detect from workspace roots
        self._explicit_project_dir = Path(project_dir) if project_dir else None
        self.project_dir: Path | None = None
        self.profiles_dir = os.path.expanduser("~/.dbt")
        self.timeout = timeout

        # Initialize dbt components (lazy-loaded)
        self.runner: BridgeRunner | None = None
        self.manifest: ManifestLoader | None = None
        self.adapter_type: str | None = None

        # Concurrency control for initialization
        self._init_lock = asyncio.Lock()

        # Add built-in FastMCP middleware (2.11.0)
        self.app.add_middleware(ErrorHandlingMiddleware())  # Handle errors first
        self.app.add_middleware(RateLimitingMiddleware(max_requests_per_second=50))
        # TimingMiddleware and LoggingMiddleware removed - they use structlog with column alignment
        # which causes formatting issues in VS Code's output panel

        # Register tools
        self._register_tools()

        logger.info("dbt Core MCP Server initialized")
        logger.info(f"Profiles directory: {self.profiles_dir}")

    def _detect_project_dir(self) -> Path:
        """Detect the dbt project directory.

        Resolution order:
        1. Use explicit project_dir if provided during initialization
        2. Fall back to current working directory

        Note: Workspace roots detection happens in _detect_workspace_roots()
        which is called asynchronously from tool contexts.

        Returns:
            Path to the dbt project directory
        """
        # Use explicit project_dir if provided
        if self._explicit_project_dir:
            logger.debug(f"Using explicit project directory: {self._explicit_project_dir}")
            return self._explicit_project_dir

        # Fall back to current working directory
        cwd = Path.cwd()
        logger.info(f"Using current working directory: {cwd}")
        return cwd

    async def _detect_workspace_roots(self, ctx: Any) -> Path | None:
        """Attempt to detect workspace roots from MCP context.

        Args:
            ctx: FastMCP Context object

        Returns:
            Path to first workspace root, or None if unavailable
        """
        try:
            if isinstance(ctx, Context):
                roots = await ctx.list_roots()
                if roots:
                    # Convert file:// URL to platform-appropriate path
                    # First unquote to decode %XX sequences, then url2pathname for platform conversion
                    uri_path = roots[0].uri.path if hasattr(roots[0].uri, "path") else str(roots[0].uri)
                    if uri_path:
                        workspace_root = Path(url2pathname(unquote(uri_path)))
                        logger.info(f"Detected workspace root from MCP client: {workspace_root}")
                        return workspace_root
        except Exception as e:
            logger.debug(f"Could not access workspace roots: {e}")

        return None

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
        if not self.project_dir:
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

    async def _initialize_dbt_components(self, needs_parse: bool = True) -> None:
        """Initialize dbt runner and manifest loader.

        Args:
            needs_parse: Whether to run dbt parse. If False, assumes manifest already exists and is fresh.
        """

        if not self.project_dir:
            raise RuntimeError("Project directory not set")

        # Only initialize runner once
        if not self.runner:
            # Detect Python command for user's environment
            python_cmd = detect_python_command(self.project_dir)
            logger.info(f"Detected Python command: {python_cmd}")

            # Create bridge runner
            self.runner = BridgeRunner(self.project_dir, python_cmd, timeout=self.timeout)

        # Only parse if needed (manifest is stale or missing)
        if needs_parse:
            logger.info("Running dbt parse to generate manifest...")
            result = await self.runner.invoke(["parse"])
            if not result.success:
                error_msg = str(result.exception) if result.exception else "Unknown error"
                raise RuntimeError(f"Failed to parse dbt project: {error_msg}")
        else:
            logger.info("Skipping dbt parse - manifest is fresh")

        # Initialize or reload manifest loader
        manifest_path = self.runner.get_manifest_path()
        if not self.manifest:
            self.manifest = ManifestLoader(manifest_path)
        await self.manifest.load()

        logger.info("dbt components initialized successfully")

    async def _ensure_initialized_with_context(self, ctx: Any) -> None:
        """Ensure dbt components are initialized, with optional workspace root detection.

        Uses async lock to prevent concurrent initialization races when multiple tools
        are called simultaneously.

        Args:
            ctx: FastMCP Context for accessing workspace roots
        """
        async with self._init_lock:
            # Always check for workspace changes, even if previously initialized
            detected_workspace: Path | None = None

            if not self._explicit_project_dir:
                detected_workspace = await self._detect_workspace_roots(ctx)

            # If workspace changed, reinitialize everything
            if detected_workspace and detected_workspace != self.project_dir:
                logger.info(f"Workspace changed from {self.project_dir} to {detected_workspace}, reinitializing...")
                self.project_dir = detected_workspace
                self.runner = None
                self.manifest = None

            # Ensure project directory is set (first time or after workspace change)
            if not self.project_dir:
                if detected_workspace:
                    self.project_dir = detected_workspace
                else:
                    self.project_dir = self._detect_project_dir()
                    logger.info(f"dbt project directory: {self.project_dir}")

            if not self.project_dir:
                raise RuntimeError("dbt project directory not set. The MCP server requires a workspace with a dbt_project.yml file.")

            # Check if manifest is stale (time delta check)
            needs_parse = self._is_manifest_stale()

            # Initialize components if needed (first time or after workspace change)
            # Parse only if manifest is stale
            if not self.runner or not self.manifest or needs_parse:
                await self._initialize_dbt_components(needs_parse=needs_parse)
            else:
                # Components exist and manifest is fresh, but ensure manifest data is loaded
                # (in case this is a new process instance with existing ManifestLoader object)
                if not self.manifest.is_loaded():
                    await self.manifest.load()

    def _parse_run_results(self) -> dict[str, Any]:
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

            current_manifest_data = self.manifest.get_manifest_dict()

            schema_changes: dict[str, dict[str, Any]] = {}

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

    async def _get_table_schema_from_db(self, model_name: str, source_name: str | None = None) -> list[dict[str, Any]]:
        """Get full table schema from database using DESCRIBE.

        Args:
            model_name: Name of the model/table
            source_name: If provided, treat as source and use source() instead of ref()

        Returns:
            List of column dictionaries with details (column_name, column_type, null, etc.)
            Empty list if query fails or table doesn't exist
        """
        try:
            if source_name:
                sql = f"DESCRIBE {{{{ source('{source_name}', '{model_name}') }}}}"
            else:
                sql = f"DESCRIBE {{{{ ref('{model_name}') }}}}"
            result = await self.runner.invoke_query(sql)  # type: ignore

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

    async def _get_table_columns_from_db(self, model_name: str) -> list[str]:
        """Get actual column names from database table.

        Args:
            model_name: Name of the model

        Returns:
            List of column names from the actual table
        """
        schema = await self._get_table_schema_from_db(model_name)
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

    async def _detect_modified_models(self, selector: str, state_path: str = "target/state_last_run") -> list[str]:
        """Detect which models would be selected by dbt without running them.

        Uses `dbt list` command to perform a dry-run.

        Args:
            selector: DBT selector (e.g., "state:modified", "state:modified+")
            state_path: Path to state directory

        Returns:
            List of model names that would be selected
        """
        args = ["list", "--select", selector, "--state", state_path, "--output", "name"]
        result = await self.runner.invoke(args)  # type: ignore

        if not result.success:
            raise ValueError(f"Failed to detect modified models: {result.exception}")

        # Parse output to get model names
        models = []
        logger.info(f"dbt list output: {result.stdout[:500]}")

        for line in result.stdout.split("\n"):
            line = line.strip()
            # Skip empty lines
            if not line:
                continue
            # Skip log/info lines (start with timestamp, brackets, or contain common log keywords)
            if line.startswith("[") or line.startswith("{"):
                continue
            if ":" in line[:15]:  # Timestamp like "12:34:56"
                continue
            if any(skip in line for skip in ["Running with dbt=", "Registered adapter:", "Found", "Concurrency:", "Finished running", "Completed successfully"]):
                continue
            # Line should be a model name (e.g., "jaffle_shop.customers" or just "customers")
            models.append(line)

        logger.info(f"Detected modified models: {models}")
        return models

    async def toolImpl_get_resource_info(
        self,
        name: str,
        resource_type: str | None = None,
        include_database_schema: bool = True,
        include_compiled_sql: bool = True,
    ) -> dict[str, Any]:
        """Implementation for get_resource_info tool."""
        try:
            # Get resource info with manifest method (handles basic enrichment)
            result = self.manifest.get_resource_info(  # type: ignore
                name,
                resource_type,
                include_database_schema=False,  # We'll handle this below for database schema
                include_compiled_sql=include_compiled_sql,
            )

            # Handle multiple matches case
            if result.get("multiple_matches"):
                # Enrich each match with database schema if requested
                if include_database_schema:
                    matches = result.get("matches", [])
                    for match in matches:
                        node_type = match.get("resource_type")
                        if node_type in ("model", "seed", "snapshot", "source"):
                            resource_name = match.get("name")
                            source_name = match.get("source_name") if node_type == "source" else None
                            schema = await self._get_table_schema_from_db(resource_name, source_name)
                            if schema:
                                match["database_columns"] = schema
                return result

            # Single match - check if we need to trigger compilation
            node_type = result.get("resource_type")

            if include_compiled_sql and node_type == "model":
                # If compiled SQL requested but not available, trigger compilation
                if result.get("compiled_sql") is None and not result.get("compiled_sql_cached"):
                    logger.info(f"Compiling model: {name}")
                    compile_result = await self.runner.invoke_compile(name, force=False)  # type: ignore

                    if compile_result.success:
                        # Reload manifest to get compiled code
                        await self.manifest.load()  # type: ignore
                        # Re-fetch the resource to get updated compiled_code
                        result = self.manifest.get_resource_info(  # type: ignore
                            name,
                            resource_type,
                            include_database_schema=False,
                            include_compiled_sql=True,
                        )

            # Query database schema for applicable resource types
            if include_database_schema and node_type in ("model", "seed", "snapshot", "source"):
                resource_name = result.get("name", name)
                # For sources, pass source_name to use source() instead of ref()
                source_name = result.get("source_name") if node_type == "source" else None
                schema = await self._get_table_schema_from_db(resource_name, source_name)
                if schema:
                    result["database_columns"] = schema

            return result

        except ValueError as e:
            raise ValueError(f"Resource not found: {e}")

    async def toolImpl_list_resources(self, resource_type: str | None = None) -> list[dict[str, Any]]:
        """Implementation for list_resources tool.

        Args:
            resource_type: Optional filter (model, source, seed, snapshot, test, analysis, macro)

        Returns:
            List of resource dictionaries with consistent structure
        """
        return self.manifest.get_resources(resource_type)  # type: ignore

    async def toolImpl_get_lineage(self, name: str, resource_type: str | None = None, direction: str = "both", depth: int | None = None) -> dict[str, Any]:
        """Implementation for get_lineage tool."""
        try:
            return self.manifest.get_lineage(name, resource_type, direction, depth)  # type: ignore
        except ValueError as e:
            raise ValueError(f"Lineage error: {e}")

    async def toolImpl_analyze_impact(self, name: str, resource_type: str | None = None) -> dict[str, Any]:
        """Implementation for analyze_impact tool."""
        try:
            return self.manifest.analyze_impact(name, resource_type)  # type: ignore
        except ValueError as e:
            raise ValueError(f"Impact analysis error: {e}")

    async def toolImpl_get_project_info(self, run_debug: bool = True) -> dict[str, Any]:
        """Implementation for get_project_info tool."""
        # Get project info from manifest
        info = self.manifest.get_project_info()  # type: ignore
        info["project_dir"] = str(self.project_dir)
        info["profiles_dir"] = self.profiles_dir
        info["status"] = "ready"

        # Run full dbt debug if requested (default behavior)
        if run_debug:
            debug_result_obj = await self.runner.invoke(["debug"])  # type: ignore

            # Convert DbtRunnerResult to dictionary
            debug_result = {
                "success": debug_result_obj.success,
                "output": debug_result_obj.stdout if debug_result_obj.stdout else "",
            }

            # Parse the debug output
            diagnostics: dict[str, Any] = {
                "command_run": "dbt debug",
                "success": debug_result.get("success", False),
                "output": debug_result.get("output", ""),
            }

            # Extract connection status from output
            output = str(debug_result.get("output", ""))
            if "Connection test: [OK connection ok]" in output or "Connection test: OK" in output:
                diagnostics["connection_status"] = "ok"
            elif "Connection test: [ERROR" in output or "Connection test: FAIL" in output:
                diagnostics["connection_status"] = "failed"
            else:
                diagnostics["connection_status"] = "unknown"

            info["diagnostics"] = diagnostics

        return info

    async def toolImpl_query_database(self, sql: str, output_file: str | None = None, output_format: str = "json") -> dict[str, Any]:
        """Implementation for query_database tool."""
        # Execute query using dbt show --inline
        result = await self.runner.invoke_query(sql)  # type: ignore

        if not result.success:
            error_msg = str(result.exception) if result.exception else "Unknown error"
            response = {
                "status": "failed",
                "error": error_msg,
            }
            # Include dbt output for debugging
            if result.stdout:
                response["dbt_output"] = result.stdout
            if result.stderr:
                response["stderr"] = result.stderr
            return response

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
                    "status": "failed",
                    "error": "No JSON output found in dbt show response",
                }

            # Use JSONDecoder to parse just the first complete JSON object
            # This handles extra data after the JSON (like log lines)
            decoder = json.JSONDecoder()
            data, _ = decoder.raw_decode(output, json_match.start())

            if "show" in data:
                rows = data["show"]
                row_count = len(rows)

                # Handle different output formats
                if output_format in ("csv", "tsv"):
                    # Convert to CSV/TSV format
                    import csv
                    import io

                    delimiter = "\t" if output_format == "tsv" else ","
                    csv_buffer = io.StringIO()

                    if rows:
                        writer = csv.DictWriter(csv_buffer, fieldnames=rows[0].keys(), delimiter=delimiter)
                        writer.writeheader()
                        writer.writerows(rows)
                        csv_string = csv_buffer.getvalue()
                    else:
                        csv_string = ""

                    if output_file:
                        # Save to file
                        from pathlib import Path

                        output_path = Path(output_file)
                        output_path.parent.mkdir(parents=True, exist_ok=True)

                        with open(output_path, "w", newline="") as f:
                            f.write(csv_string)

                        # Get file size
                        file_size_bytes = output_path.stat().st_size
                        file_size_kb = file_size_bytes / 1024

                        return {
                            "status": "success",
                            "row_count": row_count,
                            "format": output_format,
                            "saved_to": str(output_path),
                            "file_size_kb": round(file_size_kb, 2),
                        }
                    else:
                        # Return CSV/TSV inline
                        return {
                            "status": "success",
                            "row_count": row_count,
                            "format": output_format,
                            output_format: csv_string,
                        }
                else:
                    # JSON format (default)
                    if output_file:
                        # Ensure directory exists
                        from pathlib import Path

                        output_path = Path(output_file)
                        output_path.parent.mkdir(parents=True, exist_ok=True)

                        # Write rows to file
                        with open(output_path, "w") as f:
                            json.dump(rows, f, indent=2)

                        # Get file size
                        file_size_bytes = output_path.stat().st_size
                        file_size_kb = file_size_bytes / 1024

                        # Return metadata with preview
                        return {
                            "status": "success",
                            "row_count": row_count,
                            "saved_to": str(output_path),
                            "file_size_kb": round(file_size_kb, 2),
                            "columns": list(rows[0].keys()) if rows else [],
                            "preview": rows[:3],  # First 3 rows as preview
                        }
                    else:
                        # Return all rows inline
                        return {
                            "status": "success",
                            "row_count": row_count,
                            "rows": rows,
                        }
            else:
                return {
                    "status": "failed",
                    "error": "Unexpected JSON format from dbt show",
                    "data": data,
                }

        except json.JSONDecodeError as e:
            return {
                "status": "error",
                "message": f"Failed to parse query results: {e}",
                "raw_output": output[:500],
            }

    async def toolImpl_run_models(
        self,
        ctx: Context | None,
        select: str | None = None,
        exclude: str | None = None,
        modified_only: bool = False,
        modified_downstream: bool = False,
        full_refresh: bool = False,
        fail_fast: bool = False,
        check_schema_changes: bool = False,
        confirm_threshold: int = 5,
    ) -> dict[str, Any]:
        """Implementation for run_models tool."""
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

            # Detect which models will be run
            try:
                models_to_run = await self._detect_modified_models(selector)
            except Exception as e:
                return {"status": "error", "message": f"Failed to detect modified models: {str(e)}"}

            # Ask for user confirmation
            if not models_to_run:
                if ctx:
                    await ctx.info("No modified models detected")
                return {"status": "success", "message": "No modified models to run", "results": [], "elapsed_time": 0}

            # Ask for user confirmation only if above threshold
            if len(models_to_run) >= confirm_threshold and ctx:
                model_list = "\n".join(f"- {m}" for m in models_to_run)
                message = f"""The following {len(models_to_run)} model(s) have changes and will be run:

{model_list}

Do you want to proceed?"""

                result = await ctx.elicit(message=message, response_type=None)

                if result.action != "accept":
                    raise ValueError("User declined to run models")

            if ctx:
                await ctx.info(f"Running {len(models_to_run)} modified model(s)...")

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
        # Also get expected count of models for progress reporting
        pre_run_columns: dict[str, list[str]] = {}
        expected_total: int | None = None

        if check_schema_changes or True:  # Always get count for progress
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
            logger.info(f"Getting model list: {list_args}")
            list_result = await self.runner.invoke(list_args)  # type: ignore

            if list_result.success and list_result.stdout:
                model_count = 0
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
                    model_count += 1

                    # For schema change detection, query pre-run columns
                    if check_schema_changes:
                        model_name = line
                        logger.info(f"Querying pre-run columns for {model_name}")
                        cols = await self._get_table_columns_from_db(model_name)
                        if cols:
                            pre_run_columns[model_name] = cols
                        else:
                            # Table doesn't exist yet - mark as new
                            pre_run_columns[model_name] = []

                # Set expected total from model count
                if model_count > 0:
                    expected_total = model_count
                    logger.info(f"Expected total models to run: {expected_total}")

        # Execute with progress reporting
        logger.info(f"Running dbt models with args: {args}")
        logger.info(f"Expected total for progress: {expected_total}")

        # Define progress callback if context available
        async def progress_callback(current: int, total: int, message: str) -> None:
            if ctx:
                await ctx.report_progress(progress=current, total=total, message=message)

        result = await self.runner.invoke(args, progress_callback=progress_callback if ctx else None, expected_total=expected_total)  # type: ignore

        if not result.success:
            error_msg = str(result.exception) if result.exception else "Run failed"
            response = {
                "status": "error",
                "message": error_msg,
                "command": " ".join(args),
            }
            # Include dbt output for debugging
            if result.stdout:
                response["dbt_output"] = result.stdout
            if result.stderr:
                response["stderr"] = result.stderr
            return response

        # Parse run_results.json for details
        run_results = self._parse_run_results()

        # Check for schema changes if requested
        schema_changes: dict[str, dict[str, list[str]]] = {}
        if check_schema_changes and pre_run_columns:
            logger.info("Detecting schema changes by comparing pre/post-run database columns")

            for model_name, old_columns in pre_run_columns.items():
                # Query post-run columns from database
                new_columns = await self._get_table_columns_from_db(model_name)

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

        response: dict[str, Any] = {
            "status": "success",
            "command": " ".join(args),
            "results": run_results.get("results", []),
            "elapsed_time": run_results.get("elapsed_time"),
        }

        if schema_changes:
            response["schema_changes"] = schema_changes
            response["recommendation"] = "Schema changes detected. Consider running downstream models with modified_downstream=True to propagate changes."

        return response

    async def toolImpl_test_models(
        self,
        ctx: Context | None,
        select: str | None = None,
        exclude: str | None = None,
        modified_only: bool = False,
        modified_downstream: bool = False,
        fail_fast: bool = False,
    ) -> dict[str, Any]:
        """Implementation of test_models tool."""
        # Validate: can't use both smart and manual selection
        if (modified_only or modified_downstream) and select:
            raise ValueError("Cannot use both modified_* flags and select parameter")

        # Build command args
        args = ["test"]

        # Handle smart selection
        # Use relative path since DBT runs from project_dir
        assert self.project_dir is not None
        state_dir = self.project_dir / "target" / "state_last_run"

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

        # Execute with progress reporting
        logger.info(f"Running dbt tests with args: {args}")

        # Define progress callback if context available
        async def progress_callback(current: int, total: int, message: str) -> None:
            if ctx:
                await ctx.report_progress(progress=current, total=total, message=message)

        result = await self.runner.invoke(args, progress_callback=progress_callback if ctx else None)  # type: ignore

        if not result.success:
            error_msg = str(result.exception) if result.exception else "Tests failed"
            response = {
                "status": "error",
                "message": error_msg,
                "command": " ".join(args),
            }
            # Include dbt output for debugging
            if result.stdout:
                response["dbt_output"] = result.stdout
            if result.stderr:
                response["stderr"] = result.stderr
            return response

        # Parse run_results.json for details
        run_results = self._parse_run_results()

        return {
            "status": "success",
            "command": " ".join(args),
            "results": run_results.get("results", []),
            "elapsed_time": run_results.get("elapsed_time"),
        }

    async def toolImpl_build_models(
        self,
        ctx: Context | None,
        select: str | None = None,
        exclude: str | None = None,
        modified_only: bool = False,
        modified_downstream: bool = False,
        full_refresh: bool = False,
        fail_fast: bool = False,
        confirm_threshold: int = 5,
    ) -> dict[str, Any]:
        """Implementation of build_models tool."""
        # Validate: can't use both smart and manual selection
        if (modified_only or modified_downstream) and select:
            raise ValueError("Cannot use both modified_* flags and select parameter")

        # Build command args
        args = ["build"]

        # Handle smart selection
        # Use relative path since DBT runs from project_dir
        assert self.project_dir is not None
        state_dir = self.project_dir / "target" / "state_last_run"

        if modified_only or modified_downstream:
            if not state_dir.exists():
                return {
                    "status": "error",
                    "message": "No previous run state found. Run without modified_* flags first to establish baseline.",
                }

            selector = "state:modified+" if modified_downstream else "state:modified"

            # Detect which models will be built
            try:
                models_to_build = await self._detect_modified_models(selector)
            except Exception as e:
                return {"status": "error", "message": f"Failed to detect modified models: {str(e)}"}

            # Ask for user confirmation
            if not models_to_build:
                if ctx:
                    await ctx.info("No modified models detected")
                return {"status": "success", "message": "No modified models to build", "results": [], "elapsed_time": 0}

            # Ask for user confirmation only if above threshold
            if len(models_to_build) >= confirm_threshold and ctx:
                model_list = "\n".join(f"- {m}" for m in models_to_build)
                message = f"""The following {len(models_to_build)} model(s) have changes and will be built:

{model_list}

Do you want to proceed?"""

                result = await ctx.elicit(message=message, response_type=None)

                if result.action != "accept":
                    raise ValueError("User declined to build models")

            if ctx:
                await ctx.info(f"Building {len(models_to_build)} modified model(s)...")

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

        # Execute with progress reporting
        logger.info(f"Running DBT build with args: {args}")

        # Define progress callback if context available
        async def progress_callback(current: int, total: int, message: str) -> None:
            if ctx:
                await ctx.report_progress(progress=current, total=total, message=message)

        result = await self.runner.invoke(args, progress_callback=progress_callback if ctx else None)  # type: ignore

        if not result.success:
            error_msg = str(result.exception) if result.exception else "Build failed"
            response = {
                "status": "error",
                "message": error_msg,
                "command": " ".join(args),
            }
            # Include dbt output for debugging
            if result.stdout:
                response["dbt_output"] = result.stdout
            if result.stderr:
                response["stderr"] = result.stderr
            return response

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

    async def toolImpl_seed_data(
        self,
        ctx: Context | None = None,
        select: str | None = None,
        exclude: str | None = None,
        modified_only: bool = False,
        modified_downstream: bool = False,
        full_refresh: bool = False,
        show: bool = False,
    ) -> dict[str, Any]:
        """Implementation of seed_data tool."""
        # Validate: can't use both smart and manual selection
        if (modified_only or modified_downstream) and select:
            raise ValueError("Cannot use both modified_* flags and select parameter")

        # Build command args
        args = ["seed"]

        # Handle smart selection
        assert self.project_dir is not None
        state_dir = self.project_dir / "target" / "state_last_run"

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

        # Execute with progress reporting
        logger.info(f"Running DBT seed with args: {args}")

        # Define progress callback if context available
        async def progress_callback(current: int, total: int, message: str) -> None:
            if ctx:
                await ctx.report_progress(progress=current, total=total, message=message)

        result = await self.runner.invoke(args, progress_callback=progress_callback if ctx else None)  # type: ignore

        if not result.success:
            error_msg = str(result.exception) if result.exception else "Seed failed"
            response = {
                "status": "error",
                "message": error_msg,
                "command": " ".join(args),
            }
            # Include dbt output for debugging
            if result.stdout:
                response["dbt_output"] = result.stdout
            if result.stderr:
                response["stderr"] = result.stderr
            return response

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

    async def toolImpl_snapshot_models(
        self,
        select: str | None = None,
        exclude: str | None = None,
    ) -> dict[str, Any]:
        """Implementation of snapshot_models tool."""
        # Build command args
        args = ["snapshot"]

        if select:
            args.extend(["-s", select])

        if exclude:
            args.extend(["--exclude", exclude])

        # Execute
        logger.info(f"Running DBT snapshot with args: {args}")
        result = await self.runner.invoke(args)  # type: ignore

        if not result.success:
            error_msg = str(result.exception) if result.exception else "Snapshot failed"
            response = {
                "status": "error",
                "message": error_msg,
                "command": " ".join(args),
            }
            # Include dbt output for debugging
            if result.stdout:
                response["dbt_output"] = result.stdout
            if result.stderr:
                response["stderr"] = result.stderr
            return response

        # Parse run_results.json for details
        run_results = self._parse_run_results()

        return {
            "status": "success",
            "command": " ".join(args),
            "results": run_results.get("results", []),
            "elapsed_time": run_results.get("elapsed_time"),
        }

    async def toolImpl_install_deps(self) -> dict[str, Any]:
        """Implementation of install_deps tool."""
        # Execute dbt deps
        logger.info("Running dbt deps to install packages")
        result = await self.runner.invoke(["deps"])  # type: ignore

        if not result.success:
            error_msg = str(result.exception) if result.exception else "deps failed"
            return {
                "status": "error",
                "message": error_msg,
                "command": "dbt deps",
            }

        # Reload manifest to pick up newly installed packages
        logger.info("Reloading manifest to include new packages")
        await self.manifest.load()  # type: ignore

        # Get list of installed packages by checking for package macros
        installed_packages = set()
        assert self.manifest is not None
        manifest_dict = self.manifest.get_manifest_dict()
        macros = manifest_dict.get("macros", {})
        project_name = manifest_dict.get("metadata", {}).get("project_name", "")

        for unique_id in macros:
            # macro.package_name.macro_name format
            if unique_id.startswith("macro."):
                parts = unique_id.split(".")
                if len(parts) >= 2:
                    package_name = parts[1]
                    # Exclude built-in dbt package and project package
                    if package_name != "dbt" and package_name != project_name:
                        installed_packages.add(package_name)

        return {
            "status": "success",
            "command": "dbt deps",
            "installed_packages": sorted(installed_packages),
            "message": f"Successfully installed {len(installed_packages)} package(s)",
        }

    def _register_tools(self) -> None:
        """Register all dbt tools."""

        @self.app.tool()
        async def get_project_info(
            ctx: Context,
            run_debug: bool = True,
        ) -> dict[str, Any]:
            """Get information about the dbt project with optional diagnostics.

            Args:
                run_debug: Run `dbt debug` to validate environment and test connection (default: True)

            Returns:
                Dictionary with project information and diagnostic results
            """
            await self._ensure_initialized_with_context(ctx)
            return await self.toolImpl_get_project_info(run_debug)

        @self.app.tool()
        async def list_resources(ctx: Context, resource_type: str | None = None) -> list[dict[str, Any]]:
            """List all resources in the dbt project with optional filtering by type.

            This unified tool provides a consistent view across all dbt resource types.
            Returns simplified resource information optimized for LLM consumption.

            Args:
                resource_type: Optional filter to narrow results:
                    - "model": Data transformation models
                    - "source": External data sources
                    - "seed": CSV reference data files
                    - "snapshot": SCD Type 2 historical tables
                    - "test": Data quality tests
                    - "analysis": Ad-hoc analysis queries
                    - "macro": Jinja macros (includes macros from installed packages)
                    - None: Return all resources (default)

            Returns:
                List of resource dictionaries with consistent structure across types.
                Each resource includes: name, unique_id, resource_type, description, tags, etc.

            Package Discovery:
                Use resource_type="macro" to discover installed dbt packages.
                Macros follow the naming pattern: macro.{package_name}.{macro_name}

                Example - Check if dbt_utils is installed:
                    macros = list_resources("macro")
                    has_dbt_utils = any(m["unique_id"].startswith("macro.dbt_utils.") for m in macros)

                Example - List all installed packages:
                    macros = list_resources("macro")
                    packages = {m["unique_id"].split(".")[1] for m in macros
                               if m["unique_id"].startswith("macro.") and
                               m["unique_id"].split(".")[1] != "dbt"}

            Examples:
                list_resources() -> all resources
                list_resources("model") -> only models
                list_resources("source") -> only sources
                list_resources("test") -> only tests
                list_resources("macro") -> all macros (discover installed packages)
            """
            await self._ensure_initialized_with_context(ctx)
            return await self.toolImpl_list_resources(resource_type=resource_type)

        @self.app.tool()
        async def get_resource_info(
            ctx: Context,
            name: str,
            resource_type: str | None = None,
            include_database_schema: bool = True,
            include_compiled_sql: bool = True,
        ) -> dict[str, Any]:
            """Get detailed information about any dbt resource (model, source, seed, snapshot, test, etc.).

            This unified tool works across all resource types, auto-detecting the resource or filtering by type.
            Designed for LLM consumption - returns complete data even when multiple matches exist.

            Args:
                name: Resource name. For sources, use "source_name.table_name" or just "table_name"
                resource_type: Optional filter to narrow search:
                    - "model": Data transformation models
                    - "source": External data sources
                    - "seed": CSV reference data files
                    - "snapshot": SCD Type 2 historical tables
                    - "test": Data quality tests
                    - "analysis": Ad-hoc analysis queries
                    - None: Auto-detect (searches all types)
                include_database_schema: If True (default), query actual database table schema
                    for models/seeds/snapshots/sources and add as 'database_columns' field
                include_compiled_sql: If True (default), include compiled SQL with Jinja resolved
                    ({{ ref() }}, {{ source() }} → actual table names). Only applicable to models.
                    Will trigger dbt compile if not already compiled. Set to False to skip compilation.

            Returns:
                Resource information dictionary. If multiple matches found, returns:
                {"multiple_matches": True, "matches": [...], "message": "..."}

            Raises:
                ValueError: If resource not found

            Examples:
                get_resource_info("customers") -> auto-detect model or source
                get_resource_info("customers", "model") -> get model only
                get_resource_info("jaffle_shop.customers", "source") -> specific source
                get_resource_info("test_unique_customers") -> find test
                get_resource_info("customers", include_compiled_sql=True) -> include compiled SQL
            """
            await self._ensure_initialized_with_context(ctx)
            return await self.toolImpl_get_resource_info(name, resource_type, include_database_schema, include_compiled_sql)

        @self.app.tool()
        async def get_lineage(
            ctx: Context,
            name: str,
            resource_type: str | None = None,
            direction: str = "both",
            depth: int | None = None,
        ) -> dict[str, Any]:
            """Get lineage (dependency tree) for any dbt resource with auto-detection.

            This unified tool works across all resource types (models, sources, seeds, snapshots, etc.)
            showing upstream and/or downstream dependencies with configurable depth.

            Args:
                name: Resource name. For sources, use "source_name.table_name" or just "table_name"
                    Examples: "customers", "jaffle_shop.orders", "raw_customers"
                resource_type: Optional filter to narrow search:
                    - "model": Data transformation models
                    - "source": External data sources
                    - "seed": CSV reference data files
                    - "snapshot": SCD Type 2 historical tables
                    - "test": Data quality tests
                    - "analysis": Ad-hoc analysis queries
                    - None: Auto-detect (searches all types)
                direction: Lineage direction:
                    - "upstream": Show where data comes from (parents)
                    - "downstream": Show what depends on this resource (children)
                    - "both": Show full lineage (default)
                depth: Maximum levels to traverse (None for unlimited)
                    - depth=1: Immediate dependencies only
                    - depth=2: Dependencies + their dependencies
                    - None: Full dependency tree

            Returns:
                Lineage information with upstream/downstream nodes and statistics.
                If multiple matches found, returns all matches for LLM to process.

            Raises:
                ValueError: If resource not found or invalid direction

            Examples:
                get_lineage("customers") -> auto-detect and show full lineage
                get_lineage("customers", "model", "upstream") -> where customers model gets data
                get_lineage("jaffle_shop.orders", "source", "downstream", 2) -> 2 levels of dependents
            """
            await self._ensure_initialized_with_context(ctx)
            return await self.toolImpl_get_lineage(name, resource_type, direction, depth)

        @self.app.tool()
        async def analyze_impact(
            ctx: Context,
            name: str,
            resource_type: str | None = None,
        ) -> dict[str, Any]:
            """Analyze the impact of changing any dbt resource with auto-detection.

            This unified tool works across all resource types (models, sources, seeds, snapshots, etc.)
            showing all downstream dependencies that would be affected by changes. Provides actionable
            recommendations for running affected resources.

            Args:
                name: Resource name. For sources, use "source_name.table_name" or just "table_name"
                    Examples: "stg_customers", "jaffle_shop.orders", "raw_customers"
                resource_type: Optional filter to narrow search:
                    - "model": Data transformation models
                    - "source": External data sources
                    - "seed": CSV reference data files
                    - "snapshot": SCD Type 2 historical tables
                    - "test": Data quality tests
                    - "analysis": Ad-hoc analysis queries
                    - None: Auto-detect (searches all types)

            Returns:
                Impact analysis with:
                - List of affected models by distance
                - Count of affected tests and other resources
                - Total impact statistics
                - Resources grouped by distance from changed resource
                - Recommended dbt command to run affected resources
                - Human-readable impact assessment message
                If multiple matches found, returns all matches for LLM to process.

            Raises:
                ValueError: If resource not found

            Examples:
                analyze_impact("stg_customers") -> auto-detect and show impact
                analyze_impact("jaffle_shop.orders", "source") -> impact of source change
                analyze_impact("raw_customers", "seed") -> impact of seed data change
            """
            await self._ensure_initialized_with_context(ctx)
            return await self.toolImpl_analyze_impact(name, resource_type)

        @self.app.tool()
        async def query_database(ctx: Context, sql: str, output_file: str | None = None, output_format: str = "json") -> dict[str, Any]:
            """Execute a SQL query against the dbt project's database.

            BEST PRACTICES:
            1. Before querying: Inspect schema using get_resource_info() with include_database_schema=True
            2. Always use {{ ref('model_name') }} for dbt models (never hard-code table paths)
            3. Always use {{ source('source_name', 'table_name') }} for source tables
            4. For non-dbt tables: Verify schema with user before querying
            5. After results: Report "Query Result: X rows retrieved" and summarize key findings

            QUERY EFFICIENCY:
            - Use aggregations (COUNT, SUM, AVG, etc.) instead of pulling raw data
            - Apply WHERE filters early to narrow scope before aggregation
            - Use LIMIT for exploratory queries to get representative samples
            - Calculate totals, ratios, and trends in SQL rather than returning all rows
            - Use GROUP BY for categorization within the query
            - Always ask: "Can SQL answer this question directly?" before returning data

            LARGE RESULT HANDLING:
            - For queries returning many rows (>100), use output_file parameter to save results to disk
            - This prevents context window overflow and improves performance
            - The tool returns metadata + preview instead of full results when output_file is used
            - Example: query_database(sql="SELECT * FROM large_table", output_file="temp_auto/results.json")

            OUTPUT FORMATS:
            - json (default): Returns data as JSON array of objects
            - csv: Returns comma-separated values with header row
            - tsv: Returns tab-separated values with header row
            - CSV/TSV formats use proper quoting (only when necessary) and are Excel-compatible

            Args:
                sql: SQL query with Jinja templating: {{ ref('model') }}, {{ source('src', 'table') }}
                     For exploratory queries, include LIMIT. For aggregations/counts, omit it.
                output_file: Optional file path to save results. Recommended for large result sets (>100 rows).
                            If provided, only metadata is returned (no preview for CSV/TSV).
                            If omitted, all data is returned inline (may consume large context).
                output_format: Output format - "json" (default), "csv", or "tsv"

            Returns:
                JSON inline: {"status": "success", "row_count": N, "rows": [...]}
                JSON file: {"status": "success", "row_count": N, "saved_to": "path", "preview": [...]}
                CSV/TSV inline: {"status": "success", "row_count": N, "format": "csv", "csv": "..."}
                CSV/TSV file: {"status": "success", "row_count": N, "format": "csv", "saved_to": "path"}
            """
            await self._ensure_initialized_with_context(ctx)
            return await self.toolImpl_query_database(sql, output_file, output_format)

        @self.app.tool()
        async def run_models(
            ctx: Context,
            select: str | None = None,
            exclude: str | None = None,
            modified_only: bool = False,
            modified_downstream: bool = False,
            full_refresh: bool = False,
            fail_fast: bool = False,
            check_schema_changes: bool = False,
            confirm_threshold: int = 5,
        ) -> dict[str, Any]:
            """Run dbt models (compile SQL and execute against database).

            Smart selection modes for developers:
            - modified_only: Run only models that changed since last successful run
            - modified_downstream: Run changed models + all downstream dependencies

            Manual selection (if not using smart modes):
            - select: dbt selector syntax (e.g., "customers", "tag:mart", "stg_*")
            - exclude: Exclude specific models

            Args:
                select: Manual selector (e.g., "customers", "tag:mart", "path:marts/*")
                exclude: Exclude selector (e.g., "tag:deprecated")
                modified_only: Only run models modified since last successful run
                modified_downstream: Run modified models + downstream dependencies
                full_refresh: Force full refresh of incremental models
                fail_fast: Stop execution on first failure
                check_schema_changes: Detect schema changes and recommend downstream runs
                confirm_threshold: Minimum number of models to trigger confirmation (default: 5)

            Returns:
                Execution results with status, models run, timing info, and optional schema_changes

            Examples:
                - run_models(select="customers") - Run specific model
                - run_models(modified_only=True) - Run only what changed
                - run_models(modified_downstream=True) - Run changed + downstream
                - run_models(select="tag:mart", full_refresh=True) - Full refresh marts
                - run_models(modified_only=True, check_schema_changes=True) - Detect schema changes
                - run_models(modified_only=True, confirm_threshold=10) - Confirm only if 10+ models
            """
            await self._ensure_initialized_with_context(ctx)
            return await self.toolImpl_run_models(ctx, select, exclude, modified_only, modified_downstream, full_refresh, fail_fast, check_schema_changes, confirm_threshold)

        @self.app.tool()
        async def test_models(
            ctx: Context,
            select: str | None = None,
            exclude: str | None = None,
            modified_only: bool = False,
            modified_downstream: bool = False,
            fail_fast: bool = False,
        ) -> dict[str, Any]:
            """Run dbt tests on models and sources.

            Smart selection modes for developers:
            - modified_only: Test only models that changed since last successful run
            - modified_downstream: Test changed models + all downstream dependencies

            Manual selection (if not using smart modes):
            - select: dbt selector syntax (e.g., "customers", "tag:mart", "test_type:generic")
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
            await self._ensure_initialized_with_context(ctx)
            return await self.toolImpl_test_models(ctx, select, exclude, modified_only, modified_downstream, fail_fast)

        @self.app.tool()
        async def build_models(
            ctx: Context,
            select: str | None = None,
            exclude: str | None = None,
            modified_only: bool = False,
            modified_downstream: bool = False,
            full_refresh: bool = False,
            fail_fast: bool = False,
            confirm_threshold: int = 5,
        ) -> dict[str, Any]:
            """Run DBT build (run + test in DAG order).

            Smart selection modes for developers:
            - modified_only: Build only models that changed since last successful run
            - modified_downstream: Build changed models + all downstream dependencies

            Manual selection (if not using smart modes):
            - select: dbt selector syntax (e.g., "customers", "tag:mart", "stg_*")
            - exclude: Exclude specific models

            Args:
                select: Manual selector
                exclude: Exclude selector
                modified_only: Only build models modified since last successful run
                modified_downstream: Build modified models + downstream dependencies
                full_refresh: Force full refresh of incremental models
                fail_fast: Stop execution on first failure
                confirm_threshold: Minimum number of models to trigger confirmation (default: 5)

            Returns:
                Build results with status, models run/tested, and timing info
            """
            await self._ensure_initialized_with_context(ctx)
            return await self.toolImpl_build_models(ctx, select, exclude, modified_only, modified_downstream, full_refresh, fail_fast, confirm_threshold)

        @self.app.tool()
        async def seed_data(
            ctx: Context,
            select: str | None = None,
            exclude: str | None = None,
            modified_only: bool = False,
            modified_downstream: bool = False,
            full_refresh: bool = False,
            show: bool = False,
        ) -> dict[str, Any]:
            """Load seed data (CSV files) from seeds/ directory into database tables.

            Seeds are typically used for reference data like country codes, product categories, etc.

            Smart selection modes detect changed CSV files:
            - modified_only: Load only seeds that changed since last successful run
            - modified_downstream: Load changed seeds + all downstream dependencies

            Manual selection (if not using smart modes):
            - select: dbt selector syntax (e.g., "raw_customers", "tag:lookup")
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
            await self._ensure_initialized_with_context(ctx)
            return await self.toolImpl_seed_data(ctx, select, exclude, modified_only, modified_downstream, full_refresh, show)

        @self.app.tool()
        async def snapshot_models(
            ctx: Context,
            select: str | None = None,
            exclude: str | None = None,
        ) -> dict[str, Any]:
            """Execute dbt snapshots to capture slowly changing dimensions (SCD Type 2).

            Snapshots track historical changes over time by recording:
            - When records were first seen (valid_from)
            - When records changed or were deleted (valid_to)
            - The state of records at each point in time

            Unlike models and seeds, snapshots are time-based and should be run on a schedule
            (e.g., daily or hourly), not during interactive development.

            Args:
                select: dbt selector syntax (e.g., "snapshot_name", "tag:daily")
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
            await self._ensure_initialized_with_context(ctx)
            return await self.toolImpl_snapshot_models(select, exclude)

        @self.app.tool()
        async def install_deps(ctx: Context) -> dict[str, Any]:
            """Install dbt packages defined in packages.yml.

            This tool enables interactive workflow where an LLM can:
            1. Suggest using a dbt package (e.g., dbt_utils)
            2. Edit packages.yml to add the package
            3. Run install_deps() to install it
            4. Write code that uses the package's macros

            This completes the recommendation workflow without breaking conversation flow.

            Returns:
                Installation results with status and installed packages

            Example workflow:
                User: "Create a date dimension table"
                LLM: 1. Checks: list_resources(type="macro") -> no dbt_utils
                     2. Edits: packages.yml (adds dbt_utils package)
                     3. Runs: install_deps() (installs package)
                     4. Creates: models/date_dim.sql (uses dbt_utils.date_spine)

            Note: This is an interactive development tool, not infrastructure automation.
            It enables the LLM to act on its own recommendations mid-conversation.
            """
            await self._ensure_initialized_with_context(ctx)
            return await self.toolImpl_install_deps()

    def run(self) -> None:
        """Run the MCP server."""
        self.app.run(show_banner=False)


def create_server(project_dir: Optional[str] = None, timeout: Optional[float] = None) -> DbtCoreMcpServer:
    """Create a new dbt Core MCP server instance.

    Args:
        project_dir: Optional path to dbt project directory.
                     If not provided, automatically detects from MCP workspace roots
                     or falls back to current working directory.
        timeout: Optional timeout in seconds for dbt commands (default: None for no timeout).

    Returns:
        DbtCoreMcpServer instance
    """
    return DbtCoreMcpServer(project_dir=project_dir, timeout=timeout)
