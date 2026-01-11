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
from typing import Any, Awaitable, Callable
from urllib.parse import unquote
from urllib.request import url2pathname

import yaml
from fastmcp import FastMCP
from fastmcp.server.context import Context
from fastmcp.server.middleware.error_handling import ErrorHandlingMiddleware
from fastmcp.server.middleware.rate_limiting import RateLimitingMiddleware
from mcp import McpError
from mcp.types import ErrorData

from .dbt.bridge_runner import BridgeRunner
from .dbt.manifest import ManifestLoader
from .utils.env_detector import detect_python_command

# Type alias for progress reporting callbacks
ProgressCallback = Callable[[int, int, str], Awaitable[None]]

logger = logging.getLogger(__name__)


class DbtCoreMcpServer:
    """
    dbt Core MCP Server.

    Provides tools for interacting with dbt projects.
    """

    def __init__(self, project_dir: str | None = None, timeout: float | None = None) -> None:
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
            
            ## Subagent Delegation Protocol
            
            ALWAYS use #runSubagent for dbt data exploration and analysis. Your context window fills quickly with query results - avoid this by delegating complex workflows to subagents.
            
            When to delegate to #runSubagent:
            - Data exploration across multiple models/sources
            - Multi-query analysis workflows  
            - Large result sets (>100 rows expected)
            - Impact analysis requiring sample queries
            - Any workflow involving: schema inspection → queries → analysis
            
            The subagent handles all the heavy lifting and returns only a concise summary, keeping your main context clean.
            
            Single simple queries with known schema: Execute directly using the tools.
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
        # For testing: force fresh BridgeRunner every call to test complete isolation
        self.force_fresh_runner = False  # Set to False to reuse runners for performance
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
            with open(project_file, encoding="utf-8") as f:
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

    async def _get_runner(self) -> BridgeRunner:
        """Get BridgeRunner instance with explicit control over creation.

        Uses self.force_fresh_runner to determine behavior:
        - If True, always create a fresh BridgeRunner instance
        - If False, reuse existing runner if available

        Returns:
            BridgeRunner instance
        """
        if self.force_fresh_runner or not self.runner:
            if not self.project_dir:
                raise RuntimeError("Project directory not set")

            # Detect Python command for user's environment
            python_cmd = detect_python_command(self.project_dir)
            logger.info(f"Creating {'fresh' if self.force_fresh_runner else 'new'} BridgeRunner with command: {python_cmd}")

            # Create bridge runner with persistent process for better performance
            self.runner = BridgeRunner(self.project_dir, python_cmd, timeout=self.timeout, use_persistent_process=True)

        return self.runner

    def _manifest_exists(self) -> bool:
        """
        Check if manifest.json exists.

        Simple check - tools will handle their own parsing as needed.
        """
        if self.project_dir is None:
            return False
        manifest_path = self.project_dir / "target" / "manifest.json"
        return manifest_path.exists()

    async def _initialize_dbt_components(self, needs_parse: bool = True, force_parse: bool = False) -> None:
        """Initialize dbt runner and manifest loader.

        Args:
            needs_parse: Whether to run dbt parse. If False, assumes manifest already exists and is fresh.
            force_parse: If True, force parsing even if manifest exists (for tools needing fresh data).
        """

        if not self.project_dir:
            raise RuntimeError("Project directory not set")

        # Get runner (fresh or reused based on self.force_fresh_runner)
        runner = await self._get_runner()

        # Parse if manifest missing OR force requested
        should_parse = needs_parse or force_parse
        if should_parse:
            if not self._manifest_exists():
                logger.info("No manifest found - running initial dbt parse...")
            else:
                logger.info("Force parse requested - running dbt parse for fresh data...")
            parse_args = ["parse"]  # Use partial parse for efficiency
            result = await runner.invoke(parse_args)
            if not result.success:
                error_msg = str(result.exception) if result.exception else "Unknown error"
                raise RuntimeError(f"Failed to parse dbt project: {error_msg}")
        else:
            logger.info("Manifest exists and no force parse - tools will handle parsing as needed")

        # Initialize or reload manifest loader
        manifest_path = runner.get_manifest_path()
        if not self.manifest:
            self.manifest = ManifestLoader(manifest_path)
        await self.manifest.load()

        logger.info("dbt components initialized successfully")

    async def _ensure_initialized_with_context(self, ctx: Any, force_parse: bool = False) -> None:
        """Ensure dbt components are initialized, with optional workspace root detection.

        Uses async lock to prevent concurrent initialization races when multiple tools
        are called simultaneously.

        Args:
            ctx: FastMCP Context for accessing workspace roots
            force_parse: If True, force parsing even if manifest exists (for tools needing fresh data)
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

            await self._initialize_dbt_components(needs_parse=not self._manifest_exists(), force_parse=force_parse)

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
            with open(run_results_path, encoding="utf-8") as f:
                data = json.load(f)

            # Simplify results for output
            simplified_results = []
            for result in data.get("results", []):
                simplified_result = {
                    "unique_id": result.get("unique_id"),
                    "status": result.get("status"),
                    "message": result.get("message"),
                    "execution_time": result.get("execution_time"),
                    "failures": result.get("failures"),
                }

                # Include additional diagnostic fields for failed tests
                if result.get("status") in ("fail", "error"):
                    simplified_result["compiled_code"] = result.get("compiled_code")
                    simplified_result["adapter_response"] = result.get("adapter_response")

                simplified_results.append(simplified_result)

            return {
                "results": simplified_results,
                "elapsed_time": data.get("elapsed_time", 0),
            }
        except Exception as e:
            logger.warning(f"Failed to parse run_results.json: {e}")
            return {"results": [], "elapsed_time": 0}

    async def _report_final_progress(
        self,
        ctx: Context | None,
        results_list: list[dict[str, Any]],
        command_name: str,
        resource_type: str,
    ) -> None:
        """Report final progress with status breakdown.

        Args:
            ctx: MCP context for progress reporting
            results_list: List of result dictionaries from dbt execution
            command_name: Command prefix for message (e.g., "Run", "Test", "Build")
            resource_type: Resource type for message (e.g., "models", "tests", "resources")
        """
        if not ctx:
            return

        if not results_list:
            await ctx.report_progress(progress=0, total=0, message=f"0 {resource_type} matched selector")
            return

        # Count statuses - different commands use different status values
        total = len(results_list)
        passed_count = sum(1 for r in results_list if r.get("status") in ("success", "pass"))
        failed_count = sum(1 for r in results_list if r.get("status") in ("error", "fail"))
        skip_count = sum(1 for r in results_list if r.get("status") in ("skipped", "skip"))
        warn_count = sum(1 for r in results_list if r.get("status") == "warn")

        # Build status parts
        parts = []
        if passed_count > 0:
            # Use "All passed" only if no other statuses present
            has_other_statuses = failed_count > 0 or warn_count > 0 or skip_count > 0
            parts.append(f"✅ {passed_count} passed" if has_other_statuses else "✅ All passed")
        if failed_count > 0:
            parts.append(f"❌ {failed_count} failed")
        if warn_count > 0:
            parts.append(f"⚠️ {warn_count} warned")
        if skip_count > 0:
            parts.append(f"⏭️ {skip_count} skipped")

        summary = f"{command_name}: {total}/{total} {resource_type} completed ({', '.join(parts)})"
        await ctx.report_progress(progress=total, total=total, message=summary)

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
            with open(state_manifest_path, encoding="utf-8") as f:
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
            runner = await self._get_runner()
            result = await runner.invoke_query(sql)  # type: ignore

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

    def _clear_stale_run_results(self) -> None:
        """Delete stale run_results.json before command execution.

        This prevents reading cached results from previous runs.
        """
        if not self.project_dir:
            return

        run_results_path = self.project_dir / "target" / "run_results.json"
        if run_results_path.exists():
            try:
                run_results_path.unlink()
                logger.debug("Deleted stale run_results.json before execution")
            except OSError as e:
                logger.warning(f"Could not delete stale run_results.json: {e}")

    async def _save_execution_state(self) -> None:
        """Save current manifest as state for future state-based runs.

        After successful execution, saves manifest.json to target/state_last_run/
        so future runs can use --state to detect modifications.
        """
        if not self.project_dir:
            return

        state_dir = self.project_dir / "target" / "state_last_run"
        state_dir.mkdir(parents=True, exist_ok=True)

        runner = await self._get_runner()
        manifest_path = runner.get_manifest_path()  # type: ignore

        try:
            shutil.copy(manifest_path, state_dir / "manifest.json")
            logger.debug(f"Saved execution state to {state_dir}")
        except OSError as e:
            logger.warning(f"Failed to save execution state: {e}")

    def _validate_and_parse_results(self, result: Any, command_name: str) -> dict[str, Any]:
        """Parse run_results.json and validate execution succeeded.

        Args:
            result: The execution result from dbt runner
            command_name: Name of dbt command (e.g., "run", "test", "build", "seed")

        Returns:
            Parsed run_results dictionary

        Raises:
            RuntimeError: If dbt failed before execution (parse error, connection failure, etc.)
        """
        run_results = self._parse_run_results()

        if not run_results.get("results"):
            # No results means dbt failed before execution
            if result and not result.success:
                error_msg = str(result.exception) if result.exception else f"dbt {command_name} execution failed"
                # Extract specific error from stdout if available
                if result.stdout and "Error" in result.stdout:
                    lines = result.stdout.split("\n")
                    for i, line in enumerate(lines):
                        if "Error" in line or "error" in line:
                            error_msg = "\n".join(lines[i : min(i + 5, len(lines))]).strip()
                            break
                else:
                    # Include full stdout/stderr for debugging when no specific error found
                    stdout_preview = (result.stdout[:500] + "...") if result.stdout and len(result.stdout) > 500 else (result.stdout or "(no stdout)")
                    stderr_preview = (result.stderr[:500] + "...") if result.stderr and len(result.stderr) > 500 else (result.stderr or "(no stderr)")
                    error_msg = f"{error_msg}\nstdout: {stdout_preview}\nstderr: {stderr_preview}"
                raise RuntimeError(f"dbt {command_name} failed to execute: {error_msg}")

        return run_results

    async def _prepare_state_based_selection(
        self,
        select_state_modified: bool,
        select_state_modified_plus_downstream: bool,
        select: str | None,
    ) -> str | None:
        """Validate and prepare state-based selection.

        Args:
            select_state_modified: Use state:modified selector
            select_state_modified_plus_downstream: Extend to state:modified+
            select: Manual selector (conflicts with state-based)

        Returns:
            The dbt selector string to use ("state:modified" or "state:modified+"), or None if:
            - Not using state-based selection
            - No previous state exists (cannot determine modifications)

        Raises:
            ValueError: If validation fails
        """
        # Validate: hierarchical requirement
        if select_state_modified_plus_downstream and not select_state_modified:
            raise ValueError("select_state_modified_plus_downstream requires select_state_modified=True")

        # Validate: can't use both state-based and manual selection
        if select_state_modified and select:
            raise ValueError("Cannot use both select_state_modified* flags and select parameter")

        # If not using state-based selection, return None
        if not select_state_modified:
            return None

        # Check if state exists
        state_dir = self.project_dir / "target" / "state_last_run"  # type: ignore
        if not state_dir.exists():
            # No state - cannot determine modifications
            return None

        # Return selector (state exists)
        return "state:modified+" if select_state_modified_plus_downstream else "state:modified"

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
                    runner = await self._get_runner()
                    compile_result = await runner.invoke_compile(name, force=False)  # type: ignore

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
            runner = await self._get_runner()
            debug_result_obj = await runner.invoke(["debug"])  # type: ignore

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

    async def toolImpl_query_database(self, ctx: Context | None, sql: str, output_file: str | None = None, output_format: str = "json") -> dict[str, Any]:
        """Implementation for query_database tool."""

        # Define progress callback if context available
        async def progress_callback(current: int, total: int, message: str) -> None:
            if ctx:
                await ctx.report_progress(progress=current, total=total, message=message)

        # Execute query using dbt show with --no-populate-cache for optimal performance
        runner = await self._get_runner()
        result = await runner.invoke_query(sql, progress_callback=progress_callback if ctx else None)  # type: ignore

        if not result.success:
            error_msg = str(result.exception) if result.exception else "Unknown error"
            # Include dbt output in error message for context
            full_error = error_msg
            if result.stdout and "Database Error" in result.stdout:
                # Extract the helpful database error details
                full_error = result.stdout
            raise RuntimeError(f"Query execution failed: {full_error}")

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

                        with open(output_path, "w", encoding="utf-8", newline="") as f:
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
                        with open(output_path, "w", encoding="utf-8") as f:
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
        select_state_modified: bool = False,
        select_state_modified_plus_downstream: bool = False,
        full_refresh: bool = False,
        fail_fast: bool = False,
        check_schema_changes: bool = False,
        cache_selected_only: bool = True,
    ) -> dict[str, Any]:
        """Implementation for run_models tool."""
        # Prepare state-based selection (validates and returns selector)
        selector = await self._prepare_state_based_selection(select_state_modified, select_state_modified_plus_downstream, select)

        # Early return if state-based requested but no state exists
        if select_state_modified and not selector:
            raise RuntimeError("No previous state found - cannot determine modifications. Run 'dbt run' or 'dbt build' first to create baseline state.")

        # Build command args
        args = ["run"]

        # Optimize cache: only cache schemas containing selected models (if enabled)
        # Default True for performance, can be disabled if full caching needed
        if cache_selected_only and (select or selector or select_state_modified):
            args.append("--cache-selected-only")

        # Add selector if we have one (state-based or manual)
        if selector:
            args.extend(["-s", selector, "--state", "target/state_last_run"])
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

            if select_state_modified:
                selector = "state:modified+" if select_state_modified_plus_downstream else "state:modified"
                list_args.extend(["-s", selector, "--state", "target/state_last_run"])
            elif select:
                list_args.extend(["-s", select])

            if exclude:
                list_args.extend(["--exclude", exclude])

            # Get list of models
            logger.info(f"Getting model list: {list_args}")
            runner = await self._get_runner()
            list_result = await runner.invoke(list_args)  # type: ignore

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

        # Delete stale run_results.json to ensure we only read fresh results
        self._clear_stale_run_results()

        runner = await self._get_runner()
        result = await runner.invoke(args, progress_callback=progress_callback if ctx else None, expected_total=expected_total)  # type: ignore

        # Parse run_results.json to discriminate system errors from business outcomes
        run_results = self._validate_and_parse_results(result, "run")

        # Business outcome - dbt executed models (some may have failed)
        # Continue with existing run_results parsing

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
        if result.success:
            await self._save_execution_state()

        # Send final progress update with run summary
        results_list = run_results.get("results", [])
        await self._report_final_progress(ctx, results_list, "Run", "models")

        # Empty results means selector matched nothing - this is an error
        if not results_list:
            raise RuntimeError(f"No models matched selector: {select or selector or 'all'}")

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
        select_state_modified: bool = False,
        select_state_modified_plus_downstream: bool = False,
        fail_fast: bool = False,
    ) -> dict[str, Any]:
        """Implementation of test_models tool."""
        # Prepare state-based selection (validates and returns selector)
        selector = await self._prepare_state_based_selection(select_state_modified, select_state_modified_plus_downstream, select)

        # Early return if state-based requested but no state exists
        if select_state_modified and not selector:
            raise RuntimeError("No previous state found - cannot determine modifications. Run 'dbt run' or 'dbt build' first to create baseline state.")

        # Build command args
        args = ["test"]

        # Add selector if we have one (state-based or manual)
        if selector:
            args.extend(["-s", selector, "--state", "target/state_last_run"])
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

        # Delete stale run_results.json to ensure we only read fresh results
        self._clear_stale_run_results()

        runner = await self._get_runner()
        result = await runner.invoke(args, progress_callback=progress_callback if ctx else None)  # type: ignore

        # Parse run_results.json to discriminate system errors from business outcomes
        run_results = self._validate_and_parse_results(result, "test")

        # Business outcome - dbt executed tests (some may have failed)
        # Continue with existing run_results parsing
        results_list = run_results.get("results", [])

        # Remove heavy fields from results to reduce token usage
        for result in results_list:
            result.pop("compiled_code", None)
            result.pop("raw_code", None)

        # Send final progress update with test summary
        await self._report_final_progress(ctx, results_list, "Test", "tests")

        # Empty results means selector matched nothing - this is an error
        if not results_list:
            raise RuntimeError(f"No tests matched selector: {select or selector or 'all'}")

        # Check if any tests failed - if so, return RPC error with structured data
        failed_tests = [r for r in results_list if r.get("status") == "fail"]
        if failed_tests:
            # Check if any failed tests are unit tests (they have diff output)
            has_unit_test_failures = any("unit_test" in r.get("unique_id", "") for r in failed_tests)

            # Add diff format legend for unit test failures
            diff_legend = ""
            if has_unit_test_failures:
                diff_legend = (
                    "\n\nUnit test diff format (daff tabular diff):\n"
                    "  @@    Header row with column names\n"
                    "  +++   Row present in actual output but missing from expected\n"
                    "  ---   Row present in expected but missing from actual output\n"
                    "  →     Row with at least one modified cell (→, -->, etc.)\n"
                    "  ...   Rows omitted for brevity\n"
                    "  old_value→new_value   Shows the change in a specific cell\n"
                    "\nFull specification: https://paulfitz.github.io/daff-doc/spec.html"
                )

            error_data = {
                "error": "test_failure",
                "message": f"{len(failed_tests)} test(s) failed{diff_legend}",
                "command": " ".join(args),
                "results": results_list,  # Include ALL results (both passing and failing)
                "elapsed_time": run_results.get("elapsed_time"),
                "summary": {
                    "total": len(results_list),
                    "passed": len([r for r in results_list if r.get("status") == "pass"]),
                    "failed": len(failed_tests),
                },
            }
            # Use McpError with custom code for business failures (not system errors)
            # Code -32000 to -32099 are reserved for implementation-defined server errors
            raise McpError(
                ErrorData(
                    code=-32000,  # Server error (business failure, not system error)
                    message=json.dumps(error_data, indent=2),
                )
            )

        # All tests passed - return success
        return {
            "status": "success",
            "command": " ".join(args),
            "results": results_list,
            "elapsed_time": run_results.get("elapsed_time"),
        }

    async def toolImpl_build_models(
        self,
        ctx: Context | None,
        select: str | None = None,
        exclude: str | None = None,
        select_state_modified: bool = False,
        select_state_modified_plus_downstream: bool = False,
        full_refresh: bool = False,
        fail_fast: bool = False,
        cache_selected_only: bool = True,
    ) -> dict[str, Any]:
        """Implementation of build_models tool."""
        # Prepare state-based selection (validates and returns selector)
        selector = await self._prepare_state_based_selection(select_state_modified, select_state_modified_plus_downstream, select)

        # Early return if state-based requested but no state exists
        if select_state_modified and not selector:
            raise RuntimeError("No previous state found - cannot determine modifications. Run 'dbt build' first to create baseline state.")

        # Build command args
        args = ["build"]

        # Optimize cache: only cache schemas containing selected models (if enabled)
        # Default True for performance, can be disabled if full caching needed
        if cache_selected_only and (select or selector or select_state_modified):
            args.append("--cache-selected-only")

        # Add selector if we have one (state-based or manual)
        if selector:
            args.extend(["-s", selector, "--state", "target/state_last_run"])
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

        # Delete stale run_results.json to ensure we only read fresh results
        self._clear_stale_run_results()

        runner = await self._get_runner()
        result = await runner.invoke(args, progress_callback=progress_callback if ctx else None)  # type: ignore

        # Parse run_results.json to discriminate system errors from business outcomes
        run_results = self._validate_and_parse_results(result, "build")

        # Business outcome - dbt executed successfully
        # Save state on success for next modified run
        if result and result.success:
            await self._save_execution_state()

        # Send final progress update with build summary
        results_list = run_results.get("results", [])
        if ctx:
            if results_list:
                passed_count = sum(1 for r in results_list if r.get("status") in ("success", "pass"))
                failed_count = sum(1 for r in results_list if r.get("status") in ("error", "fail"))
                skip_count = sum(1 for r in results_list if r.get("status") == "skipped")

                total = len(results_list)
                parts = []
                if passed_count > 0:
                    parts.append(f"✅ {passed_count} passed" if failed_count > 0 or skip_count > 0 else "✅ All passed")
                if failed_count > 0:
                    parts.append(f"❌ {failed_count} failed")
                if skip_count > 0:
                    parts.append(f"⏭️ {skip_count} skipped")

                summary = f"Build: {total}/{total} resources completed ({', '.join(parts)})"
                await ctx.report_progress(progress=total, total=total, message=summary)
            else:
                await ctx.report_progress(progress=0, total=0, message="0 resources matched selector")

        # Empty results means selector matched nothing - this is an error
        if not results_list:
            raise RuntimeError(f"No resources matched selector: {select or selector or 'all'}")

        return {
            "status": "success",
            "command": " ".join(args),
            "results": run_results.get("results", []),
            "elapsed_time": run_results.get("elapsed_time"),
        }

    async def toolImpl_seed_data(
        self,
        ctx: Context | None,
        select: str | None = None,
        exclude: str | None = None,
        select_state_modified: bool = False,
        select_state_modified_plus_downstream: bool = False,
        full_refresh: bool = False,
        show: bool = False,
    ) -> dict[str, Any]:
        """Implementation of seed_data tool."""
        # Prepare state-based selection (validates and returns selector)
        selector = await self._prepare_state_based_selection(select_state_modified, select_state_modified_plus_downstream, select)

        # Early return if state-based requested but no state exists
        if select_state_modified and not selector:
            raise RuntimeError("No previous state found - cannot determine modifications. Run 'dbt seed' first to create baseline state.")

        # Build command args
        args = ["seed"]

        # Add selector if we have one (state-based or manual)
        if selector:
            args.extend(["-s", selector, "--state", "target/state_last_run"])
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

        # Delete stale run_results.json to ensure we only read fresh results
        self._clear_stale_run_results()

        runner = await self._get_runner()
        result = await runner.invoke(args, progress_callback=progress_callback if ctx else None)  # type: ignore

        # Parse run_results.json to discriminate system errors from business outcomes
        run_results = self._validate_and_parse_results(result, "seed")

        # Business outcome - dbt executed successfully
        # Save state on success for next modified run
        if result.success:
            await self._save_execution_state()

        # Parse run_results.json for details
        run_results = self._parse_run_results()

        # Send tool-specific final progress
        if ctx:
            if run_results.get("results"):
                results_list = run_results["results"]
                total = len(results_list)
                passed_count = sum(1 for r in results_list if r.get("status") == "success")
                failed_count = sum(1 for r in results_list if r.get("status") in ("error", "fail"))

                parts = []
                if passed_count > 0:
                    parts.append(f"✅ {passed_count} passed" if failed_count > 0 else "✅ All passed")
                if failed_count > 0:
                    parts.append(f"❌ {failed_count} failed")

                summary = f"Seed: {total}/{total} seeds completed ({', '.join(parts)})"
                await ctx.report_progress(progress=total, total=total, message=summary)
            else:
                await ctx.report_progress(progress=0, total=0, message="0 seeds matched selector")

        # Empty results means selector matched nothing - this is an error
        if not run_results.get("results"):
            raise RuntimeError(f"No seeds matched selector: {select or selector or 'all'}")

        return {
            "status": "success",
            "command": " ".join(args),
            "results": run_results.get("results", []),
            "elapsed_time": run_results.get("elapsed_time"),
        }

    async def toolImpl_snapshot_models(
        self,
        ctx: Context | None,
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

        # Delete stale run_results.json to ensure we only read fresh results
        self._clear_stale_run_results()

        runner = await self._get_runner()
        result = await runner.invoke(args)  # type: ignore

        # Parse run_results.json to discriminate system errors from business outcomes
        run_results = self._validate_and_parse_results(result, "snapshot")

        # Send tool-specific final progress
        if ctx:
            if run_results.get("results"):
                results_list = run_results["results"]
                total = len(results_list)
                passed_count = sum(1 for r in results_list if r.get("status") == "success")
                failed_count = sum(1 for r in results_list if r.get("status") in ("error", "fail"))

                parts = []
                if passed_count > 0:
                    parts.append(f"✅ {passed_count} passed" if failed_count > 0 else "✅ All passed")
                if failed_count > 0:
                    parts.append(f"❌ {failed_count} failed")

                summary = f"Snapshot: {total}/{total} snapshots completed ({', '.join(parts)})"
                await ctx.report_progress(progress=total, total=total, message=summary)
            else:
                await ctx.report_progress(progress=0, total=0, message="0 snapshots matched selector")

        # Empty results means selector matched nothing - this is an error
        if not run_results.get("results"):
            raise RuntimeError(f"No snapshots matched selector: {select or 'all'}")

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

        runner = await self._get_runner()
        result = await runner.invoke(["deps"])

        if not result.success:
            raise RuntimeError(f"dbt deps failed: {result.exception}")

        # Parse installed packages from manifest
        installed_packages: set[str] = set()

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
            await self._ensure_initialized_with_context(ctx, force_parse=True)
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
            await self._ensure_initialized_with_context(ctx, force_parse=True)
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
            await self._ensure_initialized_with_context(ctx, force_parse=True)
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
            await self._ensure_initialized_with_context(ctx, force_parse=True)
            return await self.toolImpl_analyze_impact(name, resource_type)

        @self.app.tool()
        async def query_database(
            ctx: Context | None,
            sql: str,
            output_file: str | None = None,
            output_format: str = "json",
        ) -> dict[str, Any]:
            """Execute a SQL query against the dbt project's database.
                       await self._ensure_initialized_with_context(ctx)
            output_file is used
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
            return await self.toolImpl_query_database(ctx, sql, output_file, output_format)

        @self.app.tool()
        async def run_models(
            ctx: Context,
            select: str | None = None,
            exclude: str | None = None,
            select_state_modified: bool = False,
            select_state_modified_plus_downstream: bool = False,
            full_refresh: bool = False,
            fail_fast: bool = False,
            check_schema_changes: bool = False,
            cache_selected_only: bool = True,
        ) -> dict[str, Any]:
            """Run dbt models (compile SQL and execute against database).

            **What are models**: SQL files (.sql) containing SELECT statements that define data transformations.
            Models are compiled and executed to create/update tables and views in your database.

            **Important**: This tool runs models only (SQL files). For CSV seed files, use load_seeds().
            For running everything together (seeds + models + tests), use build_models().

            State-based selection modes (uses dbt state:modified selector):
            - select_state_modified: Run only models modified since last successful run (state:modified)
            - select_state_modified_plus_downstream: Run modified + downstream dependencies (state:modified+)
              Note: Requires select_state_modified=True

            Manual selection (alternative to state-based):
            - select: dbt selector syntax (e.g., "customers", "tag:mart", "stg_*")
            - exclude: Exclude specific models

            Args:
                select: Manual selector (e.g., "customers", "tag:mart", "path:marts/*")
                exclude: Exclude selector (e.g., "tag:deprecated")
                select_state_modified: Use state:modified selector (changed models only)
                select_state_modified_plus_downstream: Extend to state:modified+ (changed + downstream)
                full_refresh: Force full refresh of incremental models
                fail_fast: Stop execution on first failure
                check_schema_changes: Detect schema changes and recommend downstream runs
                cache_selected_only: Only cache schemas for selected models (default True for performance)

            Returns:
                Execution results with status, models run, timing info, and optional schema_changes

            See also:
                - seed_data(): Load CSV files (must run before models that reference them)
                - build_models(): Run models + tests together in DAG order
                - test_models(): Run tests after models complete

            Examples:
                # Run a specific model
                run_models(select="customers")

                # After loading seeds, run dependent models
                seed_data()
                run_models(select="stg_orders")

                # Incremental: run only what changed
                run_models(select_state_modified=True)

                # Run changed models + everything downstream
                run_models(select_state_modified=True, select_state_modified_plus_downstream=True)

                # Full refresh marts (rebuild from scratch)
                run_models(select="tag:mart", full_refresh=True)
            """
            await self._ensure_initialized_with_context(ctx)
            return await self.toolImpl_run_models(ctx, select, exclude, select_state_modified, select_state_modified_plus_downstream, full_refresh, fail_fast, check_schema_changes, cache_selected_only)

        @self.app.tool()
        async def test_models(
            ctx: Context,
            select: str | None = None,
            exclude: str | None = None,
            select_state_modified: bool = False,
            select_state_modified_plus_downstream: bool = False,
            fail_fast: bool = False,
        ) -> dict[str, Any]:
            """Run dbt tests on models and sources.

            **When to use**: After running models to validate data quality. Tests check constraints
            like uniqueness, not-null, relationships, and custom data quality rules.

            **Important**: Ensure seeds and models are built before running tests that depend on them.

            State-based selection modes (uses dbt state:modified selector):
            - select_state_modified: Test only models modified since last successful run (state:modified)
            - select_state_modified_plus_downstream: Test modified + downstream dependencies (state:modified+)
              Note: Requires select_state_modified=True

            Manual selection (alternative to state-based):
            - select: dbt selector syntax (e.g., "customers", "tag:mart", "test_type:generic")
            - exclude: Exclude specific tests

            Args:
                select: Manual selector for tests/models to test
                exclude: Exclude selector
                select_state_modified: Use state:modified selector (changed models only)
                select_state_modified_plus_downstream: Extend to state:modified+ (changed + downstream)
                fail_fast: Stop execution on first failure

            Returns:
                Test results with status and failures

            See also:
                - run_models(): Execute models before testing them
                - build_models(): Run models + tests together automatically
                - load_seeds(): Load seeds if tests reference seed data

            Examples:
                # After building a model, test it
                run_models(select="customers")
                test_models(select="customers")

                # Test only generic tests (not singular)
                test_models(select="test_type:generic")

                # Test everything that changed
                test_models(select_state_modified=True)

                # Stop on first failure for quick feedback
                test_models(fail_fast=True)

            Note: Unit test failures show diffs in the "daff" tabular format:
                @@ = column headers
                +++ = row in actual, not in expected (extra row)
                --- = row in expected, not in actual (missing row)
                → = row with modified cell(s), shown as old_value→new_value
                ... = omitted matching rows
                Full format spec: https://paulfitz.github.io/daff-doc/spec.html
            """
            await self._ensure_initialized_with_context(ctx)
            return await self.toolImpl_test_models(ctx, select, exclude, select_state_modified, select_state_modified_plus_downstream, fail_fast)

        @self.app.tool()
        async def build_models(
            ctx: Context,
            select: str | None = None,
            exclude: str | None = None,
            select_state_modified: bool = False,
            select_state_modified_plus_downstream: bool = False,
            full_refresh: bool = False,
            fail_fast: bool = False,
            cache_selected_only: bool = True,
        ) -> dict[str, Any]:
            """Run dbt build (execute models and tests together in correct dependency order).

            **When to use**: This is the recommended "do everything" command that runs seeds, models,
            snapshots, and tests in the correct order based on your DAG. It automatically handles
            dependencies, so you don't need to run load_seeds() → run_models() → test_models() separately.

            **How it works**: Executes resources in dependency order:
            1. Seeds (if selected)
            2. Models (with their upstream dependencies)
            3. Tests (after their parent models complete)
            4. Snapshots (if selected)

            State-based selection modes (uses dbt state:modified selector):
            - select_state_modified: Build only resources modified since last successful run (state:modified)
            - select_state_modified_plus_downstream: Build modified + downstream dependencies (state:modified+)
              Note: Requires select_state_modified=True

            Manual selection (alternative to state-based):
            - select: dbt selector syntax (e.g., "customers", "tag:mart", "stg_*")
            - exclude: Exclude specific models

            Args:
                select: Manual selector
                exclude: Exclude selector
                select_state_modified: Use state:modified selector (changed resources only)
                select_state_modified_plus_downstream: Extend to state:modified+ (changed + downstream)
                full_refresh: Force full refresh of incremental models
                fail_fast: Stop execution on first failure
                cache_selected_only: Only cache schemas for selected models (default True for performance)

            Returns:
                Build results with status, models run/tested, and timing info

            See also:
                - run_models(): Run only models (no tests)
                - test_models(): Run only tests
                - load_seeds(): Run only seeds

            Examples:
                # Full project build (first-time setup or comprehensive run)
                build_models()

                # Build only what changed (efficient incremental workflow)
                build_models(select_state_modified=True)

                # Build changed resources + everything downstream
                build_models(select_state_modified=True, select_state_modified_plus_downstream=True)

                # Build specific model and its dependencies + tests
                build_models(select="customers")

                # Build all marts (includes their seed dependencies automatically)
                build_models(select="tag:mart")

                # Quick feedback: stop on first test failure
                build_models(fail_fast=True)
            """
            await self._ensure_initialized_with_context(ctx)
            return await self.toolImpl_build_models(ctx, select, exclude, select_state_modified, select_state_modified_plus_downstream, full_refresh, fail_fast, cache_selected_only)

        @self.app.tool()
        async def load_seeds(
            ctx: Context,
            select: str | None = None,
            exclude: str | None = None,
            select_state_modified: bool = False,
            select_state_modified_plus_downstream: bool = False,
            full_refresh: bool = False,
            show: bool = False,
        ) -> dict[str, Any]:
            """Load seed data (CSV files) from seeds/ directory into database tables.

            **When to use**: Run this before building models or tests that depend on reference data.
            Seeds must be loaded before models that reference them can execute.

            **What are seeds**: CSV files containing static reference data (country codes,
            product categories, lookup tables, etc.). Unlike models (which are .sql files),
            seeds are CSV files that are loaded directly into database tables.

            State-based selection modes (detects changed CSV files):
            - select_state_modified: Load only seeds modified since last successful run (state:modified)
            - select_state_modified_plus_downstream: Load modified + downstream dependencies (state:modified+)
              Note: Requires select_state_modified=True

            Manual selection (alternative to state-based):
            - select: dbt selector syntax (e.g., "raw_customers", "tag:lookup")
            - exclude: Exclude specific seeds

            Important: Change detection for seeds works via file hash comparison:
            - Seeds < 1 MiB: Content hash is compared (recommended)
            - Seeds >= 1 MiB: Only file path changes are detected (content changes ignored)
            For large seeds, use manual selection or run all seeds.

            Args:
                select: Manual selector for seeds
                exclude: Exclude selector
                select_state_modified: Use state:modified selector (changed seeds only)
                select_state_modified_plus_downstream: Extend to state:modified+ (changed + downstream)
                full_refresh: Truncate and reload seed tables (default behavior)
                show: Show preview of loaded data

            Returns:
                Seed results with status and loaded seed info

            See also:
                - run_models(): Execute .sql model files (not CSV seeds)
                - build_models(): Runs both seeds and models together in DAG order
                - test_models(): Run tests (requires seeds to be loaded first if tests reference them)

            Examples:
                # Before running tests that depend on reference data
                load_seeds()
                test_models(select="test_customer_country_code")

                # After adding a new CSV lookup table
                load_seeds(select="new_product_categories")

                # Fix "relation does not exist" errors from models referencing seeds
                load_seeds()  # Load missing seed tables first
                run_models(select="stg_orders")

                # Incremental workflow: only reload what changed
                load_seeds(select_state_modified=True)

                # Full refresh of a specific seed
                load_seeds(select="country_codes", full_refresh=True)
            """
            await self._ensure_initialized_with_context(ctx)
            return await self.toolImpl_seed_data(ctx, select, exclude, select_state_modified, select_state_modified_plus_downstream, full_refresh, show)

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

            Note: Snapshots do not support state-based selection (select_state_modified*)
                because they are time-dependent, not change-dependent.
            """
            await self._ensure_initialized_with_context(ctx)
            return await self.toolImpl_snapshot_models(ctx, select, exclude)

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


def create_server(project_dir: str | None = None, timeout: float | None = None) -> DbtCoreMcpServer:
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
