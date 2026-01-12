"""
dbt Core MCP Server Implementation.

This server provides tools for interacting with dbt projects via the Model Context Protocol.
"""

import asyncio
import importlib
import json
import logging
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable
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

# Type alias for progress reporting callbacks
ProgressCallback = Callable[[int, int, str], Awaitable[None]]

logger = logging.getLogger(__name__)


@dataclass
class SharedState:
    """Shared state accessible to all tools."""

    app: FastMCP
    project_dir: Path | None
    profiles_dir: str
    timeout: float | None
    runner: BridgeRunner | None
    manifest: ManifestLoader | None
    adapter_type: str | None
    force_fresh_runner: bool
    _init_lock: asyncio.Lock
    _explicit_project_dir: Path | None
    server: "DbtCoreMcpServer | None" = None

    async def ensure_initialized(self, ctx: Any, force_parse: bool = False) -> None:
        """Ensure server is initialized (delegates to server instance)."""
        if self.server:
            await self.server.ensure_initialized_with_context(ctx, force_parse=force_parse)

    async def get_runner(self) -> BridgeRunner:
        """Get BridgeRunner instance (delegates to server instance)."""
        if self.server:
            return await self.server.get_runner()
        raise RuntimeError("Server not initialized")

    def parse_run_results(self) -> dict[str, Any]:
        """Parse run results from manifest (delegates to server instance)."""
        if self.server:
            return self.server.parse_run_results()
        raise RuntimeError("Server not initialized")

    async def prepare_state_based_selection(
        self,
        select_state_modified: bool,
        select_state_modified_plus_downstream: bool,
        select: str | None,
    ) -> str | None:
        """Delegate to server to build state:modified selector."""
        if self.server:
            return await self.server.prepare_state_based_selection(select_state_modified, select_state_modified_plus_downstream, select)
        raise RuntimeError("Server not initialized")

    def clear_stale_run_results(self) -> None:
        """Delegate to server to remove stale run_results.json."""
        if self.server:
            self.server.clear_stale_run_results()
            return
        raise RuntimeError("Server not initialized")

    async def save_execution_state(self) -> None:
        """Delegate to server to persist execution state for state comparison."""
        if self.server:
            await self.server.save_execution_state()
            return
        raise RuntimeError("Server not initialized")

    def validate_and_parse_results(self, result: Any, command_name: str) -> dict[str, Any]:
        """Delegate to server to parse dbt results safely."""
        if self.server:
            return self.server.validate_and_parse_results(result, command_name)
        raise RuntimeError("Server not initialized")

    async def report_final_progress(
        self,
        ctx: Context | None,
        results_list: list[dict[str, Any]],
        command_name: str,
        resource_type: str,
    ) -> None:
        """Delegate to server to emit final progress summary."""
        if self.server:
            await self.server.report_final_progress(ctx, results_list, command_name, resource_type)
            return
        raise RuntimeError("Server not initialized")

    async def get_table_schema_from_db(self, model_name: str, source_name: str | None = None) -> list[dict[str, Any]]:
        """Delegate to server to fetch table schema from database."""
        if self.server:
            return await self.server.get_table_schema_from_db(model_name, source_name)
        raise RuntimeError("Server not initialized")

    async def get_table_columns_from_db(self, model_name: str) -> list[str]:
        """Delegate to server to fetch table columns from database."""
        if self.server:
            return await self.server.get_table_columns_from_db(model_name)
        raise RuntimeError("Server not initialized")


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
        _explicit_project_dir = Path(project_dir) if project_dir else None
        project_dir_resolved: Path | None = None
        profiles_dir = os.path.expanduser("~/.dbt")

        # Create shared state with all dbt components
        self.state = SharedState(
            app=self.app,
            project_dir=project_dir_resolved,
            profiles_dir=profiles_dir,
            timeout=timeout,
            runner=None,
            manifest=None,
            adapter_type=None,
            force_fresh_runner=False,  # Set to False to reuse runners for performance
            _init_lock=asyncio.Lock(),
            _explicit_project_dir=_explicit_project_dir,
        )

        # Set back-reference for delegation
        self.state.server = self

        # Keep references for backward compatibility with existing helper methods
        self.project_dir = project_dir_resolved
        self.profiles_dir = profiles_dir
        self.timeout = timeout
        self._explicit_project_dir = _explicit_project_dir
        self.runner = None
        self.manifest = None
        self.adapter_type = None
        self.force_fresh_runner = False
        self._init_lock = asyncio.Lock()

        # Add built-in FastMCP middleware (2.11.0)
        self.app.add_middleware(ErrorHandlingMiddleware())  # Handle errors first
        self.app.add_middleware(RateLimitingMiddleware(max_requests_per_second=50))
        # TimingMiddleware and LoggingMiddleware removed - they use structlog with column alignment
        # which causes formatting issues in VS Code's output panel

        # Register tools via auto-discovery
        self._register_tools()

        logger.info("dbt Core MCP Server initialized")
        logger.info(f"Profiles directory: {self.profiles_dir}")

    # Public wrappers for SharedState to avoid private-member access warnings
    async def ensure_initialized_with_context(self, ctx: Context | None, force_parse: bool = False) -> None:
        await self._ensure_initialized_with_context(ctx, force_parse=force_parse)

    async def get_runner(self) -> BridgeRunner:
        return await self._get_runner()

    def parse_run_results(self) -> dict[str, Any]:
        return self._parse_run_results()

    async def prepare_state_based_selection(
        self,
        select_state_modified: bool,
        select_state_modified_plus_downstream: bool,
        select: str | None,
    ) -> str | None:
        return await self._prepare_state_based_selection(select_state_modified, select_state_modified_plus_downstream, select)

    def clear_stale_run_results(self) -> None:
        self._clear_stale_run_results()

    async def save_execution_state(self) -> None:
        await self._save_execution_state()

    def validate_and_parse_results(self, result: Any, command_name: str) -> dict[str, Any]:
        return self._validate_and_parse_results(result, command_name)

    async def report_final_progress(
        self,
        ctx: Context | None,
        results_list: list[dict[str, Any]],
        command_name: str,
        resource_type: str,
    ) -> None:
        await self._report_final_progress(ctx, results_list, command_name, resource_type)

    async def get_table_schema_from_db(self, model_name: str, source_name: str | None = None) -> list[dict[str, Any]]:
        return await self._get_table_schema_from_db(model_name, source_name)

    async def get_table_columns_from_db(self, model_name: str) -> list[str]:
        return await self._get_table_columns_from_db(model_name)

    # ------------------------------------------------------------------
    # Legacy toolImpl_* compatibility wrappers (used by tests)
    # ------------------------------------------------------------------
    async def toolImpl_analyze_impact(self, name: str, resource_type: str | None = None) -> dict[str, Any]:
        from .tools import analyze_impact

        await self.state.ensure_initialized(ctx=None, force_parse=True)
        return await analyze_impact._implementation(name, resource_type, self.state)  # pyright: ignore[reportPrivateUsage]

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
        from .tools import build_models

        await self.state.ensure_initialized(ctx, force_parse=False)
        return await build_models._implementation(  # pyright: ignore[reportPrivateUsage]
            ctx,
            select,
            exclude,
            select_state_modified,
            select_state_modified_plus_downstream,
            full_refresh,
            fail_fast,
            cache_selected_only,
            self.state,
        )

    async def toolImpl_get_lineage(
        self,
        name: str,
        resource_type: str | None = None,
        direction: str = "both",
        depth: int | None = None,
    ) -> dict[str, Any]:
        from .tools import get_lineage

        await self.state.ensure_initialized(ctx=None, force_parse=True)
        return await get_lineage._implementation(name, resource_type, direction, depth, self.state)  # pyright: ignore[reportPrivateUsage]

    async def toolImpl_get_project_info(self, run_debug: bool = True) -> dict[str, Any]:
        from .tools import get_project_info

        await self.state.ensure_initialized(ctx=None, force_parse=True)
        return await get_project_info._implementation(run_debug, self.state)  # pyright: ignore[reportPrivateUsage]

    async def toolImpl_get_resource_info(
        self,
        name: str,
        resource_type: str | None = None,
        include_database_schema: bool = False,
        include_compiled_sql: bool = False,
    ) -> dict[str, Any]:
        from .tools import get_resource_info

        await self.state.ensure_initialized(ctx=None, force_parse=True)
        return await get_resource_info._implementation(  # pyright: ignore[reportPrivateUsage]
            name,
            resource_type,
            include_database_schema,
            include_compiled_sql,
            self.state,
        )

    async def toolImpl_install_deps(self) -> dict[str, Any]:
        from .tools import install_deps

        await self.state.ensure_initialized(ctx=None, force_parse=False)
        return await install_deps._implementation(self.state)  # pyright: ignore[reportPrivateUsage]

    async def toolImpl_list_resources(self, resource_type: str | None = None) -> list[dict[str, Any]]:
        from .tools import list_resources

        await self.state.ensure_initialized(ctx=None, force_parse=True)
        return await list_resources._implementation(resource_type, self.state)  # pyright: ignore[reportPrivateUsage]

    async def toolImpl_query_database(
        self,
        ctx: Context | None,
        sql: str,
        output_file: str | None = None,
        output_format: str = "json",
    ) -> dict[str, Any]:
        from .tools import query_database

        await self.state.ensure_initialized(ctx, force_parse=False)
        return await query_database._implementation(ctx, sql, output_file, output_format, self.state)  # pyright: ignore[reportPrivateUsage]

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
        from .tools import run_models

        await self.state.ensure_initialized(ctx, force_parse=False)
        return await run_models._implementation(  # pyright: ignore[reportPrivateUsage]
            ctx,
            select,
            exclude,
            select_state_modified,
            select_state_modified_plus_downstream,
            full_refresh,
            fail_fast,
            check_schema_changes,
            cache_selected_only,
            self.state,
        )

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
        from .tools import load_seeds

        await self.state.ensure_initialized(ctx, force_parse=False)
        return await load_seeds._implementation(  # pyright: ignore[reportPrivateUsage]
            ctx,
            select,
            exclude,
            select_state_modified,
            select_state_modified_plus_downstream,
            full_refresh,
            show,
            self.state,
        )

    async def toolImpl_snapshot_models(
        self,
        ctx: Context | None,
        select: str | None = None,
        exclude: str | None = None,
    ) -> dict[str, Any]:
        from .tools import snapshot_models

        await self.state.ensure_initialized(ctx, force_parse=False)
        return await snapshot_models._implementation(  # pyright: ignore[reportPrivateUsage]
            ctx,
            select,
            exclude,
            self.state,
        )

    async def toolImpl_test_models(
        self,
        ctx: Context | None,
        select: str | None = None,
        exclude: str | None = None,
        select_state_modified: bool = False,
        select_state_modified_plus_downstream: bool = False,
        fail_fast: bool = False,
    ) -> dict[str, Any]:
        from .tools import test_models

        await self.state.ensure_initialized(ctx, force_parse=False)
        return await test_models._implementation(  # pyright: ignore[reportPrivateUsage]
            ctx,
            select,
            exclude,
            select_state_modified,
            select_state_modified_plus_downstream,
            fail_fast,
            self.state,
        )

    def _register_tools(self) -> None:
        """Auto-discover and register all tools from tools/ directory.

        Scans tools/ for .py files, imports them, and calls their setup() function.
        Errors in tool loading don't crash server startup.
        """
        tools_dir = Path(__file__).parent / "tools"

        if not tools_dir.exists():
            logger.warning(f"Tools directory not found: {tools_dir}")
            return

        # Find all .py files in tools/ (excluding __init__.py and __pycache__)
        tool_files = sorted(tools_dir.glob("*.py"))

        if not tool_files:
            logger.warning(f"No tool files found in {tools_dir}")
            return

        for tool_file in tool_files:
            if tool_file.name.startswith("_"):
                continue

            # Dynamically import the module
            module_name = f"dbt_core_mcp.tools.{tool_file.stem}"
            try:
                module = importlib.import_module(module_name)

                # Look for setup() function
                if hasattr(module, "setup") and callable(module.setup):
                    # Call setup - it registers the tool with FastMCP
                    module.setup(self.app, self.state)
                    logger.info(f"Registered tool from {tool_file.name}")
                else:
                    logger.warning(f"No setup() function found in {module_name}")
            except Exception as e:
                logger.error(f"Failed to load tool from {tool_file.name}: {e}")
                # Don't fail server startup if a tool fails to load
                continue

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

        # Keep shared state in sync with server-owned components
        self._sync_shared_state()

        logger.info("dbt components initialized successfully")

    def _sync_shared_state(self) -> None:
        """Keep SharedState references aligned with server fields."""
        self.state.project_dir = self.project_dir
        self.state.runner = self.runner
        self.state.manifest = self.manifest
        self.state.adapter_type = self.adapter_type
        self.state.force_fresh_runner = self.force_fresh_runner

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
