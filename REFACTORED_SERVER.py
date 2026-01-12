"""
dbt Core MCP Server Implementation (Refactored).

This server provides tools for interacting with dbt projects via the Model Context Protocol.
Uses auto-discovery to load tools from the tools/ directory.
"""

import asyncio
import importlib
import json
import logging
import os
from dataclasses import dataclass, field
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
    """Shared state accessible to all tools.

    Contains all instance state that was previously scattered across DbtCoreMcpServer.
    Methods moved from DbtCoreMcpServer are now here for better organization.
    """

    app: FastMCP
    project_dir: Path | None
    profiles_dir: str
    timeout: float | None
    runner: BridgeRunner | None = None
    manifest: ManifestLoader | None = None
    adapter_type: str | None = None
    force_fresh_runner: bool = False
    _init_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    _explicit_project_dir: Path | None = None

    async def ensure_initialized(self, ctx: Any, force_parse: bool = False) -> None:
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

    async def get_runner(self) -> BridgeRunner:
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

    def parse_run_results(self) -> dict[str, Any]:
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

    def _detect_project_dir(self) -> Path:
        """Detect the dbt project directory.

        Resolution order:
        1. Use explicit project_dir if provided during initialization
        2. Fall back to current working directory

        Returns:
            Path to the dbt project directory
        """
        if self._explicit_project_dir:
            logger.debug(f"Using explicit project directory: {self._explicit_project_dir}")
            return self._explicit_project_dir

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
                    uri_path = roots[0].uri.path if hasattr(roots[0].uri, "path") else str(roots[0].uri)
                    if uri_path:
                        workspace_root = Path(url2pathname(unquote(uri_path)))
                        logger.info(f"Detected workspace root from MCP client: {workspace_root}")
                        return workspace_root
        except Exception as e:
            logger.debug(f"Could not access workspace roots: {e}")

        return None

    def _manifest_exists(self) -> bool:
        """Check if manifest.json exists."""
        if self.project_dir is None:
            return False
        manifest_path = self.project_dir / "target" / "manifest.json"
        return manifest_path.exists()

    async def _initialize_dbt_components(self, needs_parse: bool = True, force_parse: bool = False) -> None:
        """Initialize dbt runner and manifest loader.

        Args:
            needs_parse: Whether to run dbt parse.
            force_parse: If True, force parsing even if manifest exists.
        """
        if not self.project_dir:
            raise RuntimeError("Project directory not set")

        runner = await self.get_runner()

        should_parse = needs_parse or force_parse
        if should_parse:
            if not self._manifest_exists():
                logger.info("No manifest found - running initial dbt parse...")
            else:
                logger.info("Force parse requested - running dbt parse for fresh data...")
            parse_args = ["parse"]
            result = await runner.invoke(parse_args)
            if not result.success:
                error_msg = str(result.exception) if result.exception else "Unknown error"
                raise RuntimeError(f"Failed to parse dbt project: {error_msg}")
        else:
            logger.info("Manifest exists and no force parse - tools will handle parsing as needed")

        manifest_path = runner.get_manifest_path()
        if not self.manifest:
            self.manifest = ManifestLoader(manifest_path)
        await self.manifest.load()

        logger.info("dbt components initialized successfully")


class DbtCoreMcpServer:
    """dbt Core MCP Server.

    Provides tools for interacting with dbt projects via auto-discovery.
    """

    def __init__(self, project_dir: str | None = None, timeout: float | None = None) -> None:
        """Initialize the server.

        Args:
            project_dir: Optional path to dbt project directory.
            timeout: Optional timeout in seconds for dbt commands.
        """
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
            include_fastmcp_meta=True,
        )

        # Create shared state containing all instance variables
        self.state = SharedState(
            app=self.app,
            project_dir=Path(project_dir) if project_dir else None,
            profiles_dir=os.path.expanduser("~/.dbt"),
            timeout=timeout,
            runner=None,
            manifest=None,
            adapter_type=None,
            force_fresh_runner=False,
            _init_lock=asyncio.Lock(),
            _explicit_project_dir=Path(project_dir) if project_dir else None,
        )

        # Add built-in FastMCP middleware
        self.app.add_middleware(ErrorHandlingMiddleware())
        self.app.add_middleware(RateLimitingMiddleware(max_requests_per_second=50))

        # Auto-discover and register tools
        self._register_tools()

        logger.info("dbt Core MCP Server initialized")
        logger.info(f"Profiles directory: {self.state.profiles_dir}")

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

    def get_project_paths(self) -> dict[str, list[str]]:
        """Read configured paths from dbt_project.yml.

        Returns:
            Dictionary with path types as keys and lists of paths as values
        """
        if not self.state.project_dir:
            return {}

        project_file = self.state.project_dir / "dbt_project.yml"
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

    async def report_final_progress(
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
            command_name: Command prefix for message
            resource_type: Resource type for message
        """
        if not ctx:
            return

        if not results_list:
            await ctx.report_progress(progress=0, total=0, message=f"0 {resource_type} matched selector")
            return

        total = len(results_list)
        passed_count = sum(1 for r in results_list if r.get("status") in ("success", "pass"))
        failed_count = sum(1 for r in results_list if r.get("status") in ("error", "fail"))
        skip_count = sum(1 for r in results_list if r.get("status") in ("skipped", "skip"))
        warn_count = sum(1 for r in results_list if r.get("status") == "warn")

        parts = []
        if passed_count > 0:
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


def create_server(project_dir: str | None = None, timeout: float | None = None) -> DbtCoreMcpServer:
    """Factory function to create and return a DbtCoreMcpServer instance.

    Args:
        project_dir: Optional path to dbt project directory
        timeout: Optional timeout in seconds for dbt commands

    Returns:
        Configured DbtCoreMcpServer instance
    """
    return DbtCoreMcpServer(project_dir=project_dir, timeout=timeout)
