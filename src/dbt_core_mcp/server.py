"""
dbt Core MCP Server Implementation.

This server provides tools for interacting with dbt projects via the Model Context Protocol.
"""

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator
from urllib.parse import unquote
from urllib.request import url2pathname

from fastmcp import FastMCP
from fastmcp.server.context import Context
from fastmcp.server.middleware.error_handling import ErrorHandlingMiddleware
from fastmcp.server.middleware.rate_limiting import RateLimitingMiddleware

from .context import DbtCoreServerContext
from .dbt.bridge_runner import BridgeRunner
from .dbt.manifest import ManifestLoader
from .dependencies import set_server_state

# Import tools for static registration
from .utils.env_detector import detect_python_command

# Re-export DbtCoreServerContext for tools
__all__ = ["DbtCoreServerContext", "DbtCoreMcpServer", "create_server"]

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

        @asynccontextmanager
        async def lifespan(app: FastMCP) -> AsyncIterator[None]:
            """Lifespan context manager for server startup and shutdown."""
            # Startup
            logger.info("dbt Core MCP Server starting up...")
            yield
            # Shutdown
            logger.info("dbt Core MCP Server shutting down...")
            if self.runner:
                logger.info("Stopping persistent dbt process...")
                await self.runner.shutdown()
                logger.info("Persistent dbt process stopped successfully")
            logger.info("dbt Core MCP Server shutdown complete")

        self.app = FastMCP(
            version=__version__,
            name="dbt Core MCP",
            lifespan=lifespan,
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
            include_fastmcp_meta=True,  # Include FastMCP metadata for clients
        )  # type: ignore[arg-type]

        # Store the explicit project_dir if provided, otherwise will detect from workspace roots
        _explicit_project_dir = Path(project_dir) if project_dir else None
        project_dir_resolved: Path | None = None
        profiles_dir = os.path.expanduser("~/.dbt")

        # Parse experimental features flag from environment
        experimental_features = os.getenv("EXPERIMENTAL_FEATURES", "false").lower() == "true"

        # Create shared state with all dbt components
        self.state = DbtCoreServerContext(
            app=self.app,
            project_dir=project_dir_resolved,
            profiles_dir=profiles_dir,
            timeout=timeout,
            runner=None,
            manifest=None,
            adapter_type=None,
            force_fresh_runner=False,  # Set to False to reuse runners for performance
            experimental_features=experimental_features,
            _init_lock=asyncio.Lock(),
            _explicit_project_dir=_explicit_project_dir,
        )

        # Set back-reference for delegation
        self.state.server = self
        set_server_state(self.state)

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

        # Register tools statically
        self._register_tools()

        logger.info("dbt Core MCP Server initialized")
        logger.info(f"Profiles directory: {self.profiles_dir}")

    # Public wrappers for DbtCoreServerContext to avoid private-member access warnings
    async def ensure_initialized_with_context(self, ctx: Context | None, force_parse: bool = False) -> None:
        await self._ensure_initialized_with_context(ctx, force_parse=force_parse)

    async def get_runner(self) -> BridgeRunner:
        return await self._get_runner()

    def _register_tools(self) -> None:
        """Manually register all dbt Core MCP tools."""
        # Import all tool modules
        from .tools import analyze_impact, build_models, get_lineage, get_project_info, get_resource_info, install_deps, list_resources, load_seeds, query_database, run_models, snapshot_models, test_models

        # Register each tool
        self.app.tool()(analyze_impact.analyze_impact)
        self.app.tool()(build_models.build_models)
        self.app.tool()(get_lineage.get_lineage)
        self.app.tool()(get_project_info.get_project_info)
        self.app.tool()(get_resource_info.get_resource_info)
        self.app.tool()(install_deps.install_deps)
        self.app.tool()(list_resources.list_resources)
        self.app.tool()(load_seeds.load_seeds)
        self.app.tool()(query_database.query_database)
        self.app.tool()(run_models.run_models)
        self.app.tool()(snapshot_models.snapshot_models)
        self.app.tool()(test_models.test_models)

    # (Removed) Legacy toolImpl_* wrappers: tools are now auto-discovered via FileSystemProvider

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

        # Run parse if needed and not skipped via force_parse=False
        if needs_parse or force_parse:
            logger.info("Running dbt parse...")
            parse_args = ["parse"]  # Use partial parse for efficiency
            result = await runner.invoke(parse_args)
            if not result.success:
                error_msg = str(result.exception) if result.exception else "Unknown error"
                raise RuntimeError(f"Failed to parse dbt project: {error_msg}")

        # Initialize or reload manifest loader
        manifest_path = runner.get_manifest_path()
        if not self.manifest:
            self.manifest = ManifestLoader(manifest_path)
        await self.manifest.load()

        # Keep shared state in sync with server-owned components
        self._sync_shared_state()

        logger.info("dbt components initialized successfully")

    def _sync_shared_state(self) -> None:
        """Keep DbtCoreServerContext references aligned with server fields."""
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

            await self._initialize_dbt_components(needs_parse=not self.state.manifest_exists(), force_parse=force_parse)

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
