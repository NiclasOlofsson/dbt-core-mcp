"""
DBT Core MCP Server Implementation.

This server provides tools for interacting with DBT projects via the Model Context Protocol.
"""

import logging
import os
from pathlib import Path
from typing import Optional

from fastmcp import FastMCP
from fastmcp.server.middleware.error_handling import ErrorHandlingMiddleware
from fastmcp.server.middleware.logging import LoggingMiddleware
from fastmcp.server.middleware.rate_limiting import RateLimitingMiddleware
from fastmcp.server.middleware.timing import TimingMiddleware

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

        # Initialize DBT components
        self.runner: BridgeRunner | None = None
        self.manifest: ManifestLoader | None = None
        self.adapter_type: str | None = None

        if self.project_dir:
            self._initialize_dbt_components()

        # Add built-in FastMCP middleware (2.11.0)
        self.app.add_middleware(ErrorHandlingMiddleware())  # Handle errors first
        self.app.add_middleware(RateLimitingMiddleware(max_requests_per_second=50))
        self.app.add_middleware(TimingMiddleware())  # Time actual execution
        self.app.add_middleware(LoggingMiddleware(include_payloads=True, max_payload_length=1000))

        # Register tools
        self._register_tools()

        logger.info("DBT Core MCP Server initialized")
        if self.project_dir:
            logger.info(f"Project directory: {self.project_dir}")
            logger.info(f"Adapter type: {self.adapter_type}")
        else:
            logger.info("Project directory will be set from MCP workspace roots")
        logger.info(f"Profiles directory: {self.profiles_dir}")

    def _initialize_dbt_components(self) -> None:
        """Initialize DBT runner and manifest loader."""
        if not self.project_dir:
            raise RuntimeError("Project directory not set")

        # Detect Python command for user's environment
        python_cmd = detect_python_command(self.project_dir)
        logger.info(f"Detected Python command: {python_cmd}")

        # Detect DBT adapter type
        self.adapter_type = detect_dbt_adapter(self.project_dir)
        logger.info(f"Detected adapter: {self.adapter_type}")

        # Create bridge runner
        self.runner = BridgeRunner(self.project_dir, python_cmd)

        # Run parse to generate/update manifest
        logger.info("Running dbt parse to generate manifest...")
        result = self.runner.invoke(["parse"])
        if not result.success:
            error_msg = str(result.exception) if result.exception else "Unknown error"
            raise RuntimeError(f"Failed to parse DBT project: {error_msg}")

        # Initialize manifest loader
        manifest_path = self.runner.get_manifest_path()
        self.manifest = ManifestLoader(manifest_path)
        self.manifest.load()

        logger.info("DBT components initialized successfully")

    def _register_tools(self) -> None:
        """Register all DBT tools."""

        @self.app.tool()
        def get_project_info() -> dict[str, object]:
            """Get information about the DBT project.

            Returns:
                Dictionary with project information
            """
            if not self.manifest:
                return {
                    "project_dir": str(self.project_dir) if self.project_dir else None,
                    "profiles_dir": self.profiles_dir,
                    "status": "not_initialized",
                    "message": "DBT components not initialized. Provide a project directory.",
                }

            # Get project info from manifest
            info = self.manifest.get_project_info()
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
            if not self.manifest:
                raise RuntimeError("DBT components not initialized. Provide a project directory.")

            models = self.manifest.get_models()
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
        def list_sources() -> list[dict[str, object]]:
            """List all sources in the DBT project.

            Returns:
                List of source information dictionaries
            """
            if not self.manifest:
                raise RuntimeError("DBT components not initialized. Provide a project directory.")

            sources = self.manifest.get_sources()
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

        logger.info("Registered DBT tools")

    def run(self) -> None:
        """Run the MCP server."""
        self.app.run()


def create_server(project_dir: Optional[str] = None) -> DBTCoreMCPServer:
    """Create a new DBT Core MCP server instance.

    Args:
        project_dir: Optional path to DBT project directory for testing.
                     If not provided, uses MCP workspace roots.

    Returns:
        DBTCoreMCPServer instance
    """
    return DBTCoreMCPServer(project_dir=project_dir)
