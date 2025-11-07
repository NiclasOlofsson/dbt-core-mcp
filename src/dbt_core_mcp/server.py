"""
DBT Core MCP Server Implementation.

This server provides tools for interacting with DBT projects via the Model Context Protocol.
"""

import logging
import os
from pathlib import Path
from typing import Optional

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
        def get_model_info(name: str) -> dict[str, object]:
            """Get detailed information about a specific DBT model.

            Returns the complete manifest node for a model, including all metadata,
            columns, configuration, dependencies, and more. Excludes raw_code to keep
            context lightweight (use file path to read SQL when needed).

            Args:
                name: The name of the model

            Returns:
                Complete model information dictionary from the manifest (without raw_code)
            """
            self._ensure_initialized()

            try:
                node = self.manifest.get_model_node(name)  # type: ignore
                # Remove raw_code to keep context lightweight
                node_copy = dict(node)
                node_copy.pop("raw_code", None)
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
