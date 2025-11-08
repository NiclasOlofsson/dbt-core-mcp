"""
dbt Core MCP Server.

This package provides an MCP server for interacting with dbt projects
via the Model Context Protocol.
"""

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as pkg_version

from .server import create_server

try:
    __version__ = pkg_version("dbt-core-mcp")
except PackageNotFoundError:  # pragma: no cover - when not installed
    __version__ = "0.0.0"

__all__ = ["create_server", "__version__"]
