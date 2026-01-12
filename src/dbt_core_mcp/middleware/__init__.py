"""Middleware package for dbt-core-mcp server.

Provides request interceptors and handlers for the MCP server.
"""

from .initialization import InitializationMiddleware

__all__ = ["InitializationMiddleware"]
