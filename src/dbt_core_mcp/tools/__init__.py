"""dbt-core-mcp tools package.

Each tool module implements a single MCP tool with:
- setup(app, state): Registers the tool with FastMCP
- _implementation(...): Pure logic function (testable)
"""

from typing import Callable, TypeVar, Any

F = TypeVar("F", bound=Callable[..., Any])


def dbtTool() -> Callable[[F], F]:
    """Decorator to mark dbt MCP tool functions.
    
    This is a no-op decorator - actual tool registration happens in server.py.
    Used for documentation and code clarity.
    """
    def decorator(func: F) -> F:
        return func
    return decorator


__all__ = ["dbtTool"]
