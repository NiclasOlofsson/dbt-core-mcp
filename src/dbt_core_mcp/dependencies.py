"""Dependency injection providers for dbt Core MCP tools.

This module provides dependency functions that can be used with FastMCP's Depends()
to inject shared state and context into tool functions.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .context import DbtCoreServerContext

# Global state reference - set by server during initialization
_server_state: "DbtCoreServerContext | None" = None


def set_server_state(state: "DbtCoreServerContext") -> None:
    """Set the global server state reference.

    Called by DbtCoreMcpServer during initialization.

    Args:
        state: The DbtCoreServerContext instance to make available to tools
    """
    global _server_state
    _server_state = state


def get_state() -> "DbtCoreServerContext":
    """Dependency provider for server state.

    Used with FastMCP's Depends() to inject DbtCoreServerContext into tools.

    Returns:
        The shared DbtCoreServerContext instance

    Raises:
        RuntimeError: If server state has not been initialized

    Example:
        @tool
        async def my_tool(
            ctx: Context,
            state: DbtCoreServerContext = Depends(get_state),
        ) -> dict:
            # state is automatically injected
            pass
    """
    if _server_state is None:
        raise RuntimeError("Server state not initialized")
    return _server_state
