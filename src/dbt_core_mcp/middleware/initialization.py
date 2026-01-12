"""Initialization middleware for dbt Core MCP Server.

Ensures dbt components are initialized before any tool execution.
"""

import logging
from typing import Any

from fastmcp.server.middleware import CallNext, Middleware, MiddlewareContext

logger = logging.getLogger(__name__)


class InitializationMiddleware(Middleware):
    """Middleware that ensures dbt initialization before tool execution.

    This middleware automatically calls ensure_initialized() on the server context
    before any tool is executed, removing the need for individual tools to handle
    initialization.
    """

    def __init__(self, state: Any) -> None:
        """Initialize the middleware with server context.

        Args:
            state: DbtCoreServerContext instance (typed as Any to avoid circular import)
        """
        self.state = state
        logger.debug("InitializationMiddleware registered")

    async def __call__(
        self,
        context: MiddlewareContext,
        call_next: CallNext,
    ) -> Any:
        """Process the request by ensuring initialization before delegating to tool.

        Args:
            context: Middleware context for the current request
            call_next: Next middleware or tool handler in the chain

        Returns:
            Result from the tool handler
        """
        # Ensure dbt components are initialized before tool execution
        # Use force_parse=False to allow tools to override if needed
        await self.state.ensure_initialized(context.fastmcp_context, force_parse=False)

        # Continue to next handler (next middleware or the actual tool)
        return await call_next(context)
