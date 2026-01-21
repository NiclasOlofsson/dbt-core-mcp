"""Simple tool for MCP UI resource testing."""

import logging
from typing import Any

from fastmcp.server.context import Context

from .. import dbtTool

logger = logging.getLogger(__name__)


@dbtTool(
    description="Render the demo UI resource (resource://demo/hello).",
    meta={
        "ui": {
            "resourceUri": "resource://demo/hello",
        }
    },
)
async def demo_ui(_ctx: Context) -> dict[str, Any]:
    """Render the demo UI resource (resource://demo/hello)."""
    logger.info("demo_ui tool called")
    return {
        "message": "Demo UI tool invoked",
    }


# @dbtTool(description="Return basic client context metadata for diagnostics.")
# async def demo_client_context(ctx: Context) -> dict[str, Any]:
#     """Return basic client context metadata for diagnostics."""
#     logger.info("demo_client_context tool called")
#     meta = getattr(getattr(ctx, "request_context", None), "meta", None)
#     meta_value = meta if meta is None else getattr(meta, "__dict__", meta)
#     return {
#         "client_id": getattr(ctx, "client_id", None),
#         "request_id": getattr(ctx, "request_id", None),
#         "session_id": getattr(ctx, "session_id", None),
#         "has_sampling_capability": getattr(ctx, "has_sampling_capability", None),
#         "request_meta": meta_value,
#     }
