"""dbt-core-mcp tools package.

Each tool module implements a single MCP tool with:
- setup(app, state): Registers the tool with FastMCP
- _implementation(...): Pure logic function (testable)
"""

import importlib
import inspect
import pkgutil
from pathlib import Path
from typing import Any, Callable, TypeVar

F = TypeVar("F", bound=Callable[..., Any])


def dbtTool(**metadata: Any) -> Callable[[F], F]:
    """Decorator to mark dbt MCP tool functions.

    Stores optional metadata on the function for later use during registration.
    Actual tool registration happens in server.py.
    """

    def decorator(func: F) -> F:
        func.metadata = metadata  # type: ignore[attr-defined]
        return func

    return decorator


def get_tool_metadata(func: Callable[..., Any], default: Any = None) -> Any:
    return getattr(func, "metadata", default)


def has_tool_metadata(func: Callable[..., Any]) -> bool:
    return hasattr(func, "metadata")


def set_tool_metadata(func: Callable[..., Any], metadata: Any) -> None:
    func.metadata = metadata  # type: ignore[attr-defined]


def discover_tools(module: Any) -> list[Callable[..., Any]]:
    tools: list[Callable[..., Any]] = []
    for _, obj in inspect.getmembers(module):
        if (inspect.isfunction(obj) or inspect.iscoroutinefunction(obj)) and has_tool_metadata(obj):
            tools.append(obj)
    return tools


def discover_tools_in_path(path: str | Path, package_prefix: str) -> list[Callable[..., Any]]:
    tools: list[Callable[..., Any]] = []
    base_path = Path(path)
    for module_info in pkgutil.walk_packages([str(base_path)], prefix=f"{package_prefix}."):
        module = importlib.import_module(module_info.name)
        tools.extend(discover_tools(module))
    return tools


def discover_tools_in_package(package_name: str) -> list[Callable[..., Any]]:
    package = importlib.import_module(package_name)
    package_file = getattr(package, "__file__", None)
    if not package_file:
        raise RuntimeError(f"Package {package_name} has no __file__ path to scan")
    package_path = Path(package_file).parent
    return discover_tools_in_path(package_path, package_name)


__all__ = [
    "dbtTool",
    "get_tool_metadata",
    "has_tool_metadata",
    "set_tool_metadata",
    "discover_tools",
    "discover_tools_in_path",
    "discover_tools_in_package",
]
