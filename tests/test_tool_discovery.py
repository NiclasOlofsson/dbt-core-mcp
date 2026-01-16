"""Tests for tool discovery helpers."""

from pathlib import Path

from dbt_core_mcp.tools import discover_tools, discover_tools_in_package, discover_tools_in_path
from dbt_core_mcp.tools.list_resources import list_resources


def test_discover_tools_in_module() -> None:
    """Discover tools within a single module."""
    import dbt_core_mcp.tools.list_resources as module

    tools = discover_tools(module)
    assert list_resources in tools


def test_discover_tools_in_path() -> None:
    """Discover tools by scanning the tools package path."""
    import dbt_core_mcp.tools as tools_package

    package_file = tools_package.__file__
    assert package_file is not None
    tools_path = Path(package_file).parent

    tools = discover_tools_in_path(tools_path, "dbt_core_mcp.tools")
    assert list_resources in tools


def test_discover_tools_in_package() -> None:
    """Discover tools by scanning a package name recursively."""
    tools = discover_tools_in_package("dbt_core_mcp.tools")
    assert list_resources in tools
