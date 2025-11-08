"""
Basic integration test for dbt Core MCP server.
"""

from dbt_core_mcp.server import create_server


def test_server_creation() -> None:
    """Test that server can be created."""
    server = create_server()
    assert server is not None
    assert server.app is not None
    assert server.project_dir is None  # No project dir when created without argument
    assert server.profiles_dir is not None
