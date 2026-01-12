"""dbt-core-mcp tools package.

Each tool module implements a single MCP tool with:
- setup(app, state): Registers the tool with FastMCP
- _implementation(...): Pure logic function (testable)
"""
