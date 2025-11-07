# DBT Core MCP Server

An MCP (Model Context Protocol) server for interacting with DBT (Data Build Tool) projects.

## Overview

This server provides tools to interact with DBT projects via the Model Context Protocol, enabling AI assistants to:
- Query DBT project metadata
- Inspect models, sources, and tests
- View compiled SQL
- Run DBT commands
- Access DBT documentation and lineage

## Installation

### From PyPI (when published)

The easiest way to use this MCP server is with `uvx` (no installation needed):

```bash
# Run directly with uvx (recommended)
uvx dbt-core-mcp
```

Or install it permanently:

```bash
# Using uv
uv tool install dbt-core-mcp

# Using pipx
pipx install dbt-core-mcp

# Or using pip
pip install dbt-core-mcp
```

## Usage

### Running the Server

```bash
# Run with default settings
dbt-core-mcp

# Enable debug logging
dbt-core-mcp --debug
```

The server automatically detects DBT projects from workspace roots provided by VS Code.

### Configuration for VS Code

Add to your VS Code MCP settings:

```json
{
  "mcpServers": {
    "dbt-core": {
      "command": "uvx",
      "args": ["dbt-core-mcp"]
    }
  }
}
```

Or if you prefer `pipx`:

```json
{
  "mcpServers": {
    "dbt-core": {
      "command": "pipx",
      "args": ["run", "dbt-core-mcp"]
    }
  }
}
```

### For the impatient (bleeding edge from GitHub)

If you want to always run the latest code directly from GitHub:

```json
{
  "mcpServers": {
    "dbt-core": {
      "command": "uvx",
      "args": [
        "--from",
        "git+https://github.com/NiclasOlofsson/dbt-core-mcp.git",
        "dbt-core-mcp"
      ]
    }
  }
}
```

Or with `pipx`:

```json
{
  "mcpServers": {
    "dbt-core": {
      "command": "pipx",
      "args": [
        "run",
        "--no-cache",
        "--spec",
        "git+https://github.com/NiclasOlofsson/dbt-core-mcp.git",
        "dbt-core-mcp"
      ]
    }
  }
}
```

## Requirements

- **DBT Core**: Version 1.9.0 or higher
- **Python**: 3.9 or higher
- **Supported Adapters**: Any DBT adapter (dbt-duckdb, dbt-postgres, dbt-snowflake, etc.)

## Limitations

- **Python models**: Not currently supported. Only SQL-based DBT models are supported at this time.
- **DBT Version**: Requires dbt-core 1.9.0 or higher

## Features

✅ **Implemented:**
- Get DBT project information (version, adapter, counts)
- List all models with metadata
- List all sources
- Automatic environment detection (uv, poetry, pipenv, venv, conda)
- Bridge runner for executing DBT in user's environment

🚧 **Planned:**
- Get model information and metadata (enhanced)
- View compiled SQL
- Run specific models
- Test models
- View model lineage
- Access DBT documentation
- Execute custom DBT commands

## Available Tools

### `get_project_info`
Returns metadata about the DBT project including:
- Project name
- DBT version
- Adapter type (e.g., duckdb, postgres, snowflake)
- Model and source counts

### `list_models`
Lists all models in the project with:
- Name and unique ID
- Schema and database
- Materialization type (table, view, incremental, etc.)
- Tags
- Dependencies
- File path

### `list_sources`
Lists all sources in the project with:
- Source and table names
- Schema and database
- Description and tags

## How It Works

This server uses a "bridge runner" approach to execute DBT in your project's Python environment:

1. **Environment Detection**: Automatically detects your Python environment (uv, poetry, pipenv, venv, conda)
2. **Subprocess Bridge**: Executes DBT commands using inline Python scripts in your environment
3. **Manifest Parsing**: Reads `target/manifest.json` for model and source metadata
4. **No Version Conflicts**: Uses your exact dbt-core version and adapter without conflicts
5. **Concurrency Protection**: Detects running DBT processes and waits for completion to prevent conflicts

### Concurrency Safety

The server includes built-in protection against concurrent DBT execution:

- **Process Detection**: Automatically detects if DBT is already running in the same project
- **Smart Waiting**: Waits up to 10 seconds (polling every 0.2s) for running DBT commands to complete
- **Safe Execution**: Only proceeds when no conflicting DBT processes are detected
- **Database Lock Handling**: Prevents common file locking issues (especially with DuckDB)

**Note**: If you're running `dbt run` or `dbt test` manually, the MCP server will wait for completion before executing its own commands. This prevents database lock conflicts and ensures data consistency.

## Contributing

Want to help improve this server? Check out [CONTRIBUTING.md](CONTRIBUTING.md) for development setup and guidelines.

## License

MIT License - see LICENSE file for details.

## Author

Niclas Olofsson - [GitHub](https://github.com/NiclasOlofsson)
