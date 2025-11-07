# DBT Core MCP Server

An MCP (Model Context Protocol) server for interacting with DBT (Data Build Tool) projects.

## Overview

This server provides tools to interact with DBT projects via the Model Context Protocol, enabling AI assistants to:
- Query DBT project metadata and configuration
- Get detailed model and source information with full manifest metadata
- Execute SQL queries with Jinja templating support ({{ ref() }}, {{ source() }})
- Inspect models, sources, and tests
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
- Get DBT project information (version, adapter, counts, paths, status)
- List all models with metadata and dependencies
- List all sources with identifiers
- **Get detailed model information** with all manifest metadata (~40 fields)
- **Get detailed source information** with complete configuration
- Refresh manifest (run `dbt parse`)
- **Query database with Jinja templating** ({{ ref('model') }}, {{ source('src', 'table') }})
- Full SQL support (SELECT, DESCRIBE, EXPLAIN, aggregations, JOINs)
- Configurable result limits (no forced LIMIT clauses)
- Automatic environment detection (uv, poetry, pipenv, venv, conda)
- Bridge runner for executing DBT in user's environment
- Lazy-loaded configuration with file modification tracking
- Concurrency protection for safe DBT execution

🚧 **Planned:**
- View compiled SQL for models
- Run specific models
- Test models  
- View model lineage graph
- Access DBT documentation
- Execute custom DBT commands with streaming output

## Available Tools

### `get_project_info`
Returns metadata about the DBT project including:
- Project name
- DBT version
- Adapter type (e.g., duckdb, postgres, snowflake)
- Model and source counts
- Project and profiles directory paths
- Status indicator

### `list_models`
Lists all models in the project with:
- Name and unique ID
- Schema and database
- Materialization type (table, view, incremental, etc.)
- Tags and descriptions
- Dependencies on other models/sources
- File path

### `list_sources`
Lists all sources in the project with:
- Source and table names
- Schema and database
- Identifiers
- Description and tags
- Package name

### `get_model_info`
Get detailed information about a specific DBT model:
- Returns the complete manifest node for a model (~40 fields)
- Includes all metadata, columns, configuration, dependencies, and more
- Excludes `raw_code` to keep context lightweight (use file path to read SQL)
- Examples: column definitions, tests, materialization config, tags, meta, etc.

**Usage:** `get_model_info(name="customers")` or `get_model_info(name="staging.stg_orders")`

### `get_source_info`
Get detailed information about a specific DBT source:
- Returns the complete manifest source node (~31 fields)
- Includes all metadata, columns, freshness configuration, etc.
- Source-specific settings like loader, identifier, quoting, etc.

**Usage:** `get_source_info(source_name="jaffle_shop", table_name="customers")`

### `refresh_manifest`
Refreshes the DBT manifest by running `dbt parse`:
- Force option to always re-parse
- Returns status with model and source counts
- Updates cached project metadata

### `query_database`
Execute SQL queries against the DBT project's database with Jinja templating support:
- **Jinja templating**: Use `{{ ref('model_name') }}` and `{{ source('source', 'table') }}`
- Supports any SQL command (SELECT, DESCRIBE, EXPLAIN, aggregations, JOINs, etc.)
- Works with any DBT adapter (DuckDB, Snowflake, BigQuery, Postgres, etc.)
- Configurable row limits or unlimited results
- Returns structured JSON with row data and count
- Uses `dbt show --inline` for query execution

**Examples:**
```sql
-- Reference DBT models
SELECT * FROM {{ ref('customers') }}

-- Reference sources
SELECT * FROM {{ source('jaffle_shop', 'orders') }} LIMIT 10

-- Schema inspection
DESCRIBE {{ ref('stg_customers') }}

-- Aggregations
SELECT COUNT(*) as total FROM {{ ref('orders') }}
```

**Key Features:**
- Executes via `dbt show --inline` with full Jinja compilation
- Clean JSON output parsed from DBT results
- No forced LIMIT clauses when `limit=None`
- Full DBT adapter compatibility

## How It Works

This server uses a "bridge runner" approach to execute DBT in your project's Python environment:

1. **Environment Detection**: Automatically detects your Python environment (uv, poetry, pipenv, venv, conda)
2. **Subprocess Bridge**: Executes DBT commands using inline Python scripts in your environment
3. **Manifest Parsing**: Reads and caches `target/manifest.json` for model and source metadata
4. **Query Execution**: Uses `dbt show --inline` for SQL queries with Jinja templating
5. **No Version Conflicts**: Uses your exact dbt-core version and adapter without conflicts
6. **Concurrency Protection**: Detects running DBT processes and waits for completion to prevent conflicts

### Query Database Implementation

The `query_database` tool uses `dbt show --inline` for maximum flexibility:

- **Jinja Templating**: Full support for `{{ ref() }}` and `{{ source() }}` in queries
- **DBT show command**: Executes SQL via DBT's native show functionality
- **Configurable Limits**: Optional `limit` parameter, uses `--limit -1` for unlimited results
- **JSON Output**: Parses structured JSON from `dbt show` output
- **Universal SQL Support**: Works with SELECT, DESCRIBE, EXPLAIN, aggregations, JOINs, etc.
- **Universal Adapter Support**: Works with any DBT adapter (DuckDB, Snowflake, BigQuery, Postgres, etc.)
- **Lazy Configuration**: Project config loaded once and cached with modification time checking

This approach provides full Jinja compilation at runtime while maintaining DBT's connection management and adapter compatibility.

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
