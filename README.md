# dbt Core MCP Server

An MCP (Model Context Protocol) server for interacting with dbt (Data Build Tool) projects.

## Overview

This server provides tools to interact with dbt projects via the Model Context Protocol, enabling AI assistants to:
- Query dbt project metadata and configuration
- Get detailed model and source information with full manifest metadata
- Execute SQL queries with Jinja templating support ({{ ref() }}, {{ source() }})
- Inspect models, sources, and tests
- Access dbt documentation and lineage

## Installation & Configuration

This MCP server is designed to run within VS Code via the Model Context Protocol. It's automatically invoked by VS Code when needed - you don't run it directly from the command line.

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

- **dbt Core**: Version 1.9.0 or higher
- **Python**: 3.9 or higher
- **Supported Adapters**: Any dbt adapter (dbt-duckdb, dbt-postgres, dbt-snowflake, etc.)

## Limitations

- **Python models**: Not currently supported. Only SQL-based dbt models are supported at this time.
- **dbt Version**: Requires dbt Core 1.9.0 or higher

## Features

✅ **Implemented:**
- Query dbt project metadata (version, adapter, model/source counts)
- List and inspect models and sources with full details
- Execute SQL queries with dbt's ref() and source() functions
- Get compiled SQL for any model
- Run, test, and build models with smart change detection
- Detect schema changes (added/removed columns)
- State-based execution for fast iteration
- Works with any dbt adapter (DuckDB, Snowflake, BigQuery, Postgres, etc.)

🚧 **Planned:**
- View model lineage graph
- Custom dbt commands with streaming output

## Available Tools

### `get_project_info`
Get basic information about your dbt project including name, version, adapter type, and model/source counts.

### `list_models`
List all models in your project with their names, schemas, materialization types, tags, and dependencies.

### `list_sources`
List all sources in your project with their identifiers, schemas, and descriptions.

### `get_model_info`
Get complete information about a specific model including configuration, dependencies, and actual database schema.

**Parameters:**
- `name`: Model name (e.g., "customers")
- `include_database_schema`: Include actual column types from database (default: true)

**Example:**
```python
get_model_info(name="customers")
# Returns manifest metadata + database columns with types
```

### `get_source_info`
Get detailed information about a specific source including all configuration and metadata.

**Parameters:**
- `source_name`: Source name (e.g., "jaffle_shop")
- `table_name`: Table name within the source (e.g., "customers")

### `get_compiled_sql`
Get the fully compiled SQL for a model with all Jinja templating resolved to actual table names.

**Parameters:**
- `name`: Model name
- `force`: Force recompilation even if cached (default: false)

**Example:**
```python
get_compiled_sql(name="customers")
# Returns SQL with {{ ref() }} replaced by actual table paths
```

### `refresh_manifest`
Update the dbt manifest by running `dbt parse`. Use after making changes to model files.

### `query_database`
Execute SQL queries against your database using dbt's ref() and source() functions.

**Parameters:**
- `sql`: SQL query with optional {{ ref() }} and {{ source() }} functions
- `limit`: Maximum rows to return (optional, defaults to unlimited)

**Examples:**
```sql
-- Query a model
SELECT * FROM {{ ref('customers') }} LIMIT 10

-- Query a source
SELECT * FROM {{ source('jaffle_shop', 'orders') }}

-- Inspect schema
DESCRIBE {{ ref('stg_customers') }}

-- Aggregations
SELECT COUNT(*) FROM {{ ref('orders') }}
```

### `run_models`
Run dbt models with smart selection for fast development:

**Smart selection modes:**
- `modified_only`: Run only models that changed
- `modified_downstream`: Run changed models + everything downstream

**Other parameters:**
- `select`: Model selector (e.g., "customers", "tag:mart")
- `exclude`: Exclude models
- `full_refresh`: Force full refresh for incremental models
- `fail_fast`: Stop on first failure
- `check_schema_changes`: Detect column additions/removals

**Examples:**
```python
# Run only changed models (fast!)
run_models(modified_only=True)

# Run changes + downstream dependencies
run_models(modified_downstream=True)

# Detect schema changes
run_models(modified_only=True, check_schema_changes=True)

# Run specific model
run_models(select="customers")
```

**Schema Change Detection:**
When enabled, detects added or removed columns and recommends running downstream models to propagate changes.

### `test_models`
Run dbt tests with smart selection:

**Parameters:**
- `modified_only`: Test only changed models
- `modified_downstream`: Test changed models + downstream
- `select`: Test selector (e.g., "customers", "tag:mart")
- `exclude`: Exclude tests
- `fail_fast`: Stop on first failure

**Example:**
```python
test_models(modified_downstream=True)
```

### `build_models`
Run models and tests together in dependency order (most efficient approach):

**Example:**
```python
build_models(modified_downstream=True)
```

### `seed_data`
Load seed data (CSV files) from `seeds/` directory into database tables.

Seeds are typically used for reference data like country codes, product categories, etc.

**Smart selection modes:**
- `modified_only`: Load only seeds that changed
- `modified_downstream`: Load changed seeds + downstream dependencies

**Other parameters:**
- `select`: Seed selector (e.g., "raw_customers", "tag:lookup")
- `exclude`: Exclude seeds
- `full_refresh`: Truncate and reload seed tables
- `show`: Show preview of loaded data

**Examples:**
```python
# Load all seeds
seed_data()

# Load only changed CSVs (fast!)
seed_data(modified_only=True)

# Load specific seed
seed_data(select="raw_customers")
```

**Important:** Change detection works via file hash:
- Seeds < 1 MiB: Content changes detected ✅
- Seeds ≥ 1 MiB: Only file path changes detected ⚠️

For large seeds, use manual selection or run all seeds.

### `snapshot_models`
Execute dbt snapshots to capture slowly changing dimensions (SCD Type 2).

Snapshots track historical changes by recording when records were first seen, when they changed, and their state at each point in time.

**Parameters:**
- `select`: Snapshot selector (e.g., "customer_history", "tag:daily")
- `exclude`: Exclude snapshots

**Examples:**
```python
# Run all snapshots
snapshot_models()

# Run specific snapshot
snapshot_models(select="customer_history")

# Run snapshots tagged 'hourly'
snapshot_models(select="tag:hourly")
```

**Note:** Snapshots are time-based and should be run on a schedule (e.g., daily/hourly), not during interactive development. They do not support smart selection.

## Developer Workflow

Fast iteration with smart selection:

```python
# 1. Edit a model file
# 2. Run only what changed (~0.3s vs full project ~5s)
run_models(modified_only=True)

# 3. Run downstream dependencies
run_models(modified_downstream=True)

# 4. Test everything affected
test_models(modified_downstream=True)
```

The first run establishes a baseline state automatically. Subsequent runs detect changes and run only what's needed.

## How It Works

This server executes dbt commands in your project's Python environment:

1. **Environment Detection**: Automatically finds your Python environment (uv, poetry, venv, conda, etc.)
2. **Bridge Execution**: Runs dbt commands using your exact dbt Core version and adapter
3. **No Conflicts**: Uses subprocess execution to avoid version conflicts with the MCP server
4. **Concurrency Safety**: Detects and waits for existing dbt processes to prevent database lock conflicts

The server reads dbt's manifest.json for metadata and uses `dbt show --inline` for SQL query execution with full Jinja templating support.

## Contributing

Want to help improve this server? Check out [CONTRIBUTING.md](CONTRIBUTING.md) for development setup and guidelines.

## License

MIT License - see LICENSE file for details.

## Author

Niclas Olofsson - [GitHub](https://github.com/NiclasOlofsson)
