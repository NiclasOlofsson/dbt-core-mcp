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

Add to your `.vscode/mcp.json` file in your dbt project workspace:

```json
{
  "servers": {
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
  "servers": {
    "dbt-core": {
      "command": "pipx",
      "args": ["run", "dbt-core-mcp"]
    }
  }
}
```

The server will automatically use your workspace directory as the dbt project location.

### For the impatient (bleeding edge from GitHub)

If you want to always run the latest code directly from GitHub:

```json
{
  "servers": {
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
  "servers": {
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
- **Lineage & impact analysis** (explore dependencies, assess change impact)
- Run, test, and build models with smart change detection
- Detect schema changes (added/removed columns)
- State-based execution for fast iteration
- Works with any dbt adapter (DuckDB, Snowflake, BigQuery, Postgres, etc.)

🚧 **Planned:**
- View model lineage graph
- Custom dbt commands with streaming output

## Available Tools

### Project Information

#### `get_project_info`
Get basic information about your dbt project including name, version, adapter type, and model/source counts.

**Ask Copilot:**
- "What dbt version is this project using?"
- "How many models and sources are in this project?"

#### `list_models`
List all models in your project with their names, schemas, materialization types, tags, and dependencies.

**Ask Copilot:**
- "Show me all the models in this project"
- "Which models are materialized as tables?"
- "List all staging models"

#### `list_sources`
List all sources in your project with their identifiers, schemas, and descriptions.

**Ask Copilot:**
- "What data sources are configured in this project?"
- "Show me all available source tables"

### Lineage & Impact Analysis

#### `get_model_lineage`
Get the full dependency tree (lineage) for one or more models showing upstream and/or downstream relationships.

**Ask Copilot:**
- "Show me the lineage for the customers model"
- "What models does stg_orders depend on?"
- "What's downstream from stg_customers and stg_orders?"
- "Show me where the revenue model gets its data from"

**Parameters:**
- `names`: Model name(s) - single string or list of models
- `direction`: "upstream" (sources), "downstream" (dependents), or "both" (default)
- `depth`: Maximum levels to traverse (None for unlimited, 1 for immediate, etc.)

**Use cases:**
- Understand data flow and model relationships
- Explore where models get their data from
- See what models depend on specific models
- Analyze combined dependencies for multiple models

#### `analyze_model_impact`
Analyze the impact of changing one or more models - shows all downstream dependencies affected.

**Ask Copilot:**
- "What's the impact of changing the stg_customers model?"
- "If I modify stg_orders, what else needs to run?"
- "What's the combined impact of changing all staging models?"
- "How many models will break if I change this?"

**Parameters:**
- `names`: Model name(s) - single string or list of models

**Returns:**
- List of affected models grouped by distance
- Count of affected tests and other resources
- Total impact statistics (deduplicated for multiple models)
- Recommended dbt command to run

**Use cases:**
- Before refactoring: understand blast radius
- Planning incremental rollouts
- Estimating rebuild time after changes
- Risk assessment for model modifications

### Model Information

#### `get_model_info`
Get complete information about a specific model including configuration, dependencies, and actual database schema.

**Ask Copilot:**
- "Show me details about the customers model"
- "What columns does the orders model have?"
- "What's the materialization type for stg_payments?"

**Parameters:**
- `name`: Model name (e.g., "customers")
- `include_database_schema`: Include actual column types from database (default: true)

#### `get_source_info`
Get detailed information about a specific source including all configuration and metadata.

**Ask Copilot:**
- "Show me the schema for the raw customers source"
- "What columns are in the orders source table?"

**Parameters:**
- `source_name`: Source name (e.g., "jaffle_shop")
- `table_name`: Table name within the source (e.g., "customers")

#### `get_compiled_sql`
Get the fully compiled SQL for a model with all Jinja templating resolved to actual table names.

**Ask Copilot:**
- "Show me the compiled SQL for the customers model"
- "What does the final query look like for stg_orders?"
- "Convert the customers model Jinja to actual SQL"

**Parameters:**
- `name`: Model name
- `force`: Force recompilation even if cached (default: false)

#### `refresh_manifest`
Update the dbt manifest by running `dbt parse`. Use after making changes to model files.

**Ask Copilot:**
- "Refresh the dbt manifest"
- "Parse the dbt project to pick up my changes"

#### `query_database`
Execute SQL queries against your database using dbt's ref() and source() functions.

**Ask Copilot:**
- "Show me 10 rows from the customers model"
- "Count the orders in the staging table"
- "What's the schema of stg_payments?"
- "Query the raw orders source and show me recent records"

**Parameters:**
- `sql`: SQL query with optional {{ ref() }} and {{ source() }} functions
- `limit`: Maximum rows to return (optional, defaults to unlimited)

### Model Execution

#### `run_models`
Run dbt models with smart selection for fast development.

**Ask Copilot:**
- "Run only the models I changed"
- "Run my changes and everything downstream"
- "Run the customers model"
- "Build all mart models with a full refresh"
- "Run modified models and check for schema changes"

**Smart selection modes:**
- `modified_only`: Run only models that changed
- `modified_downstream`: Run changed models + everything downstream

**Other parameters:**
- `select`: Model selector (e.g., "customers", "tag:mart")
- `exclude`: Exclude models
- `full_refresh`: Force full refresh for incremental models
- `fail_fast`: Stop on first failure
- `check_schema_changes`: Detect column additions/removals

**Schema Change Detection:**
When enabled, detects added or removed columns and recommends running downstream models to propagate changes.

#### `test_models`
Run dbt tests with smart selection.

**Ask Copilot:**
- "Test only the models I changed"
- "Run tests for my changes and downstream models"
- "Test the customers model"
- "Run all tests for staging models"

**Parameters:**
- `modified_only`: Test only changed models
- `modified_downstream`: Test changed models + downstream
- `select`: Test selector (e.g., "customers", "tag:mart")
- `exclude`: Exclude tests
- `fail_fast`: Stop on first failure

#### `build_models`
Run models and tests together in dependency order (most efficient approach).

**Ask Copilot:**
- "Build my changes and everything downstream"
- "Run and test only what I modified"
- "Build the entire mart layer with tests"

#### `seed_data`
Load seed data (CSV files) from `seeds/` directory into database tables.

**Ask Copilot:**
- "Load all seed data"
- "Load only the seeds I changed"
- "Reload the raw_customers seed file"
- "Show me what's in the country_codes seed"

Seeds are typically used for reference data like country codes, product categories, etc.

**Smart selection modes:**
- `modified_only`: Load only seeds that changed
- `modified_downstream`: Load changed seeds + downstream dependencies

**Other parameters:**
- `select`: Seed selector (e.g., "raw_customers", "tag:lookup")
- `exclude`: Exclude seeds
- `full_refresh`: Truncate and reload seed tables
- `show`: Show preview of loaded data

**Important:** Change detection works via file hash:
- Seeds < 1 MiB: Content changes detected ✅
- Seeds ≥ 1 MiB: Only file path changes detected ⚠️

For large seeds, use manual selection or run all seeds.

#### `snapshot_models`
Execute dbt snapshots to capture slowly changing dimensions (SCD Type 2).

**Ask Copilot:**
- "Run all snapshots"
- "Execute the customer_history snapshot"
- "Run daily snapshots"

Snapshots track historical changes by recording when records were first seen, when they changed, and their state at each point in time.

**Parameters:**
- `select`: Snapshot selector (e.g., "customer_history", "tag:daily")
- `exclude`: Exclude snapshots

**Note:** Snapshots are time-based and should be run on a schedule (e.g., daily/hourly), not during interactive development. They do not support smart selection.

## Developer Workflow

Fast iteration with smart selection:

**Ask Copilot:**
- "Run only what I changed"
- "Run my changes and test everything downstream"
- "Build my modified models with tests"

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
