# dbt Core MCP Server

[![Install in VS Code](https://img.shields.io/badge/VS_Code-Install_Server-0098FF?style=flat-square&logo=visualstudiocode&logoColor=white)](https://insiders.vscode.dev/redirect/mcp/install?name=dbtcore&config=%7B%22command%22%3A%22uvx%22%2C%22args%22%3A%5B%22dbt-core-mcp%22%5D%7D)
[![Install in VS Code Insiders](https://img.shields.io/badge/VS_Code_Insiders-Install_Server-24bfa5?style=flat-square&logo=visualstudiocode&logoColor=white)](https://insiders.vscode.dev/redirect/mcp/install?name=dbtcore&config=%7B%22command%22%3A%22uvx%22%2C%22args%22%3A%5B%22dbt-core-mcp%22%5D%7D&quality=insiders)
&nbsp;&nbsp;&nbsp;&nbsp;[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![dbt 1.9.0+](https://img.shields.io/badge/dbt-1.9.0+-orange.svg)](https://docs.getdbt.com/)

Meet your new dbt pair programmer - the one who actually understands your environment, respects your workflow, and does the heavy lifting.

## Why This Changes Everything

If you've tried other dbt tools with Copilot (dbt power user, datamate, etc.), you know the pain:
- They don't respect your Python environment
- They can't see your actual project structure
- They fail when adapters are missing from THEIR environment
- You end up doing the work yourself anyway

**dbt-core-mcp is different.** It's not just another plugin - it's a true pair programming partner that:

- **Zero dbt Dependencies**: Our server needs NO dbt-core, NO adapters - works with YOUR environment
- **Stays in Flow**: Keep the conversation going with Copilot while it handles dbt commands, runs tests, and analyzes impact
- **Respects Your Environment**: Detects and uses YOUR exact dbt version, YOUR adapter, YOUR Python setup (uv, poetry, venv, conda)
- **Actually Helps**: Instead of generic suggestions, you get real work done - "run my changes and test downstream" actually does it
- **Knows Your Project**: Full access to your models, lineage, sources, and compiled SQL - no guessing, no manual lookups

&nbsp;  
>&nbsp;  
>**Before dbt-core-mcp**  
>You: *"Copilot, help me understand what depends on stg_orders"*  
>Copilot: *"You should check the manifest.json or run dbt list..."*  
>You: *Switches to terminal, runs commands, copies output back...*
>
>**With dbt-core-mcp**  
>You: *"What depends on stg_orders?"*  
>Copilot: *Shows full lineage, impact analysis, and affected models instantly*  
>You: *"Run my changes and test everything downstream"*  
>Copilot: *Does it. Reports results. You focus on the next step.*  
>&nbsp;

**This is pair programming the way it should be** - you focus on the logic, Copilot handles the execution. No context switching, no terminal juggling, just flow.

## What You Get (Features & Benefits)

- **🔥 Zero dbt Dependencies**: Server has NO dbt-core, NO adapters - ultimate environment respect
- **Natural Language Control**: Just talk - "run my changes and test downstream" actually works
- **Bridge Execution**: Automatically detects YOUR environment and runs dbt with YOUR versions
- **Works with ANY Adapter**: duckdb, snowflake, postgres, bigquery, databricks - if you have it, we work with it
- **Smart Selection**: Automatic change detection - run only what changed, or changed + downstream
- **Full Project Awareness**: Lineage analysis, impact assessment, compiled SQL - instant access to everything
- **True Pair Programming**: Stay in conversation with Copilot while it executes dbt commands and reports results
- **Schema Change Detection**: Automatically detects column changes and recommends downstream updates
- **No Configuration Needed**: Works with your existing dbt setup - any adapter, any database, any version
- **Concurrency Safe**: Detects and waits for existing dbt processes to prevent conflicts

This server provides tools to interact with dbt projects via the Model Context Protocol, enabling AI assistants to:
- Query dbt project metadata and configuration
- Get detailed model and source information with full manifest metadata
- Execute SQL queries with Jinja templating support ({{ ref() }}, {{ source() }})
- Inspect models, sources, and tests
- Access dbt documentation and lineage

### Natural Language, Powerful Results

Just talk to Copilot naturally - no need to memorize commands or syntax:

>&nbsp;  
>**Explore your project**  
>You: *"What models do we have in this project?"*  
>Copilot: *Shows all models with materialization types and tags*
>
>**Understand dependencies**  
>You: *"Show me what the customers model depends on"*  
>Copilot: *Displays full lineage with upstream sources and models*
>
>**Run smart builds**  
>You: *"Run only the models I changed and test everything downstream"*  
>Copilot: *Executes dbt with smart selection, runs tests, reports results*  
>&nbsp;

## Get It Running (2 Minutes)

*If you don't have Python installed, get it at [python.org/downloads](https://www.python.org/downloads/) - you'll need Python 3.9 or higher.*

*Don't have `uv` yet? Install it with: `pip install uv` or see [docs.astral.sh/uv](https://docs.astral.sh/uv/getting-started/installation/)*

### Option 1: One-Click Install (Easiest)

Click the badge for your VS Code version:

[![Install in VS Code](https://img.shields.io/badge/VS_Code-Install_Server-0098FF?style=flat-square&logo=visualstudiocode&logoColor=white)](https://insiders.vscode.dev/redirect/mcp/install?name=dbtcore&config=%7B%22command%22%3A%22uvx%22%2C%22args%22%3A%5B%22dbt-core-mcp%22%5D%7D)
[![Install in VS Code Insiders](https://img.shields.io/badge/VS_Code_Insiders-Install_Server-24bfa5?style=flat-square&logo=visualstudiocode&logoColor=white)](https://insiders.vscode.dev/redirect/mcp/install?name=dbtcore&config=%7B%22command%22%3A%22uvx%22%2C%22args%22%3A%5B%22dbt-core-mcp%22%5D%7D&quality=insiders)

That's it! The server will automatically start when you open a dbt project.

### Option 2: Manual Configuration

Add this to your `.vscode/mcp.json` file in your dbt project workspace:

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

### Option 3: Bleeding Edge (Always Latest from GitHub)

For the impatient who want the latest features immediately:

**With `uvx`:**
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

**With `pipx`:**
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

This downloads and installs directly from GitHub every time - always bleeding edge!

### Optional Configuration

#### Command Timeout

By default, dbt commands have no timeout (they can run as long as needed). For complex models that take a long time to compile, you can set a timeout or explicitly disable it:

```json
{
  "servers": {
    "dbt-core": {
      "command": "uvx",
      "args": [
        "dbt-core-mcp",
        "--dbt-command-timeout", "300"  // 5 minutes, or use 0 for no timeout (default)
      ]
    }
  }
}
```

#### Project Directory

The server automatically detects your dbt project from the workspace root. If your dbt project is in a subdirectory or you need to specify a different location, use `--project-dir` with either a relative or absolute path:

```json
{
  "servers": {
    "dbt-core": {
      "command": "uvx",
      "args": [
        "dbt-core-mcp",
        "--project-dir", "path/to/dbt/project"  // relative or absolute path
      ]
    }
  }
}
```

## Requirements

**For the MCP Server:**
- Python 3.9 or higher
- NO dbt-core required, NO adapters required - just install `dbt-core-mcp`

**For Your dbt Project:**
- dbt Core 1.9.0 or higher
- Any dbt adapter (dbt-duckdb, dbt-postgres, dbt-snowflake, dbt-databricks, etc.)

The server automatically detects and uses YOUR project's dbt installation via bridge execution.

## Limitations

- **Python models**: Not currently supported. Only SQL-based dbt models are supported at this time.
- **dbt Version**: Requires dbt Core 1.9.0 or higher

## Available Tools

**Don't worry about memorizing these** - you don't need to know tool names or parameters. Just talk naturally to Copilot and it figures out what to use. This reference is here for the curious who want to understand what's happening under the hood.

**Pro tip:** Focus on the conversational examples (You: / Copilot:) - they show how to actually use these tools in practice.

### Project Information

#### `get_project_info`
Get basic information about your dbt project including name, version, adapter type, and resource counts. By default, also runs `dbt debug` to validate your environment and test the database connection.

>&nbsp;  
>You: *"What dbt version is this project using?"*  
>Copilot: *Shows project info with dbt version, adapter type, and connection status*
>
>You: *"How many models and sources are in this project?"*  
>Copilot: *Displays counts and project overview with diagnostics*
>
>You: *"Is my database connection working?"*  
>Copilot: *Shows connection test results from dbt debug*
>
>You: *"Check my dbt setup"*  
>Copilot: *Runs full environment validation and reports any issues*  
>&nbsp;

**Parameters:**
- `run_debug`: Run `dbt debug` to validate environment and test connection (default: True)

**Returns:** Project metadata plus diagnostic results including:
- Database connection status (ok/failed/unknown)
- Environment validation output
- System and dependency checks

**Note:** Set `run_debug=False` to skip diagnostics and get only basic project info (faster for repeated queries).

### Resource Discovery (Unified Tools)

**One tool, all resource types** - these unified tools work across models, sources, seeds, snapshots, and tests. No need for separate tools for each type.

#### `list_resources`
List all resources in your project, or filter by type (models, sources, seeds, snapshots, tests).

>&nbsp;  
>You: *"Show me all resources in this project"*  
>Copilot: *Lists all models, sources, seeds, snapshots, and tests*
>
>You: *"What models do we have?"*  
>Copilot: *Filters to show only models with their materialization types*
>
>You: *"List all data sources"*  
>Copilot: *Shows configured sources with schemas and descriptions*
>
>You: *"Show me the seeds"*  
>Copilot: *Displays CSV seed files available in the project*
>
>You: *"Which models are materialized as tables?"*  
>Copilot: *Filters models by materialization type*  
>&nbsp;

**Parameters:**
- `resource_type`: Optional filter - `"model"`, `"source"`, `"seed"`, `"snapshot"`, `"test"`, or `None` for all

**Returns:** Consistent structure for all types with common fields (name, description, tags) plus type-specific details (materialization, source_name, etc.)

#### `get_resource_info`
Get detailed information about any resource - works for models, sources, seeds, snapshots, and tests.

>&nbsp;  
>You: *"Show me details about the customers model"*  
>Copilot: *Displays full model metadata, config, column information, and compiled SQL*
>
>You: *"What's in the raw_customers source?"*  
>Copilot: *Shows source schema, columns, and freshness configuration*
>
>You: *"Describe the country_codes seed"*  
>Copilot: *Returns seed configuration and column definitions*
>
>You: *"What columns does the orders model have?"*  
>Copilot: *Shows column names, types, and descriptions from database*
>
>You: *"Show me the compiled SQL for customers"*  
>Copilot: *Returns model info with compiled SQL (all Jinja resolved)*
>
>You: *"Tell me about the customer_snapshot"*  
>Copilot: *Displays snapshot configuration and SCD tracking setup*  
>&nbsp;

**Parameters:**
- `name`: Resource name (e.g., "customers", "jaffle_shop.raw_orders")
- `resource_type`: Optional - auto-detects if not specified
- `include_database_schema`: Include actual column types from database (default: true)
- `include_compiled_sql`: Include compiled SQL with Jinja resolved (default: true, models only)

**Auto-detection:** Just provide the name - the tool automatically finds it whether it's a model, source, seed, snapshot, or test. For sources, use `"source_name.table_name"` format or just the table name.

**Compiled SQL:** For models, automatically includes compiled SQL with all `{{ ref() }}` and `{{ source() }}` resolved to actual table names. Will trigger `dbt compile` if not already compiled. Set `include_compiled_sql=False` to skip compilation.

### Lineage & Impact Analysis (Unified Tools)

**Understand relationships across all resource types** - analyze dependencies and impact for models, sources, seeds, snapshots, and tests.

#### `get_lineage`
Trace dependency relationships for any resource - shows what it depends on (upstream) and what depends on it (downstream).

>&nbsp;  
>You: *"Show me the lineage for the customers model"*  
>Copilot: *Displays full dependency tree with upstream sources and downstream models*
>
>You: *"What does stg_orders depend on?"*  
>Copilot: *Shows upstream dependencies (sources and parent models)*
>
>You: *"What's downstream from the raw_customers source?"*  
>Copilot: *Shows all models that use this source*
>
>You: *"Where does the revenue model get its data from?"*  
>Copilot: *Displays upstream lineage with all source data*
>
>You: *"Show me everything that uses the country_codes seed"*  
>Copilot: *Lists all downstream models that reference this seed*  
>&nbsp;

**Parameters:**
- `name`: Resource name (works for models, sources, seeds, snapshots, tests)
- `direction`: `"upstream"` (sources), `"downstream"` (dependents), or `"both"` (default)
- `depth`: Maximum levels to traverse (None for unlimited, 1 for immediate, etc.)
- `resource_type`: Optional - auto-detects if not specified

**Returns:** Dependency tree with statistics (upstream_count, downstream_count, total_dependencies)

**Use cases:**
- Understand data flow and relationships
- Explore where resources get their data
- See what depends on specific resources
- Impact analysis before making changes

#### `analyze_impact`
Analyze the blast radius of changing any resource - shows all downstream dependencies that would be affected.

>&nbsp;  
>You: *"What's the impact of changing the stg_customers model?"*  
>Copilot: *Shows all downstream models, tests, and affected resources*
>
>You: *"If I modify the raw_orders source, what needs to run?"*  
>Copilot: *Lists impacted models grouped by distance with recommended commands*
>
>You: *"What breaks if I change the country_codes seed?"*  
>Copilot: *Shows total impact count and affected resources*
>
>You: *"How many models depend on this snapshot?"*  
>Copilot: *Displays impact statistics and dependency count*  
>&nbsp;

**Parameters:**
- `name`: Resource name (works for models, sources, seeds, snapshots, tests)
- `resource_type`: Optional - auto-detects if not specified

**Returns:**
- Affected resources grouped by distance from the changed resource
- Count of affected tests and other resources
- Total impact statistics
- Context-aware recommended dbt commands (e.g., `dbt run -s stg_customers+`)
- Impact level message (No/Low/Medium/High)

**Use cases:**
- Before refactoring: understand blast radius
- Planning incremental rollouts
- Estimating rebuild time after changes
- Risk assessment for modifications

### Database Queries

#### `query_database`
Execute SQL queries against your database using dbt's ref() and source() functions. Results can be displayed inline or exported to CSV/TSV files for analysis.

>&nbsp;  
>You: *"Show me 10 rows from the customers model"*  
>Copilot: *Executes SELECT * FROM {{ ref('customers') }} LIMIT 10 and displays results*
>
>You: *"Count the orders in the staging table"*  
>Copilot: *Runs SELECT COUNT(*) and shows the count*
>
>You: *"What's the schema of stg_payments?"*  
>Copilot: *Queries column information and displays schema*
>
>You: *"Export customers data to CSV for analysis"*  
>Copilot: *Saves query results to a CSV file you can open in Excel*
>
>You: *"Save all orders to a TSV file"*  
>Copilot: *Exports data in tab-separated format for import into other tools*  
>&nbsp;

**What you can do:**
- Query any model using `{{ ref('model_name') }}` or source using `{{ source('source_name', 'table_name') }}`
- Get results displayed directly in the conversation (good for small result sets)
- Export to CSV or TSV files (perfect for large datasets or further analysis in Excel/other tools)
- Automatically handles large results without overwhelming the conversation

### Execution Tools

#### `run_models`
Run dbt models with smart selection for fast development.

>&nbsp;  
>You: *"Run only the models I changed"*  
>Copilot: *Detects modified models and executes dbt run with selection*
>
>You: *"Run my changes and everything downstream"*  
>Copilot: *Runs modified models plus all downstream dependencies*
>
>You: *"Run the customers model"*  
>Copilot: *Executes dbt run --select customers*
>
>You: *"Build all mart models with a full refresh"*  
>Copilot: *Runs dbt run --select marts.* --full-refresh*
>
>You: *"Run modified models and check for schema changes"*  
>Copilot: *Runs models and detects added/removed columns*  
>&nbsp;

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

>&nbsp;  
>You: *"Test only the models I changed"*  
>Copilot: *Runs tests for modified models only*
>
>You: *"Run tests for my changes and downstream models"*  
>Copilot: *Tests modified models and everything affected downstream*
>
>You: *"Test the customers model"*  
>Copilot: *Executes dbt test --select customers*
>
>You: *"Run all tests for staging models"*  
>Copilot: *Runs dbt test --select staging.*  
>&nbsp;

**Parameters:**
- `modified_only`: Test only changed models
- `modified_downstream`: Test changed models + downstream
- `select`: Test selector (e.g., "customers", "tag:mart")
- `exclude`: Exclude tests
- `fail_fast`: Stop on first failure

#### `build_models`
Run models and tests together in dependency order (most efficient approach).

>&nbsp;  
>You: *"Build my changes and everything downstream"*  
>Copilot: *Runs dbt build with modified models and dependencies*
>
>You: *"Run and test only what I modified"*  
>Copilot: *Executes dbt build on changed models only*
>
>You: *"Build the entire mart layer with tests"*  
>Copilot: *Runs dbt build --select marts.* with all tests*  
>&nbsp;

#### `seed_data`
Load seed data (CSV files) from `seeds/` directory into database tables.

>&nbsp;  
>You: *"Load all seed data"*  
>Copilot: *Runs dbt seed and loads all CSV files*
>
>You: *"Load only the seeds I changed"*  
>Copilot: *Detects modified seed files and loads them*
>
>You: *"Reload the raw_customers seed file"*  
>Copilot: *Executes dbt seed --select raw_customers --full-refresh*
>
>You: *"Show me what's in the country_codes seed"*  
>Copilot: *Displays preview of loaded seed data*  
>&nbsp;

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

>&nbsp;  
>You: *"Run all snapshots"*  
>Copilot: *Executes dbt snapshot for all snapshot models*
>
>You: *"Execute the customer_history snapshot"*  
>Copilot: *Runs dbt snapshot --select customer_history*
>
>You: *"Run daily snapshots"*  
>Copilot: *Executes snapshots tagged with 'daily'*  
>&nbsp;

Snapshots track historical changes by recording when records were first seen, when they changed, and their state at each point in time.

**Parameters:**
- `select`: Snapshot selector (e.g., "customer_history", "tag:daily")
- `exclude`: Exclude snapshots

**Note:** Snapshots are time-based and should be run on a schedule (e.g., daily/hourly), not during interactive development. They do not support smart selection.

#### `install_deps`
Install dbt packages defined in packages.yml to enable interactive package management workflow.

>&nbsp;  
>You: *"I need to use dbt_utils macros"*  
>Copilot: *Checks if installed, adds to packages.yml, runs install_deps()*
>
>You: *"Install the packages defined in packages.yml"*  
>Copilot: *Executes dbt deps and shows installed packages*
>
>You: *"Add dbt_utils and install it"*  
>Copilot: *Edits packages.yml, runs install_deps(), ready to use macros*  
>&nbsp;

This tool enables a complete workflow where Copilot can:
1. Suggest using a dbt package (e.g., dbt_utils)
2. Edit packages.yml to add the package
3. Run install_deps() to install it
4. Write code that uses the package's macros

All without breaking conversation flow.

**Returns:** Installation status and list of installed packages

**Package Discovery:**
Use `list_resources(type="macro")` to see which packages are already installed.
Macros follow the pattern `macro.{package_name}.{macro_name}`.

**Note:** This is an interactive development tool (like run_models/test_models), not infrastructure automation. It enables Copilot to complete its own recommendations mid-conversation.

## Developer Workflow

Fast iteration with smart selection - just describe what you want:

>&nbsp;  
>You: *"Run only what I changed"*  
>Copilot: *Detects modified models and runs them*
>
>You: *"Run my changes and test everything downstream"*  
>Copilot: *Runs modified models + downstream dependencies, then tests*
>
>You: *"Build my modified models with tests"*  
>Copilot: *Executes dbt build with smart selection*  
>&nbsp;

The first run establishes a baseline state automatically. Subsequent runs detect changes and run only what's needed.

**Before-and-After Example:**

>&nbsp;  
>**Traditional workflow:**  
>```bash
>dbt run --select customers+
>dbt test --select customers+
>```
>
>**With dbt-core-mcp:**  
>You: *"I modified the customers model, run it and test everything affected"*  
>Copilot: *Handles everything - runs, tests, and reports results*  
>&nbsp;

## How It Works

This server executes dbt commands in your project's Python environment using a bridge execution pattern:

1. **Zero dbt Dependencies**: MCP server requires NO dbt-core, NO adapters - just Python utilities
2. **Environment Detection**: Automatically finds your Python environment (uv, poetry, venv, conda, etc.)
3. **Bridge Execution**: Builds Python scripts as strings and executes them in YOUR environment
4. **Uses YOUR dbt**: Runs with YOUR dbt-core version, YOUR adapters, YOUR configuration
5. **No Conflicts**: Can't have version conflicts when we don't have dbt dependencies!
6. **Concurrency Safety**: Detects and waits for existing dbt processes to prevent database lock conflicts

The server reads dbt's manifest.json for metadata and uses `dbt show --inline` (executed in YOUR environment) for SQL query execution with full Jinja templating support.

**In practice:**

>&nbsp;  
>**Your project:** dbt-core 1.10.13 + dbt-duckdb  
>**Our server:** mcp, fastmcp, pydantic, pyyaml, psutil (no dbt!)  
>**Result:** Perfect compatibility - we detect your environment and run YOUR dbt  
>&nbsp;

No configuration needed - it just works with your existing dbt setup, any version, any adapter.

## Contributing

Want to help make this better? **The best contribution you can make is actually using it** - your feedback and bug reports are what really drive improvements.

Of course, code contributions are welcome too! Check out [CONTRIBUTING.md](CONTRIBUTING.md) for development setup and guidelines. But seriously, just using it and telling us what works (or doesn't) is incredibly valuable.

## License

MIT License - see LICENSE file for details.

## Author

Niclas Olofsson - [GitHub](https://github.com/NiclasOlofsson)
