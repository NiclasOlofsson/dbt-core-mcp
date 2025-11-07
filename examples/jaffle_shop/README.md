# Jaffle Shop Example DBT Project

This is a minimal DBT project for testing the dbt-core-mcp server.

## Project Structure

```
jaffle_shop/
├── dbt_project.yml          # Project configuration
├── profiles.yml             # Connection profiles (DuckDB)
└── models/
    ├── staging/
    │   ├── schema.yml       # Source definitions
    │   ├── stg_customers.sql
    │   └── stg_orders.sql
    └── marts/
        ├── schema.yml       # Model definitions and tests
        └── customers.sql
```

## Models

- **staging/stg_customers**: Cleaned customer data from raw source
- **staging/stg_orders**: Cleaned order data from raw source  
- **marts/customers**: Customer dimension with order metrics (materialized as table)

## Testing with MCP Server

Point the MCP server to this project:

```bash
uv run dbt-core-mcp --project-dir examples/jaffle_shop
```

Or in `.vscode/mcp.json`:
```json
{
  "servers": {
    "dbt-core": {
      "command": "uv",
      "args": ["run", "dbt-core-mcp", "--project-dir", "examples/jaffle_shop"],
      "cwd": "."
    }
  }
}
```
