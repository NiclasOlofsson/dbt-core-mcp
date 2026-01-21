---
applyTo: '**/*.py'
description: Protocol for testing dbt-core-mcp tool changes
---

# Tool Testing Protocol

## Purpose
Comprehensive test sequence to showcase tool functionality, TDD workflow, progress reporting, and error handling.

## When to Run This Protocol

Execute this test sequence when the user requests:
- "show and tell" / "demo" / "demonstrate"
- "run the test sequence" / "run tool tests"
- "showcase the tools" / "test the tools"
- "verify the changes" / "test the progress reporting"

## Test Sequence to execute

### Prerequisites: MCP Server Setup
**OPTIONAL**: You can restart the MCP server for a fresh start, but it's not required:

1. **Optional Restart**: Increment `_RESTART` counter in `.vscode/mcp.json` if desired
2. **Verify Tools**: Test key dbt-core-mcp tools are working:
   - `get_project_info(run_debug=false)`
   - `list_resources()`
   - `query_database(sql="SELECT 1 as test")`
3. **Confirm Resources**: Verify project has expected structure (3 models, 2 seeds, sources, tests)

If tools are not available, STOP and troubleshoot before proceeding.

### Demo Execution Rules

YOU MUST run each step in order, observing the expected outputs and behaviors. Do not call the next step until the current step is fully verified.

YOU MUST introduce yourself and explain the overall purpose before starting the sequence.

For each step:
- Before running, briefly explain to the user what the step is demonstrating.
- Before every tool call, express the intent.
- **SQL Display Rule**: ALWAYS show SQL code in markdown ```sql blocks before executing query_database
- After every tool call, explain the result returned.
- After running, summarize the observed results and confirm they match expectations.

### Phase 1: Project Discovery
**Narrative**: "Every great data journey starts with understanding what you're working with. Let's explore this jaffle_shop project together and see what resources are available. Think of this as getting your bearings in a new codebase - we want to know what models exist, what data sources we have, and whether everything is properly connected."

**Context**: Before starting development, understand the project structure and available resources.

#### 1.1 Get Project Info
**Showcases**: Project metadata and dbt configuration
```
get_project_info(run_debug=true)
```
**Expected**: Project name, dbt version, profile info, connection test results

#### 1.2 List All Resources
**Showcases**: Complete inventory of models, seeds, sources, tests
```
list_resources()
```
**Expected**: List of all resources grouped by type

---

### Phase 2: Data Foundation
**Narrative**: "Now that we know what we're working with, let's lay the foundation by loading our raw data. In dbt, seeds are CSV files that get loaded into your database as tables - think of them as your reference data or small datasets that your models depend on. We'll load these first and then inspect what we've got."

**Context**: Load seed data required for models and unit tests.

#### 2.1 Load Seeds
**Showcases**: Basic execution with simple progress summary
```
load_seeds()
```
**Expected**: "2 seeds loaded" (raw_customers, raw_orders)

#### 2.2 Inspect Seed Metadata
**Showcases**: Resource information without compiled SQL
```
get_resource_info(name="raw_customers", resource_type="seed")
```
**Expected**: Seed metadata, schema, row count

---

### Phase 3: TDD - Test-Driven Development
**Narrative**: "Here's where things get really exciting - we're going to demonstrate test-driven development with dbt. Instead of building first and hoping it works, we'll run our tests first, intentionally break one to see how failure detection works, then fix it and build with confidence. This is the gold standard for reliable data engineering."

**Context**: Demonstrate TDD workflow by running tests first, intentionally breaking one, fixing it, then building the model. Focus on customers model for speed while running all tests to show full validation.

#### 3.1 Run All Tests (Initial Validation)
**Showcases**: Pre-build test execution, all tests should pass
```
test_models()
```
**Expected**: "6 tests: all passed" (3 generic + 3 unit tests for customers)

#### 3.2 Break a Unit Test
**Action**: Edit `examples/jaffle_shop/unit_tests/marts/customers_unit_tests.yml`
- Change the expected `number_of_orders` in `test_customer_with_single_order` from 1 to 999
**Showcases**: Intentional test failure to demonstrate TDD red phase

#### 3.3 Run Tests Again (Show Failure)
**Showcases**: Test failure detection and reporting
```
test_models()
```
**Expected**: Test results showing failure in `test_customer_with_single_order`

#### 3.4 Fix the Test
**Action**: Restore `number_of_orders` back to 1 in the test file
**Showcases**: Test repair (completing red-green cycle)

#### 3.5 Run Tests Again (Verify Fix)
**Showcases**: All tests passing after fix
```
test_models()
```
**Expected**: "6 tests: all passed"

#### 3.6 Build the Model
**Showcases**: Model execution after tests pass (green phase validated)
```
run_models(select="customers")
```
**Expected**: "1 model succeeded"

#### 3.7 Query the Deployed Model
**Showcases**: Data exploration using dbt templating
```
query_database(sql="SELECT customer_id, first_name, number_of_orders FROM {{ ref('customers') }} LIMIT 3")
```
**Expected**: 3 customer records with aggregated order counts

---

### Phase 4: Build & Test Complete Pipeline
**Narrative**: "With our foundation solid and tests green, it's time to build out the complete data pipeline. We'll construct our staging models that clean and standardize the raw data, then inspect the compiled SQL to see exactly what dbt generated. Finally, we'll map out the dependency relationships to understand how everything connects together."

**Context**: Build remaining models and run comprehensive tests.

#### 4.1 Build Staging Models
**Showcases**: Model execution with explicit selectors
```
run_models(select="stg_customers stg_orders")
```
**Expected**: "2 models succeeded" (stg_customers, stg_orders)
**Note**: Wildcard selectors like `stg_*` may not work consistently - use explicit names

#### 4.2 Run Generic Tests
**Showcases**: Data quality validation with test type filter
```
test_models(select="test_type:generic")
```
**Expected**: "3 tests passed" (unique, not_null tests)

#### 4.3 Inspect Compiled SQL
**Showcases**: Model metadata including compiled SQL and database schema
```
get_resource_info(name="customers", resource_type="model", include_compiled_sql=true)
```
**Expected**: Resource metadata with compiled SQL (all {{ ref() }} resolved), column schema, dependencies
**Note**: The compiled SQL shows how dbt resolved all templating to actual table names

#### 4.4 Analyze Dependencies
**Showcases**: Lineage graph both upstream and downstream
```
get_lineage(name="customers", resource_type="model", direction="both")
```
**Expected**: Upstream: stg_customers, stg_orders; Downstream: tests
**Note**: Use `resource_type="model"` to avoid confusion with source named "customers"

---

### Phase 5: Data Exploration
**Narrative**: "Now for the fun part - let's explore our data! We'll run some queries to see what we've built, showcasing both direct SQL and dbt's powerful templating system. You'll see how the ref() function automatically resolves to the correct table names, making your queries portable across environments."

**Context**: Query data using both direct SQL and dbt templating.

#### 5.1 Simple Query
**Showcases**: Direct table query
**SQL to display**: 
```sql
SELECT COUNT(*) as total_customers FROM customers
```
**Tool call**: `query_database(sql="SELECT COUNT(*) as total_customers FROM customers")`
**Expected**: Customer count
**Report Format**: "Query Result: X rows retrieved"

#### 5.2 Query with dbt ref()
**Showcases**: dbt templating syntax  
**SQL to display**:
```sql
SELECT customer_id, first_name, last_name, number_of_orders 
FROM {{ ref('customers') }} 
LIMIT 3
```
**Tool call**: `query_database(sql="SELECT customer_id, first_name, last_name, number_of_orders FROM {{ ref('customers') }} LIMIT 3")`
**Expected**: 3 customer records with order metrics
**Report Format**: "Query Result: X rows retrieved" + summary of key findings

---

### Phase 6: Impact Analysis
**Narrative**: "Understanding dependencies is crucial in data engineering. Let's explore how changes ripple through our project using lineage analysis. We'll trace data flow from sources through models and see what would be affected if we changed something. This kind of impact analysis prevents breaking changes and helps with planning."

**Context**: Understand how changes propagate through the project.

#### 6.1 Source Lineage
**Showcases**: Downstream dependencies from raw data
```
get_lineage(name="orders", resource_type="source", direction="downstream")
```
**Expected**: stg_orders → customers → tests
**Note**: Source name is "orders", not "raw_orders"

#### 6.2 Impact Analysis
**Showcases**: Full impact report with recommendations
```
analyze_impact(name="stg_customers", resource_type="model")
```
**Expected**: Affected models, tests, distance grouping, run recommendations

---

### Phase 7: Advanced Operations
**Narrative**: "Now let's showcase some of dbt's more advanced capabilities. We'll demonstrate combined builds that run models and tests together, create snapshots for historical tracking, and manage package dependencies. These features make dbt-core-mcp incredibly powerful for production workflows."

**Context**: Combined operations and specialized workflows.

#### 7.1 Combined Build
**Showcases**: Run + test in single operation
```
build_models(select="customers")
```
**Expected**: "7 resources: 1 model + 3 unit tests + 3 generic tests"

#### 7.2 Snapshots
**Showcases**: SCD Type 2 historical tracking
```
snapshot_models()
```
**Expected**: "1 snapshot succeeded" (customers_snapshot)

#### 7.3 Package Management
**Showcases**: Dependency installation
```
install_deps()
```
**Expected**: Success (no packages defined in jaffle_shop)

---

### Phase 8: Error Handling
**Narrative**: \"Finally, let's see how dbt-core-mcp handles errors intelligently. We'll demonstrate how it distinguishes between different types of problems - like when you use a selector that doesn't exist versus when you write invalid SQL. Good error handling is essential for a smooth development experience.\"\n\n**Context**: Demonstrate error discrimination between system failures and business outcomes.

#### 8.1 Invalid Selector (System Error)
**Showcases**: Clear error messages for non-existent selectors
```
test_models(select="nonexistent_selector_xyz")
```
**Expected**: "Internal error: Error calling tool 'test_models': No tests matched selector: nonexistent_selector_xyz"

#### 8.2 Invalid SQL (Query Error)  
**Showcases**: Database parser error handling
```
query_database(sql="INVALID SQL STATEMENT")
```
**Expected**: "Internal error: Error calling tool 'query_database': Query execution failed" with detailed DuckDB parser error
**Note**: Shows full dbt command output including compilation and database error details

---

## Verification Points

During each test, observe:
- **Tool Availability**: All dbt-core-mcp tools respond successfully
- **Progress Reporting**: Real-time updates during model/test execution  
- **Result Formatting**: JSON responses with status, execution times, row counts
- **SQL Compilation**: `{{ ref() }}` and `{{ source() }}` resolve to actual table names
- **Error Clarity**: Distinguish between selector errors vs SQL syntax errors vs test failures
- **TDD Cycle**: Red (test fails) → Green (test passes) → Refactor (build model)
- **Dependency Resolution**: Lineage shows correct upstream/downstream relationships
- **Schema Inspection**: Database columns match model definitions

## Notes

### Tool Behavior
- Tools return structured JSON responses with execution details
- Multiple resource matches return all options (e.g., "customers" model vs source)
- Selector syntax varies: use explicit names when wildcards fail
- Query results include row counts and formatted data

### File Management  
- TDD phase modifies `unit_tests/marts/customers_unit_tests.yml` - changes are temporary
- MCP server restart requires `.vscode/mcp.json` modification - **MUST restore after demo**
- Stale run_results.json is deleted before each execution to prevent caching

### Error Types
- **Tool errors**: "Internal error: Error calling tool..."
- **dbt errors**: Command execution failures with full dbt output
- **SQL errors**: Database parser errors with line numbers and context

### Success Indicators
- All tools respond without activation errors
- Tests show clear pass/fail status with failure details
- Models build successfully with execution times
- Queries return expected row counts and data formats

