---
applyTo: '**'
description: Guidelines for dbt unit testing workflow
---

# dbt Unit Testing Guidelines

## File Organization

**Rule**: Keep unit tests in separate `unit_tests/` directory, mirroring model structure

```
models/
  ├── staging/
  │   └── stg_customers.sql
  └── marts/
      └── customers.sql

unit_tests/
  ├── staging/
  │   └── stg_customers_unit_tests.yml
  └── marts/
      └── customers_unit_tests.yml
```

**Configuration** (`dbt_project.yml`):
```yaml
model-paths: ["models", "unit_tests"]
```

## TDD Workflow for Unit Tests

### 1. Create Test First (Red Phase)

**Step 1**: Inspect the model to understand dependencies
```
get_resource_info(name="customers", include_compiled_sql=true)
```

**Step 2**: Query sample data for realistic fixtures
```sql
query_database("SELECT * FROM {{ ref('stg_customers') }} LIMIT 3")
query_database("SELECT * FROM {{ ref('stg_orders') }} LIMIT 5")
```

**Step 3**: Create test YAML with:
- Descriptive name: `test_<behavior>` (e.g., `test_customer_with_single_order`)
- Clear description
- Minimal data covering the test case
- Expected output

**Step 4**: Run the specific test
```
test_models(select="test_name:test_customer_with_single_order")
```

### 2. Implement/Fix Model (Green Phase)

Edit or create the dbt model to make the test pass.

### 3. Refactor

Clean up the model while keeping tests green.

## Test Design Principles

**Minimal Data**: Only include columns/rows needed for the specific test case

**Fixture Reuse with YAML Anchors**: Reduce duplication by anchoring common input data

```yaml
# Define reusable fixtures at the top
_base_customers: &customer_input
  input: ref('stg_customers')
  rows:
    - {customer_id: 1, first_name: 'Alice', last_name: 'Smith'}
    - {customer_id: 2, first_name: 'Bob', last_name: 'Jones'}

unit_tests:
  - name: test_basic_aggregation
    model: customers
    given:
      - *customer_input      # Reuse the fixture as-is
      - input: ref('stg_orders')
        rows: [...]
  
  - name: test_with_different_customers
    model: customers
    given:
      - input: ref('stg_customers')  # Just write it out if data is different
        rows:
          - {customer_id: 99, first_name: 'Different', last_name: 'Customer'}
      - input: ref('stg_orders')
        rows: [...]
```

**Important**: 
- Anchor the **dict** (the input item), not a list
- Use `- *anchor_name` to reuse fixture as-is
- If you need different data, just write it inline - don't use merge
- YAML merge (`<<:`) does NOT append to arrays, it replaces them

**YAML Merge for Row Templates** (not fixtures):
Use merge at the row level to create variations of complex row structures:

```yaml
_customer_template: &customer
  customer_id: 1
  first_name: 'Alice'
  last_name: 'Smith'
  email: 'alice@example.com'
  phone: '555-1234'
  address: '123 Main St'
  city: 'Portland'
  # ... many more fields

unit_tests:
  - name: test_status_variations
    given:
      - input: ref('stg_customers')
        rows:
          - <<: *customer        # Base template
            status: 'active'     # Override one field
          
          - <<: *customer        # Same template
            status: 'inactive'   # Different status
            email: null          # Test null email
```

**Use merge when**:
- Row has many fields (10+)
- Testing variations by changing 1-2 fields
- Need to avoid repeating complex structures

**Edge Cases to Test**:
- Empty inputs (no orders, no customers)
- Single item (one order, one customer)
- Null handling (missing dates, optional fields)
- Boundary conditions (first/last dates matching)

**Naming**:
- File: `<model_name>_unit_tests.yml`
- Test: `test_<what_behavior>` (e.g., `test_aggregation_with_no_orders`)

## Fast Feedback Loop

Run specific tests, not the full suite:

```
# By test name
test_models(select="test_name:test_customer_aggregation")

# By file
test_models(select="unit_tests/marts/customers_unit_tests.yml")

# By model
test_models(select="customers")
```

## When to Write Unit Tests

- **Complex aggregations** (counting, min/max, grouping)
- **Edge cases** (nulls, empty sets, single items)
- **Business logic** (calculations, conditionals, case statements)
- **Critical models** (customer-facing, financial, regulatory)

## What NOT to Test

- Simple `select *` pass-through models
- Models with only renaming/casting
- Basic joins without transformations

## Test Data Guidelines

- **Keep it minimal**: 2-5 rows per input is usually enough
- **Use realistic data**: Query existing tables for schema/structure
- **Test one thing**: Each test should verify a specific behavior
- **Make it readable**: Use clear values that show what you're testing
- **Prefer dict/JSON format**: Use `{col: val}` format over CSV - it's order-independent, self-documenting, and easier to modify
- **Use YAML anchors selectively**: Only when 3+ tests in the same file share identical fixtures
- **Avoid CSV format**: Alignment issues, hard to modify, poor git diffs

**Data Format Examples:**

```yaml
# ✅ PREFERRED: Dict/JSON format (default)
given:
  - input: ref('stg_customers')
    rows:
      - {customer_id: 1, first_name: 'Alice', last_name: 'Smith'}
      - {customer_id: 2, first_name: 'Bob', last_name: 'Jones'}

# ⚠️  YAML anchors: Use when fixture appears in 3+ tests
_base_customers: &customer_input
  input: ref('stg_customers')
  rows:
    - {customer_id: 1, first_name: 'Alice'}

# ❌ CSV format: Avoid due to alignment issues
given:
  - input: ref('stg_customers')
    format: csv
    rows: |
      customer_id,first_name,last_name
      1,Alice,Smith
      2,Bob,Jones
```

## Common Pitfalls

**❌ Wrong**: Anchoring a list (can't mix with other items)
```yaml
_customers: &customers
  - input: ref('stg_customers')
    rows: [...]

given:
  *customers  # Can't add more items
```

**✅ Right**: Anchor the dict, reference as list item
```yaml
_customers: &customer_input
  input: ref('stg_customers')
  rows: [...]

given:
  - *customer_input  # Works! Can add more
  - input: ref('stg_orders')
```

## Prerequisites

Unit tests may reference staging models (via `ref()`), so ensure:
1. Seeds are loaded: `load_seeds()`
2. Staging models are built: `run_models(select="tag:staging")`

Or use `build_models()` to handle all prerequisites automatically.
