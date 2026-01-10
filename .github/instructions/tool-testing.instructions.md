---
applyTo: '**'
description: Protocol for testing dbt-core-mcp tool changes
---

# Tool Testing Protocol

## Purpose
Autonomous test sequence to showcase tool functionality, progress reporting, and error handling.

## When to Run This Protocol

Execute this test sequence when the user requests:
- "show and tell" / "demo" / "demonstrate"
- "run the test sequence" / "run tool tests"
- "showcase the tools" / "test the tools"
- "verify the changes" / "test the progress reporting"

## Execution Steps

1. Increment `_RESTART` in `.vscode/mcp.json` to restart the MCP server
2. MUST Wait briefly for server to restart. If needed, use pause command.
3. Execute the test sequence below.

YOU MUST run each step in order, observing the expected outputs and behaviors. Do not call the next step until the current step is fully verified.

For each step, before running, briefly explain to the user what the step is demonstrating. After running, summarize the observed results and confirm they match expectations. Don't overuse emotes; use them sparingly to highlight key moments.

## Test Sequence

### 1. Load Seeds
**Showcases**: Basic execution with simple progress summary
```
load_seeds()
```
**Expected**: "3 seeds loaded" (or count from your project)

### 2. Run Staging Models
**Showcases**: Model execution with status breakdown
```
run_models(select="tag:staging")
```
**Expected**: "N models: X succeeded, Y failed, Z skipped"

### 3. Run All Tests
**Showcases**: Test execution with detailed status counts
```
test_models()
```
**Expected**: "N tests: X passed, Y failed, Z warned, W skipped"

### 4. Build Customer Mart
**Showcases**: Combined execution (models + tests) with resource summary
```
build_models(select="customers")
```
**Expected**: "N resources: X succeeded, Y failed, Z skipped"

### 5. Error Handling - Invalid Selector
**Showcases**: System error when dbt can't execute (no run_results.json)
```
test_models(select="nonexistent_selector_xyz_invalid")
```
**Expected**: RuntimeError with "Compilation Error" message

### 6. Error Handling - Business Outcome
**Showcases**: Success response with failed test results (has run_results.json)
```
# First, make customers.sql fail by adding duplicates
# Then run: test_models(select="customers")
```
**Expected**: `{"status": "success", "results": [...]}` with failed test in results array

## Verification Points

During each test, observe:
- **Real-time progress**: Incremental updates as resources execute
- **Final summary**: Tool-specific message after completion
- **Error discrimination**: System errors (RuntimeError) vs business outcomes (success with failed results)

## Notes

- Progress messages use `ctx.report_progress()` - visible in interactive sessions
- Final summaries are tool-specific, not generic "Completed" messages
- Stale run_results.json is deleted before each execution to prevent caching

