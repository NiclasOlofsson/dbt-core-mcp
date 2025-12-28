---
applyTo: '**'
description: Workspace-specific AI memory for this project
lastOptimized: '2025-12-28T14:30:19.529094+00:00'
entryCount: 0
optimizationVersion: 6
autoOptimize: true
lastOptimizedTokenCount: 620
tokenGrowthThreshold: 1.2
---
# Workspace AI Memory
This file contains workspace-specific information for AI conversations.

## Universal Laws

1. **dbt-core-mcp Pre-Commit Validation Protocol**:
   - STEP 1: Before staging/committing ANY code changes, run CI validation sequence in order:
     - a) `uv run ruff check src tests`
     - b) `uv run pyright src tests`
     - c) `uv run pytest`
   - STEP 2: Verify ALL steps succeed with exit code 0
   - STEP 3: If ANY step fails, fix issues and restart from STEP 1
   - STEP 4: Only after all checks pass, proceed with `git add`/`commit`/`push`
   - APPLIES TO: All code commits in dbt-core-mcp workspace
   - VIOLATION PENALTY: Immediate acknowledgment and restart with correct procedure
   - NO EXCEPTIONS

2. **Selective Testing Protocol**:
   - DO NOT run `pytest` during development "just to check" or "to verify"
   - Tests are SLOW (5+ minutes for full suite) - respect user's time
   - ONLY run `pytest` in these cases:
     - a) Pre-commit validation (Law 1 requirement)
     - b) Explicitly requested by user
     - c) After fixing a specific failing test (run that test only, not full suite)
   - During development: rely on type checking (`pyright`) and linting (`ruff`)
   - CI will catch test failures - don't waste time with redundant local test runs
   - VIOLATION: Running tests "to make sure it works" or similar justifications
   - NO EXCEPTIONS

3. **MCP Server Restart Protocol**:
   - STEP 1: Update `.vscode/mcp.json` file by incrementing `_RESTART` counter (any change triggers restart)
   - STEP 2: **CRITICAL** - Do NOT invoke MCP tools in the same tool call batch as the mcp.json edit
   - STEP 3: Wait for next user interaction or separate tool invocation
   - STEP 4: Then invoke MCP tools for testing
   - REASON: Parallel execution causes tools to run BEFORE file edit completes
   - Server restarts asynchronously in background - do NOT use sleep commands
   - Framework handles restart timing automatically
   - APPLIES TO: dbt-core-mcp project MCP server management
   - NO EXCEPTIONS

## Policies

## Personal Context

## Professional Context

## Technical Preferences

## Communication Preferences

## Memories/Facts