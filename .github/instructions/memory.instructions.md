---
applyTo: '**'
description: Workspace-specific AI memory for this project
lastOptimized: '2025-12-29T12:18:24.604279+00:00'
entryCount: 1
optimizationVersion: 8
autoOptimize: true
lastOptimizedTokenCount: 710
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
   - CONFIGURATION: Use `_RESTART` counter in `.vscode/mcp.json` env section to force server restarts
     - Increment the value (e.g., `"_RESTART": "71"` → `"72"`) to trigger VS Code to restart the MCP server
     - Essential for testing MCP server code changes without manually restarting VS Code
   - STEP 1: Update `.vscode/mcp.json` file by incrementing `_RESTART` counter
   - STEP 2: **CRITICAL** - Do NOT invoke MCP tools in the same tool call batch as the mcp.json edit
   - STEP 3: Wait for next user interaction or separate tool invocation
   - STEP 4: Then invoke MCP tools for testing
   - REASON: Parallel execution causes tools to run BEFORE file edit completes
   - Server restarts asynchronously in background - do NOT use sleep commands
   - Framework handles restart timing automatically
   - APPLIES TO: dbt-core-mcp project MCP server management
   - NO EXCEPTIONS

4. **Git Commit Amendment Protocol**:
   - When fixing issues in the MOST RECENT commit (before anyone else pulls):
     - Use `git commit --amend --no-edit` to fix without creating new commit
     - Use `git push --force` to rewrite remote history
     - Keeps git history clean for release notes
   - ONLY amend if commit hasn't been pulled by others
   - Examples: formatting fixes, typos, forgot to add files after initial commit
   - DO NOT create second commits for fixes to recent work (clutters release notes)
   - APPLIES TO: All git operations in dbt-core-mcp workspace
   - NO EXCEPTIONS

## Policies

## Personal Context

## Professional Context

## Technical Preferences

## Communication Preferences

## Memories/Facts