---
applyTo: '**'
description: Workspace-specific AI memory for this project
lastOptimized: '2026-01-14T02:34:41.769594+00:00'
entryCount: 7
optimizationVersion: 11
autoOptimize: true
lastOptimizedTokenCount: 1430
tokenGrowthThreshold: 1.2
---
# Workspace AI Memory
This file contains workspace-specific information for AI conversations.

## Universal Laws

1. **dbt-core-mcp Pre-Commit Validation Protocol**:
   - STEP 1: Before staging/committing ANY code changes, run CI validation sequence in order:
     - a) `uv run ruff check src tests`
     - b) `uv run ruff format --check src tests`
     - c) `uv run pyright src tests`
     - d) `uv run pytest`
   - STEP 2: Verify ALL steps succeed with exit code 0
   - STEP 3: If ANY step fails, fix issues and restart from STEP 1
   - STEP 4: Restore `.vscode/mcp.json` to original state (revert `_RESTART` counter changes)
     - Use `git restore .vscode/mcp.json` OR manually revert counter
     - NEVER commit restart counter changes - testing only
   - STEP 5: Only after all checks pass and cleanup complete, proceed with `git add`/`commit`/`push`
   - APPLIES TO: All code commits in dbt-core-mcp workspace
   - VIOLATION PENALTY: Immediate acknowledgment and restart with correct procedure
   - NO EXCEPTIONS

2. **Selective Testing Protocol**:
   - RUN `pytest` to validate implementations and verify code works
   - Tests provide definitive feedback on whether changes are correct
   - Use tests during development to catch issues early
   - For targeted testing: run specific test files or functions when fixing particular issues
   - Pre-commit validation always requires full test suite (Law 1 requirement)
   - Let tests decide if implementation works rather than guessing
   - For complete validation: `uv run ruff check src tests && uv run ruff format --check src tests && uv run pyright src tests && uv run pytest`
   - NO EXCEPTIONS

3. **Git Commit Amendment Protocol**:
   - When fixing issues in the MOST RECENT commit (before anyone else pulls):
     - Use `git commit --amend --no-edit` to fix without creating new commit
     - Use `git push --force` to rewrite remote history
     - Keeps git history clean for release notes
   - ONLY amend if commit hasn't been pulled by others
   - Examples: formatting fixes, typos, forgot to add files after initial commit
   - DO NOT create second commits for fixes to recent work (clutters release notes)
   - APPLIES TO: All git operations in dbt-core-mcp workspace
   - NO EXCEPTIONS

4. **Release Protocol (MANDATORY)**:
   - ALWAYS follow the complete release process documented in CONTRIBUTING.md
   - NEVER manually edit version in pyproject.toml or create tags with `git tag`
   - ALWAYS use bump-my-version tool for ALL releases
   - See CONTRIBUTING.md "Release Process" section for detailed steps including AI-assisted release notes
   - VIOLATION PENALTY: Immediate rollback and restart with correct procedure
   - APPLIES TO: All releases in dbt-core-mcp workspace
   - NO EXCEPTIONS

## Policies

- **CLI Command Precision**: Reuse exact same command without adding unnecessary utilities like `echo` or output concatenation. User needs to approve every command line exactly as written for precise control and auditability.
- **MCP Tool Usage**: DO NOT run dbt CLI commands directly (e.g., `dbt test`, `dbt run`, `dbt build`). ALWAYS use MCP tools instead: `mcp_dbt-core_test_models`, `mcp_dbt-core_run_models`, `mcp_dbt-core_build_models`, etc. The project is designed to work with the MCP server, not CLI execution.

## Personal Context

## Professional Context

## Technical Preferences

## Communication Preferences

## Suggestions/Hints

## Memories/Facts

### dbt-core-mcp Configuration

- **2026-01-10 01:55:** MCP server logs are located at `/tmp/dbt_core_mcp_logs/dbt_core_mcp.log` (on Windows: `%TEMP%\dbt_core_mcp_logs\dbt_core_mcp.log`). Configured in `src/dbt_core_mcp/__main__.py` `setup_logging()` function.