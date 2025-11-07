---
applyTo: '**'
description: Workspace-specific AI memory for this project
lastOptimized: '2025-11-07T04:24:28.171488+00:00'
entryCount: 1
optimizationVersion: 1
autoOptimize: true
sizeThreshold: 50000
entryThreshold: 20
timeThreshold: 7
---
# Workspace AI Memory
This file contains workspace-specific information for AI conversations.

## Universal Laws (strict rules that must always be followed)
- Law 1 (dbt-core-mcp Pre-Commit Validation Protocol):
  - STEP 1: Before staging/committing ANY code changes, run CI validation sequence in order:
    a) uv run ruff check src tests
    b) uv run mypy src tests
    c) uv run pytest
  - STEP 2: Verify ALL steps succeed with exit code 0
  - STEP 3: If ANY step fails, fix issues and restart from STEP 1
  - STEP 4: Only after all checks pass, proceed with git add/commit/push
  - APPLIES TO: All code commits in dbt-core-mcp workspace
  - VIOLATION PENALTY: Immediate acknowledgment and restart with correct procedure
  - NO EXCEPTIONS

## Memories
