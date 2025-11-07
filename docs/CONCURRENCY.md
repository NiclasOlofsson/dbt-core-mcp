# Concurrency Protection

The dbt-core-mcp server includes built-in concurrency protection to prevent conflicts when multiple DBT processes attempt to run simultaneously.

## Why Concurrency Protection?

DBT doesn't implement file-based locking to prevent concurrent executions. This can lead to several issues:

1. **Database Lock Conflicts**: File-based databases like DuckDB lock the database file during operations
2. **Manifest Corruption**: Multiple processes writing to `target/manifest.json` simultaneously
3. **State Inconsistency**: Race conditions between parsing and execution phases
4. **Resource Contention**: Competing for the same database connections or warehouse resources

## How It Works

The server uses process detection to identify running DBT commands:

```python
from dbt_core_mcp.utils.process_check import is_dbt_running, wait_for_dbt_completion

# Check if DBT is running
if is_dbt_running(project_dir):
    # Wait up to 30 seconds for completion
    wait_for_dbt_completion(project_dir, timeout=30.0)
```

### Detection Strategy

The `is_dbt_running()` function identifies DBT processes by:

1. **Command Detection**: Looks for actual `dbt` CLI commands:
   - `dbt run`, `dbt parse`, `dbt test`, etc.
   - `python -m dbt.cli.main` invocations
   - Standalone `dbt` or `dbt.exe` executables

2. **Filtering**: Excludes non-DBT processes:
   - MCP server itself (`dbt-core-mcp`)
   - Python processes that merely import dbt libraries
   - Unrelated processes with "dbt" in their names

3. **Project Matching**: Verifies the DBT process is working in the same project:
   - Compares process working directory with project directory
   - Checks for project path in command-line arguments

### Wait Behavior

The `wait_for_dbt_completion()` function:

- **Polls**: Checks every 0.2 seconds (5 times per second)
- **Timeout**: Default 10 seconds (configurable)
- **Returns**: `True` if DBT finished, `False` if timeout

## Scenarios

### Scenario 1: User Running DBT Manually

**Situation**: User runs `dbt run` in terminal while MCP server tries to execute `dbt parse`

**Behavior**:
1. MCP server detects running `dbt run` process
2. Logs: "DBT process detected, waiting for completion..."
3. Polls 5 times per second for up to 10 seconds
4. Once `dbt run` completes, MCP server executes `dbt parse`

### Scenario 2: Concurrent MCP Tool Calls

**Situation**: Multiple MCP tools called simultaneously

**Behavior**:
1. First tool starts executing DBT command
2. Second tool detects the process and waits
3. Sequential execution prevents conflicts

### Scenario 3: Timeout

**Situation**: User's DBT command takes longer than 10 seconds

**Behavior**:
1. MCP server waits for 10 seconds
2. Returns error: "DBT is already running in this project. Please wait for it to complete."
3. User can retry the MCP operation after their DBT command finishes

## Configuration

Currently, concurrency protection is enabled by default with these settings:

- **Timeout**: 10 seconds
- **Poll Interval**: 0.2 seconds (5 checks per second)

These are hardcoded in `BridgeRunner.invoke()` but can be made configurable in future versions.

### Why These Values?

- **Fast Polling (0.2s)**: Process checking is very cheap - just reading OS process info. Frequent polling provides responsive feedback without performance impact.
- **Short Timeout (10s)**: Most DBT parse/compile operations complete in seconds. Longer operations (like `dbt run`) typically indicate the user should wait anyway.

## Limitations

### What's Protected

✅ **Protected**:
- MCP server operations won't conflict with manual `dbt` CLI usage
- Multiple MCP tool calls are serialized
- Database lock conflicts are prevented
- Manifest corruption is avoided

### What's NOT Protected

❌ **Not Protected**:
- Multiple MCP server instances (different processes)
- DBT commands run via other tools (e.g., orchestrators like Airflow)
- Direct database access outside of DBT

### Known Edge Cases

1. **Process Detection Failure**: If `psutil` is not installed, process detection is disabled (assumes safe to proceed)
2. **Permission Denied**: Some processes may not be accessible due to OS permissions (skipped)
3. **Fast Commands**: Very short DBT commands might complete before detection
4. **Long-Running Commands**: Commands longer than 10 seconds will timeout (user should wait for completion and retry)

## Dependencies

- **psutil**: Cross-platform process detection library
  - Optional but highly recommended
  - If missing, concurrency protection is disabled with a warning

## Future Enhancements

Potential improvements for future versions:

1. **File-Based Locking**: Add `.dbt_mcp.lock` file for more reliable locking
2. **Configurable Timeout**: Allow users to configure wait timeout
3. **Lock Scope**: Support different lock scopes (project-level vs. global)
4. **Lock Visualization**: Show lock status in tool responses
5. **Priority Queue**: Implement priority for different operation types

## Testing

The concurrency protection includes comprehensive tests:

```bash
# Run concurrency tests
pytest tests/test_process_check.py -v
```

Tests cover:
- No process detection (clean state)
- Actual process detection (integration test)
- Timeout behavior
- Wait behavior

## Troubleshooting

### "DBT is already running" Error

**Problem**: MCP server reports DBT is running when you're not executing anything

**Solutions**:
1. Check for background DBT processes: `ps aux | grep dbt` (Linux/Mac) or Task Manager (Windows)
2. Kill stale DBT processes
3. Wait a few seconds and retry

### "Cannot check for running DBT processes" Warning

**Problem**: `psutil` not installed

**Solution**:
```bash
# Install psutil
uv pip install psutil
# or
pip install psutil
```

### False Positives

**Problem**: Server detects unrelated "dbt" processes

**Solution**: This should be rare due to filtering logic. If it occurs, please file an issue with:
- Process command line (`ps aux | grep dbt`)
- Expected vs. actual behavior

## Implementation Details

See source code:
- `src/dbt_core_mcp/utils/process_check.py`: Core detection logic
- `src/dbt_core_mcp/dbt/bridge_runner.py`: Integration with bridge runner
- `tests/test_process_check.py`: Test suite
