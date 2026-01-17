# Technical Architecture & Performance Optimizations

This document details the technical design decisions, architecture patterns, and performance optimizations implemented in dbt-core-mcp.

## Table of contents

- [Zero configuration philosophy](#zero-configuration-philosophy)
- [Bridge architecture](#bridge-architecture)
- [Performance optimizations](#performance-optimizations)
- [Safety mechanisms](#safety-mechanisms)

## Zero configuration philosophy

### What this means for users

The setup process has three steps: install the extension, open your dbt project folder, and start working. That's it. No configuration files, no paths to specify, no environment setup.

### The problem with configuration

Most dbt integrations require manual setup: specifying the Python interpreter path, configuring the dbt profiles directory location, setting the project root directory, choosing the target environment (dev/prod), and specifying the adapter type (databricks, snowflake, etc.).

**Our Philosophy:** Click install, it just works. No configuration files, no manual setup, no paths to specify.

### How we achieve zero config

#### 1. Automatic environment detection

**Challenge:** Users manage Python environments differently across projects.

**Solution:** We scan for environment markers and auto-detect the appropriate Python command:

```
DetectPythonEnvironment():
  if Pipfile and Pipfile.lock exist:
    return "pipenv run python"
  if poetry.lock exists:
    return "poetry run python"
  if environment.yml or conda.yaml exists:
    parse file for environment name
    return "conda run -n <env_name> python"
  if venv/Scripts/python.exe exists (Windows):
    return "venv/Scripts/python.exe"
  if venv/bin/python exists (Unix):
    return "venv/bin/python"
  return "python" (system fallback)
```

**Result:** dbt runs in the exact environment the user configured for their project. No manual interpreter selection needed.

**Why auto-detection:** This approach provides zero configuration for users, works with any Python environment manager, respects the project's dependency management choices, and validates that dbt is installed before attempting operations.

#### 2. Workspace context from VS Code

**Challenge:** How does a globally-installed MCP server know which project to operate on?

**Solution:** VS Code provides the workspace root automatically via MCP protocol:

```
GetWorkspaceRoot(mcp_context):
  workspace_roots = list_roots_from_mcp_context()
  if workspace_roots not empty:
    return workspace_roots[0]
```

**Result:** Install the MCP server once globally, then open any dbt project folder in VS Code and the server automatically operates in that project's context. No per-project configuration needed. The only requirement is that the folder you open must contain `dbt_project.yml` at its root.

**Override (if needed):** For edge cases, you can explicitly specify the project directory in mcp.json:
```json
{
  "mcpServers": {
    "dbt-core": {
      "command": "uvx",
      "args": ["dbt-core-mcp", "--project-dir", "/path/to/dbt/project"]
    }
  }
}
```

#### 3. Automatic adapter detection

**Challenge:** dbt supports many adapters (databricks, snowflake, postgres, bigquery, etc.). We need to know the adapter type to enable adapter-specific features like warehouse pre-warming.

**Solution:** Find and parse `profiles.yml` to get adapter information:

```
DetectAdapterType():
  profiles_path = project_dir/"profiles.yml"
  if not exists(profiles_path):
    profiles_path = home_dir/.dbt/"profiles.yml"
  profiles = parse_yaml(profiles_path)
  profile_name = dbt_project_config["profile"]
  target_name = profiles[profile_name]["target"]
  adapter_type = profiles[profile_name]["outputs"][target_name]["type"]
  return adapter_type
```

**Result:** We find profiles.yml wherever the user put it (project directory or ~/.dbt/) and immediately enable adapter-specific features like Databricks warehouse pre-warming with API credentials extracted directly from profiles.yml.

**Note:** We only parse profiles.yml for MCP features. When running dbt commands, dbt does its own profiles.yml lookup.

#### 4. Automatic target selection

**Challenge:** Projects have multiple targets (dev, prod, staging). Which one should we use?

**Solution:** Use the default target from profiles.yml:

```
SelectTarget():
  target_name = profile["target"] or "default"
  return target_name
```

**Design Philosophy:** 
We focus on everyday developer work where developers use a single environment (typically dev). Most developers don't have production credentials in their local profiles - and shouldn't, for safety reasons. 

If you need to switch targets occasionally, set `DBT_TARGET` environment variable via mcp.json (dbt's standard mechanism).

#### 5. Validation before execution

**Challenge:** Catch configuration issues early with helpful error messages.

**Solution:** We validate everything on initialization, checking that the project exists (dbt_project.yml is present), profiles are configured (profiles.yml is found), dbt is installed in the detected Python environment, and the required adapter is available:

```
ValidateSetup():
  if not exists(dbt_project.yml):
    error "No dbt_project.yml found in workspace"
  if not exists(profiles.yml):
    error "No profiles.yml found in <profiles_dir>"
  if not can_import(python_env, "dbt"):
    error "dbt not installed in detected environment"
  if not can_import(python_env, "dbt.adapters.<adapter_name>"):
    error "Adapter dbt-<adapter_name> not installed"
  all checks passed
```

**Result:** Clear errors like "dbt not installed in pipenv environment" instead of cryptic import failures.

### When configuration is needed

We support explicit configuration via environment variables for edge cases:

```json
{
  "mcpServers": {
    "dbt-core": {
      "env": {
        "DBT_PROFILES_DIR": "/custom/profiles/path",
        "DBT_TARGET": "production"
      }
    }
  }
}
```

These are passed through to the dbt subprocess and handled by dbt itself. **99% of users never need it**.

---

## Bridge architecture

### The two fundamental problems

The bridge architecture solves two critical problems from a different angle than traditional approaches:

#### Problem 1: Environment isolation

**Challenge:** dbt projects use diverse Python environments (venv, pipenv, conda, Poetry, etc.). The MCP server runs in VS Code's extension host with its own Python environment. These environments are incompatible - you cannot import dbt packages from one Python environment into another.

**Traditional Approach:** Install dbt in the MCP server's environment and try to point it at the project.

**Why That Fails:**
- Project may use different dbt version than MCP
- Project's custom packages/adapters not available in MCP environment
- Environment conflicts (incompatible dependency versions)
- Doesn't respect project's dependency management (Pipfile.lock, poetry.lock, etc.)

**Bridge Solution:** Launch dbt as a subprocess **in the project's own environment**. The MCP server detects the project's environment type and runs:
```
For Pipfile projects:
  pipenv run python bridge.py

For poetry projects:
  poetry run python bridge.py

For conda environments:
  conda run -n <env_name> python bridge.py

For venv projects:
  venv/bin/python bridge.py
```

This way, dbt runs with exactly the packages, versions, and configuration the user intended.

#### Problem 2: Command startup performance

**Challenge:** Every dbt command must read and parse the manifest.json file and load it into memory before execution. For large projects (1500 models, 500 sources, 850 macros), this manifest file can be 50+ MB and takes 4-6 seconds to load.

**Traditional Approach:** Run dbt as a one-shot CLI command for each operation.

**Cost:** Every single `dbt run`, `dbt test`, `dbt ls`, or query reads and parses the manifest from disk. With 10 operations in a workflow, you're waiting 50 seconds just reading the same file repeatedly.

**Bridge Solution:** Keep the dbt process **alive between operations**. Load the manifest once on first command, then keep it in memory for all subsequent commands. No more disk I/O, no more JSON parsing overhead.

This transforms the cost model:
```
Traditional approach:
  First operation:  5s manifest load + 2s execute = 7s
  Op 2-10 (each):   5s manifest load + 2s execute = 7s
  Total (10 ops):   70s

Bridge approach:
  First operation:  5s manifest load + 2s execute = 7s
  Op 2-10 (each):   0s load (in memory) + 2s execute = 2s
  Total (10 ops):   25s (64% faster)
```

This foundation makes all the performance optimizations in the next section possible and meaningful. Without persistent manifest loading, optimizing cache population or query execution would still leave you waiting 5 seconds reading manifest.json on every operation.

### How it works

```
┌─────────────────┐
│  MCP Server     │  (VS Code extension host)
│  (Python venv)  │
└────────┬────────┘
         │ stdin/stdout IPC
         ▼
┌─────────────────┐
│  Bridge Script  │  (pipenv run python bridge.py)
│  dbt Process    │  
└────────┬────────┘
         │ dbtRunner API
         ▼
┌─────────────────┐
│  dbt-core       │
│  Databricks     │
└─────────────────┘
```

**Communication Protocol:**
1. MCP server detects project Python environment (pipenv, venv, etc.)
2. Launches `bridge.py` in detected environment via subprocess
3. Bridge loads dbt and parses manifest on startup
4. MCP sends JSON commands via stdin: `{"command": ["run", "-s", "model"]}`
5. Bridge executes via dbtRunner API, streams output to stdout
6. Returns JSON result: `{"success": true}`

**Process Lifecycle:**
- Starts on first command
- Persists between operations (manifest stays loaded)
- Graceful shutdown on MCP server exit
- Automatic restart if process crashes

### Bridge implementation

**Key Files:**
- [bridge_runner.py](src/dbt_core_mcp/dbt/bridge_runner.py): Process manager, IPC handler, progress parser, subprocess entry point for dbtRunner

**Streaming Output:**
The bridge streams dbt output in real-time, parsing progress indicators:
```
12:04:38  1 of 5 START sql table model public.customers  [RUN]
12:04:42  1 of 5 OK created sql table model public.customers  [OK in 4.2s]
```

**Why JSON lines:** This stdin/stdout IPC approach is simple (no network stack, no ports, no authentication), reliable (OS-level pipe guarantees message delivery), debuggable (can manually test bridge with stdin/stdout), and portable (works on Windows/Linux/macOS). The stream parser buffers lines until completion, parses progress indicators in real-time, and extracts the final JSON result from the last line.

**Why progress streaming:** Real-time progress updates create a better user experience. VS Code shows progress bars during long operations, displays which model is currently executing, shows elapsed time per model, and provides clear feedback instead of silent waiting. This is especially important when AI agents make multiple sequential dbt calls.

## Performance optimizations

These optimizations were developed and tested on a production dbt project with 1500 models, 500 sources, 850 macros, and 30 seeds running on Databricks. The performance improvements are real-world measurements from this scale of project.

### 1. CTE extraction with dbt compilation

**Problem:**
When debugging or inspecting intermediate transformation steps, users needed to manually copy CTEs from model files, figure out upstream dependencies, and paste them into separate queries. This workflow broke when CTEs referenced other CTEs or used `{{ ref() }}` or `{{ source() }}` macros—requiring users to manually resolve all dependencies and templating before they could run a query.

**Solution:**
The `query_database` tool supports extracting and querying individual CTEs from dbt models with full compilation:

```python
# Query a specific CTE with optional filtering
query_database(
    cte_name="customer_agg",
    model_name="customers", 
    sql="WHERE order_count > 5 LIMIT 10"
)
```

**How It Works:**

1. **CTE Extraction**: Parses the model file to identify the target CTE and its upstream dependencies
2. **Dependency Resolution**: Recursively includes all CTEs that the target CTE references
3. **SQL Generation**: Generates a query with all dependent CTEs, then selects from the target CTE
4. **SQL Composition**: Optionally wraps the result to apply user filters/limits
5. **Execution**: Runs the composed query through `dbt show` (which handles `{{ ref() }}` and `{{ source() }}` resolution)

**Technical Implementation:**

The `extract_cte_sql` function ([query_database.py](src/dbt_core_mcp/tools/query_database.py)):
- Uses the same CTE generator logic as unit test fixture generation
- Parses the model's raw SQL file to extract the target CTE definition
- Recursively traces CTE dependencies within the model
- Generates: `WITH cte_dep1 AS (...), cte_dep2 AS (...), target_cte AS (...) SELECT * FROM target_cte [user_sql]`
- Writes to a temporary file (system temp directory to avoid dbt detecting it as a model)
- Returns the complete SQL string for execution via `dbt show`

**Use Cases:**

- **Debugging**: Inspect intermediate transformation steps to find where data issues occur
- **Validation**: Verify CTE logic produces expected results before full model execution
- **Fixture Creation**: Query CTEs to get realistic data shapes for unit test fixtures
- **Exploration**: Understand complex models by examining each step in isolation

**Performance Considerations:**

- CTE extraction overhead: Fast, just parsing raw SQL (not running dbt compile)
- dbt show execution: ~2s for first query (manifest load), < 1s for subsequent queries (warm manifest)
- No database overhead: Only the requested CTE executes, not the entire model
- Temp file cleanup: Automatically removes temporary SQL files after use

**Error Handling:**

- Clear errors if CTE doesn't exist in the model
- Reports syntax errors in the model SQL
- Handles circular CTE dependencies gracefully

**Location:** [query_database.py](src/dbt_core_mcp/tools/query_database.py), `extract_cte_sql()` function

### 2. Query Optimization: `--no-populate-cache` (70% faster)

**Problem:**
When users run a simple query, they were experiencing 6-7 second execution times even though the actual SQL took less than 200ms. The missing time was being consumed by dbt's default behavior of querying information_schema upfront to cache metadata for all tables and views in the database. For a single query, this cache population is pure overhead that users have to wait through.

**Solution:**
Add `--no-populate-cache` flag to `dbt show` commands:
```
ExecuteQuery(sql):
  args = ["show", "--inline", sql, "--no-populate-cache"]
  result = run_dbt(args)
  return result
```

**Results:**
- Before: 6-7s query execution
- After: ~2s query execution  
- **Improvement: 70% faster** (4-5s saved per query)

**Trade-offs:**
- None! This optimization is specific to `dbt show` (query operations)
- The cache isn't needed for single query execution
- `dbt run` and `dbt build` commands use normal caching (separate optimization below)
**Location:** [query_database.py](src/dbt_core_mcp/tools/query_database.py)

### 3. Selective Caching: `--cache-selected-only` (40% faster selective runs)

**Problem:**
When running a single model with selection syntax like `dbt run -s bronze_d365__customerpackingslip`, users experienced a 3.2 second gap between concurrency setup and when the model actually started running. This time was consumed by information_schema queries even though dbt already knew which models to run. The issue is that dbt's default behavior caches metadata for all schemas in the database, scanning hundreds of tables that aren't relevant to the selected model.

**Solution:**
Add `--cache-selected-only` flag to selective runs:
```
BuildCommand(args):
  if cache_selected_only is enabled AND (selection is active):
    args.append("--cache-selected-only")
  return args
```

Only caches schemas containing selected models.

**Results:**
- Before: 3.2s cache phase for single model
- After: 1.5s cache phase
- **Improvement: 40% faster** (1.7s saved)
- Database queries: "very few" instead of hundreds

**Trade-offs:**
- Won't detect schema drift in uncached schemas until runtime
- Safe for development iteration (CI/production runs use full cache)
- Can be disabled: `cache_selected_only=False` parameter

**Why default to enabled:** We default `cache_selected_only=True` because 99% of MCP usage is development iteration with selective runs where the 40% performance improvement is significant. Users can override for edge cases with `cache_selected_only=False`. Full runs (CI/production) always use full cache and catch schema drift, so this only affects the safe subset of selective development runs.

**When Applied:****
- ✅ `run_models(select="my_model")` - selective run
- ✅ `run_models(select_state_modified=True)` - modified models only  
- ❌ `run_models()` - full run, uses full cache
- ❌ `run_models(exclude="tag:deprecated")` - exclusion-only = broad run

**Location:** [build_models.py](src/dbt_core_mcp/tools/build_models.py), [run_models.py](src/dbt_core_mcp/tools/run_models.py), [test_models.py](src/dbt_core_mcp/tools/test_models.py)

### 4. Persistent Manifest Loading (5s saved per operation)

**Problem:**
dbt must parse all models/sources/tests before each operation. For 1471 models, this takes ~5 seconds.

**Solution:**
Keep dbt process alive between operations. Manifest parsed once on startup, reused for all subsequent commands.

**Results:**
- First command: 5s manifest load + execution
- Subsequent commands: 0s manifest load + execution
- **Savings: 5s per operation** (after first)

**Implementation:**
- Process started on first MCP tool call
- Manifest loaded once in bridge process
- `dbt ls` results cached in memory
- Graceful shutdown on MCP server exit

**Location:** [bridge_runner.py](src/dbt_core_mcp/dbt/bridge_runner.py)

### 5. Warehouse Pre-warming (Databricks-specific)

**Problem:**
Databricks serverless warehouses auto-suspend after inactivity. When dbt tries to connect to a stopped warehouse, it appears to wait with long timeouts (likely from the databricks-sql-connector's retry and backoff logic) before the warehouse becomes available. This adds 30-60 seconds of startup time to the first operation where users see no progress feedback.

**Solution:**
Proactively check and start the warehouse before dbt operations:
```
PreWarmDatabricksWarehouse():
  warehouse_status = query_databricks_api(warehouse_id)
  if status == "RUNNING":
    return (already warm)
  if status == "STOPPED":
    report_progress("Starting warehouse...")
    start_warehouse_via_api(warehouse_id)
    poll until status == "RUNNING" (timeout: 5 minutes)
    report_progress as we wait
  return (warehouse ready)
```

**Current Implementation:**
This optimization currently only supports Databricks clusters but uses the same extensible adapter pattern as Python environment detection. Adding support for other warehouses (Snowflake, BigQuery, Redshift) follows the same pattern: detect adapter type from profiles.yml and apply adapter-specific pre-warming logic.

**Future Extensions:**
This adapter-specific pattern opens possibilities for exposing warehouse-specific features through dedicated MCP tools. For example, Databricks could expose cluster management tools, Snowflake could expose warehouse sizing tools, and BigQuery could expose slot reservation tools. These would only appear when the appropriate adapter is detected, providing a tailored experience for each platform.

**Results:**
- Starts warehouse before first dbt operation (if stopped)
- Eliminates 30-60s cold-start penalty on first operation
- Subsequent operations hit warm warehouse
- User sees progress: "Pre-warming warehouse..."
- Uses existing credentials from profiles.yml (no additional configuration needed)

### Performance summary

**Query Operations:**
- Cold start: ~7s (manifest load + query)
- Warm execution: ~2s (query only, 70% faster than before optimization)

**Selective Runs (single model):**
- Cold start: ~12s (manifest + selective cache + execution)
- Warm execution: ~7s (40% faster cache phase)

**Why this matters:**

These improvements may seem modest in isolation, but they fundamentally change the developer experience. When you're executing actual SQL that takes 200ms, waiting 6 seconds for overhead feels broken. The optimization brings response time in line with expectations - fast operations feel fast.

The impact compounds dramatically in AI agent workflows. When an agent makes 10-20 dbt operations in a single turn (common when analyzing data, debugging models, or exploring lineage), these optimizations transform the experience from frustratingly slow to responsive and natural. A workflow that would have taken 2+ minutes now completes in under 30 seconds, maintaining flow state rather than breaking it.

Speed isn't just convenience - it enables new interaction patterns. Fast enough response times make iterative exploration feel natural, encouraging developers to ask more questions and dig deeper into their data.

## Safety mechanisms

The dbt-core-mcp architecture implements multi-layered safety mechanisms to prevent data corruption, database connection conflicts, and race conditions. These layers protect against concurrent execution both within a single MCP server and across multiple MCP server instances.

### 1. Initialization synchronization

**Problem:** Multiple MCP tool calls might attempt to initialize dbt components simultaneously (e.g., parsing the manifest, detecting adapters).

**Solution:** Async lock in SharedState prevents concurrent initialization
```
Initialize():
  acquire initialization_lock
    if project_dir not set:
      detect workspace roots from MCP context
    if manifest not loaded:
      load manifest.json
  release initialization_lock
```

**Safety features:**
- Lock is exclusive - only one tool initializes at a time
- Prevents multiple concurrent manifest parsings
- Prevents conflicting adapter detections
- Lock automatically released after initialization completes (even on errors)

**Location:** [server.py](src/dbt_core_mcp/server.py)

### 2. Operation-level concurrency control (within MCP server)

**Problem:** Multiple MCP tool calls within the same server instance could execute dbt commands simultaneously on the shared persistent process, corrupting state.

**Solution:** Operation lock in BridgeRunner serializes all dbt command execution
```
RunDbtCommand(args):
  acquire operation_lock
    if persistent_process is not running:
      start persistent dbt process
      load manifest.json (one-time cost)
    
    report_progress("Starting dbt command")
    result = execute_on_persistent_process(args)
  release operation_lock
  return result
```

**Safety features:**
- Lock is exclusive - only one dbt command can execute at a time
- Operations queue up on the lock - they don't fail, they wait their turn
- Prevents concurrent access to shared dbt process state and database connections
- Lock automatically released on operation completion (even on errors)
- Progress callbacks report "Waiting for available process..." while queued

**Location:** [bridge_runner.py](src/dbt_core_mcp/dbt/bridge_runner.py)

### 3. Cross-process detection (between dbt processes)

**Problem:** Multiple dbt processes running on the same project cause conflicts: database locks, manifest corruption, state directory races. This can happen when: user opens same project in two VS Code windows (two MCP servers), runs dbt CLI commands manually while MCP is working, or CI jobs run simultaneously.

**Solution:** Before each operation, detect and wait for external dbt processes
```
BeforeDbtOperation():
  external_pid = our_persistent_process.pid
  
  if is_dbt_running_in_project(exclude_pid=external_pid):
    report_progress("Waiting for another dbt process to finish...")
    
    if not wait_for_completion(timeout=10 seconds):
      return error("dbt is already running in this project")
  
  proceed with dbt operation
```

**Smart process detection features:**
- Scans all running processes to find dbt instances (uses system process API)
- Ignores MCP's own persistent dbt processes (they don't interfere)
- Only detects actual dbt CLI commands: `dbt run`, `dbt parse`, `dbt test`, etc.
- Checks both working directory and command-line arguments to match the exact project
- Robust error handling - if process scanning unavailable, assumes safe to proceed
- Graceful degradation on permission errors (can't access some processes)

**Wait-and-retry behavior:**
- Waits up to 10 seconds for external dbt processes to finish (polls every 0.2s)
- Reports progress to user: "Waiting for another dbt process to finish..."
- Returns clear error if timeout occurs, instructing user to wait
- Prevents silent failures from concurrent dbt execution

**Location:** [process_check.py](src/dbt_core_mcp/utils/process_check.py) and invoked in [bridge_runner.py](src/dbt_core_mcp/dbt/bridge_runner.py)

### 4. Graceful process lifecycle management

**Problem:** Abruptly killing the dbt process leaves database connections open, corrupts manifest state, and may trigger adapter cleanup failures.

**Solution:** Graceful shutdown with signal handling and timeout fallback
```
StopDbtProcess():
  signal process to shutdown gracefully
    - process closes database connections
    - process unloads manifest
  
  wait for process exit (5 second timeout):
    if process exits gracefully:
      confirm successful cleanup
    else (timeout):
      force kill process
      log warning about forced termination
```

**Cleanup sequence:**
- Database connection closure (via dbt adapter's cleanup)
- Manifest unloading and state flushing
- Process termination confirmation
- Stdin/stdout stream cleanup
- Automatic detection of stale processes (see below)

**Location:** [bridge_runner.py](src/dbt_core_mcp/dbt/bridge_runner.py)

### 5. Automatic error recovery

**Stale Process Detection:**
If the persistent dbt process crashes, the next operation detects it:
```
BeforeDbtOperation():
  if persistent_process exists and has exit code set:
    log "Process died, discarding stale process"
    persistent_process = None
  
  if no persistent_process:
    start fresh process
    load manifest.json
```

**Timeout Protection:**
Operations have configurable timeouts (default 300 seconds):
```
ExecuteCommand(args, timeout):
  start_time = now
  
  while executing:
    if (now - start_time) > timeout:
      kill process
      return error("dbt operation timed out")
  
  return result
```

**Location:** [bridge_runner.py](src/dbt_core_mcp/dbt/bridge_runner.py)

### Safety summary

| Layer | Scope | Mechanism | Prevents |
|-------|-------|-----------|----------|
| **Initialization** | Within MCP server | `asyncio.Lock()` | Concurrent manifest parsing, adapter detection conflicts |
| **Operation** | Within MCP server | `asyncio.Lock()` + persistent process | Concurrent dbt command execution, shared process state corruption |
| **Cross-process** | Across MCP servers & CLI | Process detection + wait | Multiple dbt processes on same project, database locks, manifest races |
| **Recovery** | Process lifecycle | Graceful shutdown + stale detection | Orphaned connections, corrupt state, zombie processes |

**Example scenario:** User has two VS Code windows open on the same dbt project. First window starts `dbt run`. Second window tries to run tests. The second window detects the first window's dbt process (via process detection), waits 10 seconds, then either succeeds when first completes or reports a clear error. No corruption, no silent failures, no deadlocks.

## Smart tools for natural language

One of the design goals for dbt-core-mcp is enabling natural language interaction. Users shouldn't need to memorize dbt syntax or tool parameters - they should just be able to say "run my changes and test downstream" and have it work.

### Automatic state management

**The Problem:** dbt's state-based selection (detecting modified models) requires users to manage state directories manually: `dbt run --state /path/to/manifest --select state:modified+`

**Our Solution:** Automatic state tracking with zero configuration and intelligent change detection.

**Smart Change Detection:**
```
BeforeDbtOperation():
  manifest_time = modification_time(target/manifest.json)
  if any project file newer than manifest_time:
    trigger "dbt parse" to refresh manifest
  ensure manifest is current
```

**Automatic State Snapshots:**
```
AfterSuccessfulOperation(operation_type):
  if operation_type in ("run", "test", "build"):
    copy target/manifest.json to target/state_last_run/manifest.json
    (happens automatically, user doesn't configure it)
```

**State-based selection for users:**
```
When user requests:
  "run my changes" →
    dbt run --select state:modified --state target/state_last_run
  "run my changes and downstream" →
    dbt run --select state:modified+ --state target/state_last_run
```

**What This Enables:**

Users can work naturally without thinking about state directories:
- "Run only what I changed" → `select_state_modified=True`
- "Run my changes and everything downstream" → `select_state_modified_plus_downstream=True`
- No manual state management required
- No configuration needed
- Previous run state is always available

**Implementation:** The state directory is created automatically in the project's target folder. Users never see it, never configure it, never think about it. It just works.

This is representative of the broader philosophy: make the tools smart enough that AI assistants can translate natural language into proper dbt operations without requiring users to understand dbt's command-line syntax.
