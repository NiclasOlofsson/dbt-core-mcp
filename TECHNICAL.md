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

```python
# Check project directory for environment indicators
if Path("Pipfile").exists() and Path("Pipfile.lock").exists():
    return ["pipenv", "run", "python"]

if Path("poetry.lock").exists():
    return ["poetry", "run", "python"]

if Path("environment.yml").exists() or Path("conda.yaml").exists():
    # Parse environment file for environment name
    return ["conda", "run", "-n", env_name, "python"]

if Path("venv/Scripts/python.exe").exists():  # Windows
    return ["venv/Scripts/python.exe"]

if Path("venv/bin/python").exists():  # Unix
    return ["venv/bin/python"]

# Fallback to system Python
return ["python"]
```

**Result:** dbt runs in the exact environment the user configured for their project. No manual interpreter selection needed.

**Why auto-detection:** This approach provides zero configuration for users, works with any Python environment manager, respects the project's dependency management choices, and validates that dbt is installed before attempting operations.

#### 2. Workspace context from VS Code

**Challenge:** How does a globally-installed MCP server know which project to operate on?

**Solution:** VS Code provides the workspace root automatically via MCP protocol:

```python
# MCP provides workspace context automatically
roots = await ctx.list_roots()
workspace_root = roots[0]  # Use first workspace folder
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

```python
# Find profiles.yml (project directory or ~/.dbt/)
profiles_path = project_dir / "profiles.yml"
if not profiles_path.exists():
    profiles_path = Path.home() / ".dbt" / "profiles.yml"

# Parse to get adapter type
profiles = yaml.safe_load(profiles_path.read_text())
profile_name = dbt_project["profile"]
target = profiles[profile_name]["target"]
adapter_type = profiles[profile_name]["outputs"][target]["type"]
```

**Result:** We find profiles.yml wherever the user put it (project directory or ~/.dbt/) and immediately enable adapter-specific features like Databricks warehouse pre-warming with API credentials extracted directly from profiles.yml.

**Note:** We only parse profiles.yml for MCP features. When running dbt commands, dbt does its own profiles.yml lookup.

#### 4. Automatic target selection

**Challenge:** Projects have multiple targets (dev, prod, staging). Which one should we use?

**Solution:** Use the default target from profiles.yml:

```python
# We use whatever target is in profiles.yml
target_name = profile.get("target", "default")
```

**Design Philosophy:** 
We focus on everyday developer work where developers use a single environment (typically dev). Most developers don't have production credentials in their local profiles - and shouldn't, for safety reasons. 

If you need to switch targets occasionally, set `DBT_TARGET` environment variable via mcp.json (dbt's standard mechanism).

#### 5. Validation before execution

**Challenge:** Catch configuration issues early with helpful error messages.

**Solution:** We validate everything on initialization, checking that the project exists (dbt_project.yml is present), profiles are configured (profiles.yml is found), dbt is installed in the detected Python environment, and the required adapter is available:

```python
async def _ensure_initialized():
    if not dbt_project_yml.exists():
        raise Error("No dbt_project.yml found in workspace")
    
    if not profiles_yml.exists():
        raise Error(f"No profiles.yml found in {profiles_dir}")
    
    result = subprocess.run(python_cmd + ["-c", "import dbt"])
    if result.returncode != 0:
        raise Error(f"dbt not installed in detected environment")
    
    result = subprocess.run(python_cmd + ["-c", f"import dbt.adapters.{adapter}"])
    if result.returncode != 0:
        raise Error(f"Adapter dbt-{adapter} not installed")
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
```bash
pipenv run python bridge.py    # For Pipfile projects
poetry run python bridge.py    # For poetry projects
conda run -n env python bridge.py    # For conda environments
venv/bin/python bridge.py      # For venv projects
```

This way, dbt runs with exactly the packages, versions, and configuration the user intended.

#### Problem 2: Command startup performance

**Challenge:** Every dbt command must read and parse the manifest.json file and load it into memory before execution. For large projects (1500 models, 500 sources, 850 macros), this manifest file can be 50+ MB and takes 4-6 seconds to load.

**Traditional Approach:** Run dbt as a one-shot CLI command for each operation.

**Cost:** Every single `dbt run`, `dbt test`, `dbt ls`, or query reads and parses the manifest from disk. With 10 operations in a workflow, you're waiting 50 seconds just reading the same file repeatedly.

**Bridge Solution:** Keep the dbt process **alive between operations**. Load the manifest once on first command, then keep it in memory for all subsequent commands. No more disk I/O, no more JSON parsing overhead.

This transforms the cost model:
```
Traditional: 5s manifest load + 2s execute = 7s per operation (×10 = 70s)
Bridge:      5s manifest load (once) + 2s execute per operation (×10 = 25s)
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
- `src/dbt_core_mcp/dbt/bridge.py`: Subprocess entry point, dbtRunner wrapper
- `src/dbt_core_mcp/dbt/bridge_runner.py`: Process manager, IPC handler, progress parser

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

### 1. Query Optimization: `--no-populate-cache` (70% faster)

**Problem:**
When users run a simple query, they were experiencing 6-7 second execution times even though the actual SQL took less than 200ms. The missing time was being consumed by dbt's default behavior of querying information_schema upfront to cache metadata for all tables and views in the database. For a single query, this cache population is pure overhead that users have to wait through.

**Solution:**
Add `--no-populate-cache` to `dbt show` commands:
```python
args = ["show", "--inline", sql, "--no-populate-cache"]
```

**Results:**
- Before: 6-7s query execution
- After: ~2s query execution  
- **Improvement: 70% faster** (4-5s saved per query)

**Trade-offs:**
- None! This optimization is specific to `dbt show` (query operations)
- The cache isn't needed for single query execution
- `dbt run` and `dbt build` commands use normal caching (separate optimization below)

### 2. Selective Caching: `--cache-selected-only` (40% faster selective runs)

**Problem:**
When running a single model with selection syntax like `dbt run -s bronze_d365__customerpackingslip`, users experienced a 3.2 second gap between concurrency setup and when the model actually started running. This time was consumed by information_schema queries even though dbt already knew which models to run. The issue is that dbt's default behavior caches metadata for all schemas in the database, scanning hundreds of tables that aren't relevant to the selected model.

**Solution:**
Add `--cache-selected-only` to selective runs:
```python
if cache_selected_only and (select or selector or select_state_modified):
    args.append("--cache-selected-only")
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

### 3. Persistent Manifest Loading (5s saved per operation)

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

### 4. Warehouse Pre-warming (Databricks-specific)

**Problem:**
Databricks serverless warehouses auto-suspend after inactivity. When dbt tries to connect to a stopped warehouse, it appears to wait with long timeouts (likely from the databricks-sql-connector's retry and backoff logic) before the warehouse becomes available. This adds 30-60 seconds of startup time to the first operation where users see no progress feedback.

**Solution:**
Proactively check and start the warehouse before dbt operations:
```python
async def prewarm_warehouse():
    # Query warehouse status via Databricks API
    if state == "RUNNING":
        return  # Already warm
    
    # Issue start command and poll until RUNNING (up to 5 minutes)
    # Show progress to user while waiting
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

### 1. Concurrent process detection

**Problem:** Multiple MCP operations running simultaneously could corrupt dbt state or database connections.

**Solution:** In-memory asyncio lock with process validation
```python
# Lock held by another operation
if self._process_lock.locked():
    raise RuntimeError("Another dbt operation is in progress")

# Acquire lock for this operation
async with self._process_lock:
    # Execute dbt command
    result = await self._invoke_persistent(args)
```

**Safety features:**
- Uses `asyncio.Lock()` to serialize operations within MCP server process
- Only one dbt command executes at a time
- Prevents concurrent access to shared dbt process and database connections
- Lock automatically released on operation completion (even on errors)

### 2. Graceful process shutdown

**Problem:** Killed processes leave orphaned database connections.

**Solution:** Signal handling and cleanup
```python
async def _stop_persistent_process():
    # Send shutdown command via stdin
    shutdown_msg = json.dumps({"shutdown": True})
    process.stdin.write(shutdown_msg)
    
    # Wait for graceful exit (5s timeout)
    await asyncio.wait_for(process.wait(), timeout=5.0)
    
    # Force kill if unresponsive
    if process.returncode is None:
        process.kill()
```

**Cleanup includes:**
- Database connection closure (via dbt adapter)
- Process termination confirmation
- Stdin/stdout stream cleanup

### 3. Error recovery

**Stale Process Detection:**
If bridge process crashes, next operation detects mismatch:
```python
if self._dbt_process and self._dbt_process.returncode is not None:
    logger.warning("Process died, restarting...")
    await self._start_persistent_process()
```

**Timeout Protection:**
Operations have configurable timeouts (default 300s):
```python
try:
    await asyncio.wait_for(process.wait(), timeout=self.timeout)
except asyncio.TimeoutError:
    process.kill()
    raise RuntimeError(f"Operation timed out after {self.timeout}s")
```

These mechanisms ensure reliable operation even in challenging scenarios.

## Smart tools for natural language

One of the design goals for dbt-core-mcp is enabling natural language interaction. Users shouldn't need to memorize dbt syntax or tool parameters - they should just be able to say "run my changes and test downstream" and have it work.

### Automatic state management

**The Problem:** dbt's state-based selection (detecting modified models) requires users to manage state directories manually:
```bash
# Traditional approach - user must manage state
dbt run --state path/to/previous/manifest --select state:modified+
```

**Our Solution:** Automatic state tracking with zero configuration and intelligent change detection.

**Smart Change Detection:** Before each operation, we check modification timestamps on project files (dbt_project.yml, models, sources, tests, macros) against the manifest. If files are newer than the manifest, we trigger a reparse. This ensures the manifest stays current without unnecessary reparsing on every operation.

**Automatic State Snapshots:** After every successful `run_models`, `build_models`, or `test_models` operation, we automatically copy the current manifest to `target/state_last_run/manifest.json`. When users request modified-only runs, we use this automatically:

```python
# User says: "run my changes"
# We translate to: dbt run --select state:modified --state target/state_last_run

# User says: "run my changes and downstream"  
# We translate to: dbt run --select state:modified+ --state target/state_last_run
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
