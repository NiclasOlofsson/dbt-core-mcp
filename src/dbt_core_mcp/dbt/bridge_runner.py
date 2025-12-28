"""
Bridge Runner for dbt.

Executes dbt commands in the user's Python environment via subprocess,
using an inline Python script to invoke dbtRunner.
"""

import asyncio
import json
import logging
import platform
import re
import time
from pathlib import Path
from typing import Any, Callable

import psutil

from ..utils.env_detector import get_env_vars
from ..utils.process_check import is_dbt_running, wait_for_dbt_completion
from .runner import DbtRunnerResult

logger = logging.getLogger(__name__)


class BridgeRunner:
    """
    Execute dbt commands in user's environment via subprocess bridge.

    This runner executes DBT using the dbtRunner API within the user's
    Python environment, avoiding version conflicts while still benefiting
    from dbtRunner's structured results.
    """

    def __init__(self, project_dir: Path, python_command: list[str], timeout: float | None = None):
        """
        Initialize the bridge runner.

        Args:
            project_dir: Path to the dbt project directory
            python_command: Command to run Python in the user's environment
                          (e.g., ['uv', 'run', 'python'] or ['/path/to/venv/bin/python'])
            timeout: Timeout in seconds for dbt commands (default: None for no timeout)
        """
        self.project_dir = project_dir.resolve()  # Ensure absolute path
        self.python_command = python_command
        self.timeout = timeout
        self._target_dir = self.project_dir / "target"
        self._project_config: dict[str, Any] | None = None  # Lazy-loaded project configuration
        self._project_config_mtime: float | None = None  # Track last modification time

        # Detect profiles directory (project dir or ~/.dbt)
        self.profiles_dir = self.project_dir if (self.project_dir / "profiles.yml").exists() else Path.home() / ".dbt"
        logger.info(f"Using profiles directory: {self.profiles_dir}")

    def _get_project_config(self) -> dict[str, Any]:
        """
        Lazy-load and cache dbt_project.yml configuration.
        Reloads if file has been modified since last read.

        Returns:
            Dictionary with project configuration
        """
        import yaml

        project_file = self.project_dir / "dbt_project.yml"

        # Check if file exists and get modification time
        if project_file.exists():
            current_mtime = project_file.stat().st_mtime

            # Reload if never loaded or file has changed
            if self._project_config is None or self._project_config_mtime != current_mtime:
                try:
                    with open(project_file) as f:
                        loaded_config = yaml.safe_load(f)
                        self._project_config = loaded_config if isinstance(loaded_config, dict) else {}
                    self._project_config_mtime = current_mtime
                except Exception as e:
                    logger.warning(f"Failed to parse dbt_project.yml: {e}")
                    self._project_config = {}
                    self._project_config_mtime = None
        else:
            self._project_config = {}
            self._project_config_mtime = None

        return self._project_config if self._project_config is not None else {}

    async def invoke(self, args: list[str], progress_callback: Callable[[int, int, str], Any] | None = None, expected_total: int | None = None) -> DbtRunnerResult:
        """
        Execute a dbt command via subprocess bridge.

        Args:
            args: dbt command arguments (e.g., ['parse'], ['run', '--select', 'model'])
            progress_callback: Optional async callback for progress updates.
                             Called with (current, total, message) for each model processed.
            expected_total: Optional expected total count from pre-execution `dbt list`.
                          If provided, progress will start with correct total immediately.

        Returns:
            Result of the command execution
        """
        # Check if dbt is already running and wait for completion
        if is_dbt_running(self.project_dir):
            logger.info("dbt process detected, waiting for completion...")
            if not wait_for_dbt_completion(self.project_dir, timeout=10.0, poll_interval=0.2):
                logger.error("Timeout waiting for dbt process to complete")
                return DbtRunnerResult(
                    success=False,
                    exception=RuntimeError("dbt is already running in this project. Please wait for it to complete."),
                )

        # Build inline Python script to execute dbtRunner
        script = self._build_script(args)

        # Execute in user's environment
        full_command = [*self.python_command, "-c", script]

        logger.info(f"Executing dbt command: {args}")
        logger.info(f"Using Python: {self.python_command}")
        logger.info(f"Working directory: {self.project_dir}")

        # Get environment-specific variables (e.g., PIPENV_IGNORE_VIRTUALENVS for pipenv)
        env_vars = get_env_vars(self.python_command)
        env = None
        if env_vars:
            import os

            env = os.environ.copy()
            env.update(env_vars)
            logger.info(f"Adding environment variables: {list(env_vars.keys())}")

        proc = None
        try:
            logger.info("Starting subprocess...")
            # Use create_subprocess_exec for proper async process handling
            proc = await asyncio.create_subprocess_exec(
                *full_command,
                cwd=self.project_dir,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                stdin=asyncio.subprocess.DEVNULL,
                env=env,
            )

            # Stream output and capture progress if callback provided
            if progress_callback:
                logger.info("Progress callback provided, enabling streaming output")
                stdout, stderr = await self._stream_with_progress(proc, progress_callback, expected_total)
            else:
                logger.info("No progress callback, using buffered output")
                # Wait for completion with timeout (original behavior)
                try:
                    stdout_bytes, stderr_bytes = await asyncio.wait_for(
                        proc.communicate(),
                        timeout=self.timeout,
                    )
                    stdout = stdout_bytes.decode("utf-8") if stdout_bytes else ""
                    stderr = stderr_bytes.decode("utf-8") if stderr_bytes else ""
                except asyncio.TimeoutError:
                    # Kill process on timeout
                    logger.error(f"dbt command timed out after {self.timeout} seconds, killing process")
                    proc.kill()
                    await proc.wait()
                    return DbtRunnerResult(
                        success=False,
                        exception=RuntimeError(f"dbt command timed out after {self.timeout} seconds"),
                    )

            returncode = proc.returncode
            logger.info(f"Subprocess completed with return code: {returncode}")

            # Parse result from stdout
            if returncode == 0:
                # Extract JSON from last line (DBT output may contain logs)
                try:
                    last_line = stdout.strip().split("\n")[-1]
                    output = json.loads(last_line)
                    success = output.get("success", False)
                    logger.info(f"dbt command {'succeeded' if success else 'failed'}: {args}")
                    return DbtRunnerResult(success=success, stdout=stdout, stderr=stderr)
                except (json.JSONDecodeError, IndexError) as e:
                    # If no JSON output, check return code
                    logger.warning(f"No JSON output from dbt command: {e}. stdout: {stdout[:200]}")
                    return DbtRunnerResult(success=True, stdout=stdout, stderr=stderr)
            else:
                # Non-zero return code indicates failure
                error_msg = stderr.strip() if stderr else stdout.strip()
                logger.error(f"dbt command failed with code {returncode}")
                logger.error(f"stdout: {stdout[:500]}")
                logger.error(f"stderr: {stderr[:500]}")

                # Try to extract meaningful error from stderr or stdout
                if not error_msg and stdout:
                    error_msg = stdout.strip()

                return DbtRunnerResult(
                    success=False,
                    exception=RuntimeError(f"dbt command failed (exit code {returncode}): {error_msg[:500]}"),
                    stdout=stdout,
                    stderr=stderr,
                )

        except asyncio.CancelledError:
            # Kill the subprocess when cancelled
            if proc and proc.returncode is None:
                logger.info(f"Cancellation detected, killing subprocess PID {proc.pid}")
                await asyncio.shield(self._kill_process_tree(proc))
            raise
        except Exception as e:
            logger.exception(f"Error executing dbt command: {e}")
            # Clean up process on unexpected errors
            if proc and proc.returncode is None:
                proc.kill()
                await proc.wait()
            return DbtRunnerResult(success=False, exception=e, stdout="", stderr="")

    async def _stream_with_progress(self, proc: asyncio.subprocess.Process, progress_callback: Callable[[int, int, str], Any], expected_total: int | None = None) -> tuple[str, str]:
        """
        Stream stdout/stderr and report progress in real-time.

        Parses dbt output for progress indicators like:
        - "1 of 5 START sql table model public.customers"
        - "1 of 5 OK created sql table model public.customers"

        Args:
            proc: The running subprocess
            progress_callback: Async callback(current, total, message)

        Returns:
            Tuple of (stdout, stderr) as strings
        """
        logger.info("Starting stdout/stderr streaming with progress parsing")

        # Pattern to match dbt progress lines with timestamp prefix: "12:04:38  1 of 5 START/OK/PASS/ERROR ..."
        # Models use: START, OK, ERROR, FAIL, SKIP, WARN
        # Tests use: START, PASS, FAIL, ERROR, SKIP, WARN
        # Seeds use: START, INSERT, ERROR, SKIP
        progress_pattern = re.compile(r"^\d{2}:\d{2}:\d{2}\s+(\d+) of (\d+) (START|OK|PASS|INSERT|ERROR|FAIL|SKIP|WARN)\s+(.+)$")

        stdout_lines = []
        stderr_lines = []
        line_count = 0

        # Track overall progress across all stages
        overall_progress = 0
        total_resources = expected_total if expected_total is not None else 0
        seen_resources = set()  # Track unique resources to avoid double-counting
        running_models = []  # Track models currently running (FIFO order)
        running_start_times = {}  # Track start timestamps for elapsed time
        ok_count = 0
        error_count = 0
        skip_count = 0
        warn_count = 0

        # Report initial progress if we have expected_total
        if expected_total is not None and progress_callback:
            try:
                result = progress_callback(0, expected_total, "0/{} completed • Preparing...".format(expected_total))
                if asyncio.iscoroutine(result):
                    await result
            except Exception as e:
                logger.warning(f"Initial progress callback error: {e}")

        async def read_stdout():
            """Read and parse stdout line by line."""
            nonlocal line_count
            assert proc.stdout is not None
            logger.info("Starting stdout reader")
            try:
                while True:
                    line_bytes = await proc.stdout.readline()
                    if not line_bytes:
                        logger.info(f"Stdout EOF reached after {line_count} lines")
                        break

                    line = line_bytes.decode("utf-8", errors="replace").rstrip()
                    stdout_lines.append(line)
                    line_count += 1

                    # Log ALL lines to see the actual output format
                    logger.info(f"stdout[{line_count}]: {line}")

                    # Check for progress indicators
                    match = progress_pattern.match(line)
                    if match:
                        logger.info(f"Progress match found: {line}")
                        total = int(match.group(2))
                        status = match.group(3)
                        model_info = match.group(4).strip()

                        # Declare nonlocal variables for modification
                        nonlocal total_resources, overall_progress, ok_count, error_count, skip_count, warn_count

                        # Update total from progress lines (this is the actual count being executed)
                        if total > total_resources:
                            total_resources = total

                        # Extract model/test/seed name from info string
                        # Models: "sql table model schema.model_name ..."
                        # Tests: "test not_null_customers_customer_id ...... [RUN]"
                        # Seeds START: "seed file main.raw_customers ...... [RUN]"
                        # Seeds OK: "loaded seed file main.raw_customers ...... [INSERT 3 in 0.12s]"
                        model_name = model_info

                        # For models, extract after " model "
                        if " model " in model_info:
                            parts = model_info.split(" model ")
                            if len(parts) > 1:
                                # Get "schema.model_name" or just "model_name"
                                model_name = parts[1].split()[0] if parts[1] else model_info
                        # For seeds, extract after "seed file " or "loaded seed file "
                        elif "seed file " in model_info:
                            # Find "seed file " and extract what comes after
                            idx = model_info.find("seed file ")
                            if idx != -1:
                                # Extract from after "seed file " (10 chars)
                                rest = model_info[idx + 10 :]
                                model_name = rest.split()[0] if rest.split() else model_info
                        # For tests, handle "test " prefix
                        elif model_info.startswith("test "):
                            # Remove "test " prefix and get the name
                            model_name = model_info[5:].split()[0] if len(model_info) > 5 else model_info
                        else:
                            # For other cases, just take the first word
                            first_word = model_info.split()[0] if model_info.split() else model_info
                            model_name = first_word

                        # Clean up markers like [RUN] or [PASS] or [INSERT 3] and dots
                        import re

                        model_name = re.sub(r"\s*\.+\s*\[(RUN|PASS|FAIL|ERROR|SKIP|WARN|INSERT)\].*$", "", model_name)
                        model_name = re.sub(r"\s+\[.*$", "", model_name)  # Remove any bracketed content
                        model_name = model_name.strip()

                        # Handle START events - add to running queue
                        if status == "START":
                            if model_name not in running_models:
                                running_models.append(model_name)
                                running_start_times[model_name] = time.time()
                                logger.info(f"Model started: {model_name}")

                        # Handle completion events - remove from running queue
                        elif status in ("OK", "PASS", "INSERT", "ERROR", "FAIL", "SKIP", "WARN"):
                            # Create unique resource key to avoid double-counting
                            resource_key = f"{status}:{model_name}"

                            # Only increment overall progress for new resources
                            if resource_key not in seen_resources:
                                seen_resources.add(resource_key)
                                overall_progress += 1

                                # Track success/error/skip/warn counts
                                if status in ("OK", "PASS", "INSERT"):
                                    ok_count += 1
                                elif status in ("ERROR", "FAIL"):
                                    error_count += 1
                                elif status == "SKIP":
                                    skip_count += 1
                                elif status == "WARN":
                                    warn_count += 1

                                logger.info(f"New resource: {resource_key}, overall progress: {overall_progress}/{total_resources}")

                            # ALWAYS remove from running queue on completion (regardless of whether it's new)
                            if model_name in running_models:
                                running_models.remove(model_name)
                                running_start_times.pop(model_name, None)
                                logger.info(f"Model completed: {model_name}, status: {status}")

                        # Build progress message: "5/20 completed • 3✅ 1❌ 1⚠️ 1⏭️ • Running (2): customers (5s)"
                        # Show statuses conditionally (only when > 0)
                        summary_parts = [f"{overall_progress}/{total_resources} completed"]
                        if ok_count > 0:
                            summary_parts.append(f"{ok_count}✅")
                        if error_count > 0:
                            summary_parts.append(f"{error_count}❌")
                        if warn_count > 0:
                            summary_parts.append(f"{warn_count}⚠️")
                        if skip_count > 0:
                            summary_parts.append(f"{skip_count}⏭️")
                        summary_stats = " ".join(summary_parts)

                        # Format running list with elapsed times
                        max_display = 2
                        if len(running_models) > 0:
                            current_time = time.time()
                            running_with_times = []
                            for model in running_models[:max_display]:
                                elapsed = int(current_time - running_start_times.get(model, current_time))
                                running_with_times.append(f"{model} ({elapsed}s)")

                            if len(running_models) > max_display:
                                displayed = ", ".join(running_with_times)
                                running_str = f"Running ({len(running_models)}): {displayed} +{len(running_models) - max_display} more"
                            else:
                                running_str = f"Running ({len(running_models)}): {', '.join(running_with_times)}"

                            accumulated_message = f"{summary_stats} • {running_str}"
                        else:
                            accumulated_message = summary_stats if overall_progress > 0 else ""

                        # Call progress callback with overall progress and accumulated message (non-blocking)
                        if accumulated_message:  # Only call if we have a message
                            try:
                                result = progress_callback(overall_progress, total_resources, accumulated_message)
                                if asyncio.iscoroutine(result):
                                    await result
                            except Exception as e:
                                logger.warning(f"Progress callback error: {e}")
            except asyncio.CancelledError:
                logger.info("stdout reader cancelled")
                raise
            except Exception as e:
                logger.warning(f"stdout reader error: {e}")

        async def read_stderr():
            """Read stderr line by line."""
            assert proc.stderr is not None
            try:
                while True:
                    line_bytes = await proc.stderr.readline()
                    if not line_bytes:
                        break
                    line = line_bytes.decode("utf-8", errors="replace").rstrip()
                    stderr_lines.append(line)
            except asyncio.CancelledError:
                logger.info("stderr reader cancelled")
                raise
            except Exception as e:
                logger.warning(f"stderr reader error: {e}")

        # Run both readers concurrently with timeout
        readers_task = None
        try:
            # Create the gather task
            readers_task = asyncio.gather(read_stdout(), read_stderr(), return_exceptions=False)

            if self.timeout:
                await asyncio.wait_for(readers_task, timeout=self.timeout)
            else:
                await readers_task

        except asyncio.TimeoutError:
            logger.error(f"dbt command timed out after {self.timeout} seconds, killing process")
            # Cancel the reader tasks
            if readers_task and not readers_task.done():
                readers_task.cancel()
                try:
                    await readers_task
                except asyncio.CancelledError:
                    pass
            # Kill the process
            proc.kill()
            await proc.wait()
            raise RuntimeError(f"dbt command timed out after {self.timeout} seconds")
        except asyncio.CancelledError:
            logger.info("Stream readers cancelled")
            # Cancel the reader tasks
            if readers_task and not readers_task.done():
                readers_task.cancel()
                try:
                    await readers_task
                except asyncio.CancelledError:
                    pass
            raise
        finally:
            # Ensure process completes and readers are done
            if proc.returncode is None:
                await proc.wait()

        return "\n".join(stdout_lines), "\n".join(stderr_lines)

    async def _kill_process_tree(self, proc: asyncio.subprocess.Process) -> None:
        """Kill a process and all its children."""
        pid = proc.pid
        if pid is None:
            logger.warning("Cannot kill process: PID is None")
            return

        # Log child processes before killing
        try:
            parent = psutil.Process(pid)
            children = parent.children(recursive=True)
            if children:
                logger.info(f"Process {pid} has {len(children)} child process(es): {[p.pid for p in children]}")
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

        if platform.system() == "Windows":
            # On Windows, try graceful termination first, then force kill
            try:
                # Step 1: Try graceful termination (without /F flag)
                logger.info(f"Attempting graceful termination of process tree for PID {pid}")
                terminate_proc = await asyncio.create_subprocess_exec(
                    "taskkill",
                    "/T",  # Kill tree, but no /F (force) flag
                    "/PID",
                    str(pid),
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.DEVNULL,
                )

                # Wait for taskkill command to complete (it returns immediately)
                await terminate_proc.wait()

                # Now wait for the actual process to terminate (poll with timeout)
                start_time = asyncio.get_event_loop().time()
                timeout = 10.0
                poll_interval = 0.5

                while (asyncio.get_event_loop().time() - start_time) < timeout:
                    if not self._is_process_running(pid):
                        logger.info(f"Process {pid} terminated gracefully")
                        return
                    await asyncio.sleep(poll_interval)

                # If we get here, process didn't terminate gracefully
                logger.info(f"Process {pid} still running after {timeout}s, forcing kill...")

                # Step 2: Force kill if graceful didn't work
                logger.info(f"Force killing process tree for PID {pid}")
                kill_proc = await asyncio.create_subprocess_exec(
                    "taskkill",
                    "/F",  # Force
                    "/T",  # Kill tree
                    "/PID",
                    str(pid),
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.DEVNULL,
                )

                await asyncio.wait_for(kill_proc.wait(), timeout=5.0)

                # Verify process is dead
                await asyncio.sleep(0.3)
                try:
                    if psutil.Process(pid).is_running():
                        logger.warning(f"Process {pid} still running after force kill")
                    else:
                        logger.info(f"Successfully killed process tree for PID {pid}")
                except psutil.NoSuchProcess:
                    logger.info(f"Process {pid} terminated successfully")

            except asyncio.TimeoutError:
                logger.warning(f"Force kill timed out for PID {pid}")
            except Exception as e:
                logger.warning(f"Failed to kill process tree: {e}")
                # Last resort fallback
                try:
                    proc.kill()
                    await proc.wait()
                except Exception:
                    pass
        else:
            # On Unix, terminate then kill if needed
            try:
                proc.terminate()
                await asyncio.wait_for(proc.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()

    def _is_process_running(self, pid: int) -> bool:
        """Check if a process is still running."""
        try:
            process = psutil.Process(pid)
            return process.is_running()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return False

    def get_manifest_path(self) -> Path:
        """Get the path to the manifest.json file."""
        return self._target_dir / "manifest.json"

    async def invoke_query(self, sql: str) -> DbtRunnerResult:
        """
        Execute a SQL query using dbt show --inline.

        This method supports Jinja templating including {{ ref() }} and {{ source() }}.
        The SQL should include LIMIT clause if needed - no automatic limiting is applied.

        Args:
            sql: SQL query to execute (supports Jinja: {{ ref('model') }}, {{ source('src', 'table') }})
                 Include LIMIT in the SQL if you want to limit results.

        Returns:
            Result with query output in JSON format
        """
        # Use dbt show --inline with JSON output
        # --limit -1 disables the automatic LIMIT that dbt show adds (returns all rows)
        args = [
            "show",
            "--inline",
            sql,
            "--limit",
            "-1",
            "--output",
            "json",
        ]

        # Execute the command
        result = await self.invoke(args)

        return result

    async def invoke_compile(self, model_name: str, force: bool = False) -> DbtRunnerResult:
        """
        Compile a specific model, optionally forcing recompilation.

        Args:
            model_name: Name of the model to compile (e.g., 'customers')
            force: If True, always compile. If False, only compile if not already compiled.

        Returns:
            Result of the compilation
        """
        # If not forcing, check if already compiled
        if not force:
            manifest_path = self.get_manifest_path()
            if manifest_path.exists():
                try:
                    with open(manifest_path) as f:
                        manifest = json.load(f)

                    # Check if model has compiled_code
                    nodes = manifest.get("nodes", {})
                    for node in nodes.values():
                        if node.get("resource_type") == "model" and node.get("name") == model_name:
                            if node.get("compiled_code"):
                                logger.info(f"Model '{model_name}' already compiled, skipping compilation")
                                return DbtRunnerResult(success=True, stdout="Already compiled", stderr="")
                            break
                except Exception as e:
                    logger.warning(f"Failed to check compilation status: {e}, forcing compilation")

        # Run compile for specific model
        logger.info(f"Compiling model: {model_name}")
        args = ["compile", "-s", model_name]
        result = await self.invoke(args)

        return result

    def _build_script(self, args: list[str]) -> str:
        """
        Build inline Python script to execute dbtRunner.

        Args:
            args: dbt command arguments

        Returns:
            Python script as string
        """
        # Add --profiles-dir to args if not already present
        if "--profiles-dir" not in args:
            args = [*args, "--profiles-dir", str(self.profiles_dir)]

        # Add --log-format text to get human-readable output for progress parsing
        if "--log-format" not in args:
            args = [*args, "--log-format", "text"]

        # Convert args to JSON-safe format
        args_json = json.dumps(args)

        script = f"""
import sys
import json
import os

# Enable text output for progress tracking
os.environ['DBT_USE_COLORS'] = '0'
os.environ['DBT_PRINTER_WIDTH'] = '80'

try:
    from dbt.cli.main import dbtRunner
    
    # Execute dbtRunner with arguments
    dbt = dbtRunner()
    result = dbt.invoke({args_json})
    
    # Return success status on last line (JSON)
    output = {{"success": result.success}}
    print(json.dumps(output))
    sys.exit(0 if result.success else 1)
    
except Exception as e:
    # Ensure we always exit, even on error
    error_output = {{"success": False, "error": str(e)}}
    print(json.dumps(error_output))
    sys.exit(1)
"""
        return script
