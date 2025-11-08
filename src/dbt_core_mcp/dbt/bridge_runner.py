"""
Bridge Runner for dbt.

Executes dbt commands in the user's Python environment via subprocess,
using an inline Python script to invoke dbtRunner.
"""

import asyncio
import json
import logging
import platform
from pathlib import Path
from typing import Any

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

    async def invoke(self, args: list[str]) -> DbtRunnerResult:
        """
        Execute a dbt command via subprocess bridge.

        Args:
            args: dbt command arguments (e.g., ['parse'], ['run', '--select', 'model'])

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

            # Wait for completion with timeout
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
            return DbtRunnerResult(success=False, exception=e)

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

    async def invoke_query(self, sql: str, limit: int | None = None) -> DbtRunnerResult:
        """
        Execute a SQL query using dbt show --inline.

        This method supports Jinja templating including {{ ref() }} and {{ source() }}.

        Args:
            sql: SQL query to execute (supports Jinja: {{ ref('model') }}, {{ source('src', 'table') }})
            limit: Optional row limit. If None, returns all rows (--limit -1). If specified, limits results.

        Returns:
            Result with query output in JSON format
        """
        # Convert limit: None -> -1 (no limit), otherwise use the specified value
        limit_arg = -1 if limit is None else limit

        # Use dbt show --inline with JSON output
        # --limit -1 disables the automatic LIMIT that dbt show adds
        args = [
            "show",
            "--inline",
            sql,
            "--limit",
            str(limit_arg),
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

        # Convert args to JSON-safe format
        args_json = json.dumps(args)

        script = f"""
import sys
import json
import os

# Disable interactive prompts
os.environ['DBT_USE_COLORS'] = '0'
os.environ['DBT_PRINTER_WIDTH'] = '80'

try:
    from dbt.cli.main import dbtRunner
    
    # Execute dbtRunner with arguments
    dbt = dbtRunner()
    result = dbt.invoke({args_json})
    
    # Return success status
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
