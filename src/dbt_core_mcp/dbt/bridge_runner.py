"""
Bridge Runner for DBT.

Executes DBT commands in the user's Python environment via subprocess,
using an inline Python script to invoke dbtRunner.
"""

import json
import logging
import subprocess
from pathlib import Path

from .runner import DbtRunnerResult

logger = logging.getLogger(__name__)


class BridgeRunner:
    """
    Execute DBT commands in user's environment via subprocess bridge.

    This runner executes DBT using the dbtRunner API within the user's
    Python environment, avoiding version conflicts while still benefiting
    from dbtRunner's structured results.
    """

    def __init__(self, project_dir: Path, python_command: list[str]):
        """
        Initialize the bridge runner.

        Args:
            project_dir: Path to the DBT project directory
            python_command: Command to run Python in the user's environment
                          (e.g., ['uv', 'run', 'python'] or ['/path/to/venv/bin/python'])
        """
        self.project_dir = project_dir.resolve()  # Ensure absolute path
        self.python_command = python_command
        self._target_dir = self.project_dir / "target"

        # Detect profiles directory (project dir or ~/.dbt)
        self.profiles_dir = self.project_dir if (self.project_dir / "profiles.yml").exists() else Path.home() / ".dbt"
        logger.info(f"Using profiles directory: {self.profiles_dir}")

    def invoke(self, args: list[str]) -> DbtRunnerResult:
        """
        Execute a DBT command via subprocess bridge.

        Args:
            args: DBT command arguments (e.g., ['parse'], ['run', '--select', 'model'])

        Returns:
            Result of the command execution
        """
        # Build inline Python script to execute dbtRunner
        script = self._build_script(args)

        # Execute in user's environment
        full_command = [*self.python_command, "-c", script]

        logger.debug(f"Executing DBT command: {args}")
        logger.debug(f"Using Python: {self.python_command}")

        try:
            result = subprocess.run(
                full_command,
                cwd=self.project_dir,
                capture_output=True,
                text=True,
                check=False,
            )

            # Parse result from stdout
            if result.returncode == 0:
                # Extract JSON from last line (DBT output may contain logs)
                try:
                    last_line = result.stdout.strip().split("\n")[-1]
                    output = json.loads(last_line)
                    success = output.get("success", False)
                    logger.info(f"DBT command {'succeeded' if success else 'failed'}: {args}")
                    return DbtRunnerResult(success=success)
                except (json.JSONDecodeError, IndexError):
                    # If no JSON output, check return code
                    logger.warning(f"No JSON output from DBT command, using return code. stdout: {result.stdout}")
                    return DbtRunnerResult(success=True)
            else:
                error_msg = result.stderr or result.stdout
                logger.error(f"DBT command failed with code {result.returncode}")
                logger.error(f"stdout: {result.stdout}")
                logger.error(f"stderr: {result.stderr}")
                return DbtRunnerResult(
                    success=False,
                    exception=RuntimeError(f"DBT command failed: {error_msg}"),
                )

        except Exception as e:
            logger.exception(f"Error executing DBT command: {e}")
            return DbtRunnerResult(success=False, exception=e)

    def get_manifest_path(self) -> Path:
        """Get the path to the manifest.json file."""
        return self._target_dir / "manifest.json"

    def _build_script(self, args: list[str]) -> str:
        """
        Build inline Python script to execute dbtRunner.

        Args:
            args: DBT command arguments

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
from dbt.cli.main import dbtRunner

# Execute dbtRunner with arguments
try:
    dbt = dbtRunner()
    result = dbt.invoke({args_json})
    
    # Return success status
    print(json.dumps({{"success": result.success}}))
    sys.exit(0 if result.success else 1)
except Exception as e:
    print(json.dumps({{"success": False, "error": str(e)}}), file=sys.stderr)
    sys.exit(1)
"""
        return script
