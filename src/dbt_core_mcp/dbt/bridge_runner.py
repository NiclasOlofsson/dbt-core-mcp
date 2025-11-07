"""
Bridge Runner for DBT.

Executes DBT commands in the user's Python environment via subprocess,
using an inline Python script to invoke dbtRunner.
"""

import json
import logging
import subprocess
from pathlib import Path

from ..utils.process_check import is_dbt_running, wait_for_dbt_completion
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
        self._project_config = None  # Lazy-loaded project configuration
        self._project_config_mtime = None  # Track last modification time

        # Detect profiles directory (project dir or ~/.dbt)
        self.profiles_dir = self.project_dir if (self.project_dir / "profiles.yml").exists() else Path.home() / ".dbt"
        logger.info(f"Using profiles directory: {self.profiles_dir}")

    def _get_project_config(self) -> dict:
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
                        self._project_config = yaml.safe_load(f) or {}
                    self._project_config_mtime = current_mtime
                except Exception as e:
                    logger.warning(f"Failed to parse dbt_project.yml: {e}")
                    self._project_config = {}
                    self._project_config_mtime = None
        else:
            self._project_config = {}
            self._project_config_mtime = None

        return self._project_config

    def invoke(self, args: list[str]) -> DbtRunnerResult:
        """
        Execute a DBT command via subprocess bridge.

        Args:
            args: DBT command arguments (e.g., ['parse'], ['run', '--select', 'model'])

        Returns:
            Result of the command execution
        """
        # Check if DBT is already running and wait for completion
        if is_dbt_running(self.project_dir):
            logger.info("DBT process detected, waiting for completion...")
            if not wait_for_dbt_completion(self.project_dir, timeout=10.0, poll_interval=0.2):
                logger.error("Timeout waiting for DBT process to complete")
                return DbtRunnerResult(
                    success=False,
                    exception=RuntimeError("DBT is already running in this project. Please wait for it to complete."),
                )

        # Build inline Python script to execute dbtRunner
        script = self._build_script(args)

        # Execute in user's environment
        full_command = [*self.python_command, "-c", script]

        logger.info(f"Executing DBT command: {args}")
        logger.info(f"Using Python: {self.python_command}")
        logger.info(f"Working directory: {self.project_dir}")

        try:
            logger.info("Starting subprocess...")
            result = subprocess.run(
                full_command,
                cwd=self.project_dir,
                capture_output=True,
                text=True,
                check=False,
                timeout=60.0,  # 60 second timeout to prevent indefinite hangs
                stdin=subprocess.DEVNULL,  # Ensure subprocess doesn't wait for input
            )
            logger.info(f"Subprocess completed with return code: {result.returncode}")

            # Parse result from stdout
            if result.returncode == 0:
                # Extract JSON from last line (DBT output may contain logs)
                try:
                    last_line = result.stdout.strip().split("\n")[-1]
                    output = json.loads(last_line)
                    success = output.get("success", False)
                    logger.info(f"DBT command {'succeeded' if success else 'failed'}: {args}")
                    return DbtRunnerResult(success=success, stdout=result.stdout, stderr=result.stderr)
                except (json.JSONDecodeError, IndexError) as e:
                    # If no JSON output, check return code
                    logger.warning(f"No JSON output from DBT command: {e}. stdout: {result.stdout[:200]}")
                    return DbtRunnerResult(success=True, stdout=result.stdout, stderr=result.stderr)
            else:
                # Non-zero return code indicates failure
                error_msg = result.stderr.strip() if result.stderr else result.stdout.strip()
                logger.error(f"DBT command failed with code {result.returncode}")
                logger.debug(f"stdout: {result.stdout[:500]}")
                logger.debug(f"stderr: {result.stderr[:500]}")

                # Try to extract meaningful error from stderr or stdout
                if not error_msg and result.stdout:
                    error_msg = result.stdout.strip()

                return DbtRunnerResult(
                    success=False,
                    exception=RuntimeError(f"DBT command failed (exit code {result.returncode}): {error_msg[:500]}"),
                )

        except subprocess.TimeoutExpired:
            logger.error(f"DBT command timed out after 60 seconds: {args}")
            return DbtRunnerResult(success=False, exception=RuntimeError("DBT command timed out after 60 seconds"))
        except Exception as e:
            logger.exception(f"Error executing DBT command: {e}")
            return DbtRunnerResult(success=False, exception=e)

    def get_manifest_path(self) -> Path:
        """Get the path to the manifest.json file."""
        return self._target_dir / "manifest.json"

    def invoke_query(self, sql: str, limit: int | None = None) -> DbtRunnerResult:
        """
        Execute a SQL query using dbt run-operation with __mcp_execute_sql macro.

        Args:
            sql: SQL query to execute
            limit: Optional LIMIT clause to add to query (not used for non-SELECT commands)

        Returns:
            Result with query output
        """
        # Get macro paths from dbt_project.yml
        config = self._get_project_config()
        macro_paths = config.get("macro-paths", ["macros"])
        macro_dir = macro_paths[0] if macro_paths else "macros"

        # Ensure the __mcp_execute_sql macro exists and is up to date
        macro_path = self.project_dir / macro_dir / "__mcp_execute_sql.sql"
        current_version = "1.0.1"
        macro_content = f"""{{#
  This macro is auto-generated by the DBT Core MCP Server.
  Version: {current_version}
  
  Purpose: Execute arbitrary SQL queries without automatic LIMIT clauses.
  Used by: The query_database MCP tool via dbt run-operation.
  
  You can safely delete this file if you're not using the MCP server,
  or you can use it directly with: dbt run-operation __mcp_execute_sql --args '{{sql: "..."}}'
#}}

{{% macro __mcp_execute_sql(sql) %}}
    {{% set results = run_query(sql) %}}
    {{% if execute %}}
        {{{{ log('__MCP_QUERY_RESULTS_START__', info=true) }}}}
        {{{{ results.print_json() }}}}
        {{{{ log('__MCP_QUERY_RESULTS_END__', info=true) }}}}
    {{% endif %}}
{{% endmacro %}}
"""

        should_update = False
        if not macro_path.exists():
            logger.info(f"Macro not found at {macro_path}, creating it...")
            should_update = True
        else:
            # Check version in existing macro
            existing_content = macro_path.read_text()
            import re

            version_match = re.search(r"Version:\s*(\S+)", existing_content)
            if version_match:
                existing_version = version_match.group(1)
                if existing_version != current_version:
                    logger.info(f"Updating macro from version {existing_version} to {current_version}")
                    should_update = True
            else:
                # No version found, assume old version
                logger.info("Macro has no version, updating to latest")
                should_update = True

        if should_update:
            macro_path.parent.mkdir(parents=True, exist_ok=True)
            macro_path.write_text(macro_content)

        # Apply limit if specified (only for SELECT queries)
        final_sql = sql
        if limit is not None and sql.strip().upper().startswith("SELECT"):
            final_sql = f"{sql.rstrip(';')} LIMIT {limit}"

        # Escape the SQL for JSON args
        sql_escaped = json.dumps(final_sql)

        # Use dbt run-operation with the __mcp_execute_sql macro
        args = ["run-operation", "__mcp_execute_sql", "--args", f"{{sql: {sql_escaped}}}"]

        # Execute the macro
        result = self.invoke(args)

        return result

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
