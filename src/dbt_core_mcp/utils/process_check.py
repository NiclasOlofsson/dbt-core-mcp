"""
Process checking utilities for detecting running DBT processes.

This module provides utilities to check if DBT is currently running
in the same project directory, helping prevent concurrent execution issues.
"""

import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def is_dbt_running(project_dir: Path) -> bool:
    """
    Check if DBT is currently running in the specified project directory.

    Args:
        project_dir: Path to the DBT project directory to check

    Returns:
        True if a DBT process is detected running in the project directory
    """
    try:
        import psutil
    except ImportError:
        # If psutil is not available, we can't check - return False (assume safe)
        logger.warning("psutil not installed - cannot check for running DBT processes")
        return False

    project_dir = project_dir.resolve()  # Normalize path
    logger.debug(f"Checking for DBT processes in: {project_dir}")

    try:
        for proc in psutil.process_iter(["pid", "name", "cmdline", "cwd"]):
            try:
                # Check if this is a DBT-related process
                cmdline = proc.info.get("cmdline") or []
                if not cmdline:
                    continue

                # Look for 'dbt' command in the command line
                # We want actual 'dbt' CLI commands, not just processes that import dbt
                cmdline_str = " ".join(cmdline).lower()

                # Skip if this is our own MCP server or Python imports
                if "dbt-core-mcp" in cmdline_str or "dbt_core_mcp" in cmdline_str:
                    continue

                # Look for actual dbt CLI usage: 'dbt run', 'dbt parse', 'python -m dbt.cli.main', etc.
                is_dbt_command = False
                for i, arg in enumerate(cmdline):
                    arg_lower = arg.lower()
                    # Check for 'dbt' as a standalone command or module
                    if arg_lower == "dbt" or arg_lower.endswith("dbt.exe") or arg_lower.endswith("dbt"):
                        is_dbt_command = True
                        break
                    # Check for 'python -m dbt.cli.main'
                    if "dbt.cli.main" in arg_lower or "dbt/cli/main" in arg_lower:
                        is_dbt_command = True
                        break

                if not is_dbt_command:
                    continue

                # Check if it's running in the same project directory
                # Compare working directory
                proc_cwd = proc.info.get("cwd")
                if proc_cwd:
                    proc_path = Path(proc_cwd).resolve()
                    if proc_path == project_dir or project_dir in proc_path.parents or proc_path in project_dir.parents:
                        logger.info(f"Found running DBT process (PID {proc.info['pid']}): {cmdline}")
                        return True

                # Also check if project directory is mentioned in command line
                project_str = str(project_dir)
                if project_str in cmdline_str or str(project_dir).replace("\\", "/") in cmdline_str:
                    logger.info(f"Found DBT process with project path (PID {proc.info['pid']}): {cmdline}")
                    return True

            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                # Process disappeared or we don't have permission - skip it
                continue

        logger.debug("No running DBT processes detected")
        return False

    except Exception as e:
        # If we can't check, assume it's safe to proceed
        logger.warning(f"Error checking for DBT processes: {e}")
        return False


def wait_for_dbt_completion(project_dir: Path, timeout: float = 10.0, poll_interval: float = 0.2) -> bool:
    """
    Wait for any running DBT processes to complete.

    Args:
        project_dir: Path to the DBT project directory
        timeout: Maximum time to wait in seconds (default: 10)
        poll_interval: How often to check in seconds (default: 0.2)

    Returns:
        True if DBT finished or was not running, False if timeout occurred
    """
    import time

    logger.debug(f"Waiting for DBT completion (timeout: {timeout}s)")

    elapsed = 0.0
    while elapsed < timeout:
        if not is_dbt_running(project_dir):
            logger.debug("DBT process check clear")
            return True

        logger.debug(f"DBT still running, waiting... ({elapsed:.1f}/{timeout}s)")
        time.sleep(poll_interval)
        elapsed += poll_interval

    logger.warning(f"Timeout waiting for DBT to complete after {timeout}s")
    return False
