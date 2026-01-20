"""
DBT Runner Protocol.

Defines the interface for running dbt commands, supporting both in-process
and subprocess execution.
"""

from pathlib import Path
from typing import Protocol


class DbtRunnerResult:
    """Result from a dbt command execution."""

    def __init__(self, success: bool, exception: Exception | None = None, stdout: str = "", stderr: str = "", elapsed_time: float | None = None):
        """
        Initialize a dbt runner result.

        Args:
            success: Whether the command succeeded
            exception: Exception if the command failed
            stdout: Standard output from the command
            stderr: Standard error from the command
            elapsed_time: Optional elapsed time in seconds for the command execution
        """
        self.success = success
        self.exception = exception
        self.stdout = stdout
        self.stderr = stderr
        self.elapsed_time = elapsed_time


class DbtRunner(Protocol):
    """Protocol for dbt command execution."""

    def invoke(self, args: list[str]) -> DbtRunnerResult:
        """
        Execute a dbt command.

        Args:
            args: dbt command arguments (e.g., ['parse'], ['run', '--select', 'model'])

        Returns:
            Result of the command execution
        """
        ...

    def get_manifest_path(self) -> Path:
        """
        Get the path to the manifest.json file.

        Returns:
            Path to target/manifest.json
        """
        ...

    def invoke_query(self, sql: str) -> DbtRunnerResult:
        """
        Execute a SQL query.

        Args:
            sql: SQL query to execute (include LIMIT in SQL if needed)

        Returns:
            Result with query output
        """
        ...
