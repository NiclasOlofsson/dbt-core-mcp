"""
DBT Runner Protocol.

Defines the interface for running dbt commands, supporting both in-process
and subprocess execution.
"""

from pathlib import Path
from typing import Protocol


class DbtRunnerResult:
    """Result from a dbt command execution."""

    def __init__(self, success: bool, exception: Exception | None = None, stdout: str = "", stderr: str = ""):
        """
        Initialize a dbt runner result.

        Args:
            success: Whether the command succeeded
            exception: Exception if the command failed
            stdout: Standard output from the command
            stderr: Standard error from the command
        """
        self.success = success
        self.exception = exception
        self.stdout = stdout
        self.stderr = stderr


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

    def invoke_query(self, sql: str, limit: int | None = None) -> DbtRunnerResult:
        """
        Execute a SQL query.

        Args:
            sql: SQL query to execute
            limit: Optional LIMIT clause to add to query (not used for non-SELECT commands)

        Returns:
            Result with query output
        """
        ...
