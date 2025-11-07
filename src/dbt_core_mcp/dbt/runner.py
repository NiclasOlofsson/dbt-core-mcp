"""
DBT Runner Protocol.

Defines the interface for running DBT commands, supporting both in-process
and subprocess execution.
"""

from pathlib import Path
from typing import Protocol


class DbtRunnerResult:
    """Result from a DBT command execution."""

    def __init__(self, success: bool, exception: Exception | None = None):
        """
        Initialize a DBT runner result.

        Args:
            success: Whether the command succeeded
            exception: Exception if the command failed
        """
        self.success = success
        self.exception = exception


class DbtRunner(Protocol):
    """Protocol for DBT command execution."""

    def invoke(self, args: list[str]) -> DbtRunnerResult:
        """
        Execute a DBT command.

        Args:
            args: DBT command arguments (e.g., ['parse'], ['run', '--select', 'model'])

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
