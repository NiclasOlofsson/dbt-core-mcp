"""
Environment detection for dbt projects.

Detects the Python environment setup and returns the appropriate command
and environment variables to run Python in that environment.
"""

import logging
import os
import sys
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


def detect_python_command(project_dir: Path) -> list[str]:
    """
    Detect how to run Python in the project's environment.

    Detects common Python environment setups and returns the command prefix
    needed to run Python in that environment.

    Args:
        project_dir: Path to the dbt project directory

    Returns:
        Command prefix to run Python (e.g., ['uv', 'run', 'python'])

    Examples:
        >>> detect_python_command(Path("/project/with/uv"))
        ['uv', 'run', 'python']
        >>> detect_python_command(Path("/project/with/poetry"))
        ['poetry', 'run', 'python']
    """
    # Convert to absolute path
    project_dir = project_dir.resolve()

    # Check for standard venv (.venv or venv) - prefer this for uv projects to avoid VIRTUAL_ENV conflicts
    venv_path = _find_venv(project_dir)
    if venv_path:
        logger.info(f"Detected venv at {venv_path}")
        python_exe = _get_venv_python(venv_path)
        return [str(python_exe)]

    # Check for uv (uv.lock) - only if no venv found
    if (project_dir / "uv.lock").exists():
        logger.info(f"Detected uv environment in {project_dir}")
        return ["uv", "run", "--directory", str(project_dir), "python"]

    # Check for poetry (poetry.lock)
    if (project_dir / "poetry.lock").exists():
        logger.info(f"Detected poetry environment in {project_dir}")
        return ["poetry", "run", "--directory", str(project_dir), "python"]

    # Check for pipenv (Pipfile.lock)
    if (project_dir / "Pipfile.lock").exists():
        logger.info(f"Detected pipenv environment in {project_dir}")
        # pipenv doesn't have --directory, need to cd first
        return ["pipenv", "run", "python"]

    # Check for conda environment
    conda_env = os.environ.get("CONDA_DEFAULT_ENV")
    if conda_env:
        logger.info(f"Detected conda environment: {conda_env}")
        return ["conda", "run", "-n", conda_env, "python"]

    # Fall back to system Python
    logger.warning(f"No virtual environment detected in {project_dir}, using system Python")
    return [sys.executable]


def get_env_vars(python_command: list[str]) -> dict[str, str] | None:
    """
    Get environment variables needed for the given Python command.

    This centralizes environment-specific configuration that's needed
    to properly run commands in different virtual environment managers.

    Args:
        python_command: The Python command prefix (e.g., ['pipenv', 'run', 'python'])

    Returns:
        Dictionary of environment variables to set, or None if no special env needed

    Examples:
        >>> get_env_vars(['pipenv', 'run', 'python'])
        {'PIPENV_IGNORE_VIRTUALENVS': '1', 'PIPENV_VERBOSITY': '-1'}
        >>> get_env_vars(['python'])
        None
    """
    if not python_command:
        return None

    env_tool = python_command[0]

    if env_tool == "pipenv":
        # Pipenv needs to ignore outer virtualenvs when running inside another env (e.g., uv run)
        # This prevents pipenv from using the wrong environment
        return {
            "PIPENV_IGNORE_VIRTUALENVS": "1",
            "PIPENV_VERBOSITY": "-1",
        }

    # Add more env-specific settings here as needed
    # elif env_tool == "poetry":
    #     return {"POETRY_VIRTUALENVS_IN_PROJECT": "true"}

    return None


def _find_venv(project_dir: Path) -> Optional[Path]:
    """Find a virtual environment directory."""
    for venv_name in [".venv", "venv", "env"]:
        venv_path = project_dir / venv_name
        if venv_path.is_dir() and _is_venv(venv_path):
            return venv_path
    return None


def _is_venv(path: Path) -> bool:
    """Check if a directory is a valid virtual environment."""
    # Check for pyvenv.cfg (created by venv module)
    if (path / "pyvenv.cfg").exists():
        return True

    # Check for Scripts/python.exe (Windows) or bin/python (Unix)
    if sys.platform == "win32":
        return (path / "Scripts" / "python.exe").exists()
    else:
        return (path / "bin" / "python").exists()


def _get_venv_python(venv_path: Path) -> Path:
    """Get the Python executable path from a venv."""
    if sys.platform == "win32":
        return venv_path / "Scripts" / "python.exe"
    else:
        return venv_path / "bin" / "python"


def detect_dbt_adapter(project_dir: Path) -> str:
    """
    Detect the dbt adapter type from profiles.yml.

    Args:
        project_dir: Path to the dbt project directory

    Returns:
        Adapter type (e.g., 'duckdb', 'postgres', 'snowflake')

    Raises:
        FileNotFoundError: If dbt_project.yml or profiles.yml not found
        KeyError: If required keys not found in YAML files
    """
    import yaml

    # Read dbt_project.yml to get profile name
    project_yml_path = project_dir / "dbt_project.yml"
    if not project_yml_path.exists():
        raise FileNotFoundError(f"dbt_project.yml not found in {project_dir}")

    project_yml = yaml.safe_load(project_yml_path.read_text())
    profile_name = project_yml["profile"]

    # Find profiles.yml (project dir or ~/.dbt/)
    profiles_path = project_dir / "profiles.yml"
    if not profiles_path.exists():
        profiles_path = Path.home() / ".dbt" / "profiles.yml"

    if not profiles_path.exists():
        raise FileNotFoundError(f"profiles.yml not found in {project_dir} or ~/.dbt/")

    # Read profiles.yml and get adapter type
    profiles = yaml.safe_load(profiles_path.read_text())
    profile = profiles[profile_name]

    # Get target (default or first output)
    target_name = profile.get("target")
    if target_name is None:
        target_name = list(profile["outputs"].keys())[0]

    adapter_type = profile["outputs"][target_name]["type"]

    logger.info(f"Detected dbt adapter: {adapter_type}")
    return str(adapter_type)
