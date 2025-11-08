"""
Pytest configuration and fixtures for dbt Core MCP tests.
"""

from pathlib import Path

import pytest


@pytest.fixture
def sample_project_dir(tmp_path: Path) -> str:
    """Create a temporary dbt project directory for testing."""
    project_dir = tmp_path / "test_project"
    project_dir.mkdir()

    # Create a minimal dbt_project.yml
    dbt_project_yml = project_dir / "dbt_project.yml"
    dbt_project_yml.write_text("""
name: 'test_project'
version: '1.0.0'
config-version: 2

profile: 'test_profile'

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

target-path: "target"
clean-targets:
  - "target"
  - "dbt_packages"
""")

    return str(project_dir)


@pytest.fixture
def sample_profiles_dir(tmp_path: Path) -> str:
    """Create a temporary DBT profiles directory for testing."""
    profiles_dir = tmp_path / "profiles"
    profiles_dir.mkdir()

    # Create a minimal profiles.yml
    profiles_yml = profiles_dir / "profiles.yml"
    profiles_yml.write_text("""
test_profile:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5432
      user: test_user
      pass: test_pass
      dbname: test_db
      schema: test_schema
      threads: 1
""")

    return str(profiles_dir)


@pytest.fixture
def jaffle_shop_server():
    """Create a server instance with the jaffle_shop example project."""
    from pathlib import Path

    from dbt_core_mcp.server import create_server

    # Use the example jaffle_shop project
    project_dir = Path(__file__).parent.parent / "examples" / "jaffle_shop"
    server = create_server(str(project_dir))
    server._ensure_initialized()  # pyright: ignore[reportPrivateUsage]
    return server
