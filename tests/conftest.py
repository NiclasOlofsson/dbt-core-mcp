"""
Pytest configuration and fixtures for dbt Core MCP tests.
"""

from pathlib import Path
from typing import TYPE_CHECKING, AsyncGenerator
from unittest.mock import AsyncMock, Mock

import pytest
import pytest_asyncio

if TYPE_CHECKING:
    from dbt_core_mcp.server import DbtCoreMcpServer


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


@pytest_asyncio.fixture(scope="session")
async def jaffle_shop_server() -> AsyncGenerator["DbtCoreMcpServer", None]:
    """Create a server instance with the jaffle_shop example project (shared across all tests).

    Currently unused but kept for potential future integration tests.
    """
    from pathlib import Path

    from dbt_core_mcp.dependencies import set_server_state
    from dbt_core_mcp.server import create_server

    # Use the example jaffle_shop project
    project_dir = Path(__file__).parent.parent / "examples" / "jaffle_shop"
    server = create_server(str(project_dir))

    # Initialize with a mock context (no workspace roots for tests)
    await server._ensure_initialized_with_context(None)  # pyright: ignore[reportPrivateUsage]

    # Set the server state for dependency injection in tools
    set_server_state(server.state)

    # IMPORTANT: Disable persistent process for tests to avoid asyncio event loop conflicts
    # Pytest creates new event loops per test, causing "Future attached to different loop" errors
    if server.runner:
        server.runner.use_persistent_process = False

    yield server

    # Cleanup: Stop persistent process if it was started
    if server.runner and server.runner._dbt_process:  # type: ignore[reportPrivateUsage]
        await server.runner._stop_persistent_process()  # type: ignore[reportPrivateUsage]


@pytest_asyncio.fixture
async def fixture_manifest() -> Mock:
    """Create a mock state with ManifestLoader from test fixtures.

    Uses the pre-compiled manifest.json from tests/fixtures/target/ which contains
    the jaffle_shop example project. Much faster than jaffle_shop_server since it
    skips project initialization, parsing, and compilation.

    Returns:
        Mock state object with real ManifestLoader instance
    """
    from dbt_core_mcp.context import DbtCoreServerContext
    from dbt_core_mcp.dbt.manifest import ManifestLoader

    # Load manifest from fixtures
    manifest_path = Path(__file__).parent / "fixtures" / "target" / "manifest.json"
    manifest_loader = ManifestLoader(manifest_path)
    await manifest_loader.load()

    # Create mock state with real manifest
    state = Mock(spec=DbtCoreServerContext)
    state.ensure_initialized = AsyncMock()
    state.manifest = manifest_loader

    return state
