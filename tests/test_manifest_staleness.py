"""
Tests for manifest staleness detection.

These tests verify that the server correctly detects when a manifest is stale
and needs regeneration, versus when it can reuse an existing manifest.
"""

import asyncio
from pathlib import Path
from unittest.mock import MagicMock, patch


async def test_manifest_not_stale_when_exists_and_fresh() -> None:
    """Test that staleness check returns False when manifest exists and is fresh."""
    from dbt_core_mcp.server import DbtCoreMcpServer

    # Use the real jaffle_shop example project
    project_dir = Path(__file__).parent.parent / "examples" / "jaffle_shop"
    server = DbtCoreMcpServer(str(project_dir))

    # Set project_dir and create a mock runner (to pass the initial check)
    server.project_dir = project_dir
    server.runner = MagicMock()  # Mock runner so we can check staleness

    # Verify manifest exists
    manifest_path = project_dir / "target" / "manifest.json"
    assert manifest_path.exists(), "Test requires existing manifest in jaffle_shop"

    # Check staleness - should be False since manifest exists and is fresh
    is_stale = server._is_manifest_stale()

    # This should be False, but currently returns True due to the bug!
    assert not is_stale, "Manifest should not be stale when it exists and is fresh"


async def test_manifest_stale_when_missing() -> None:
    """Test that staleness check returns True when manifest doesn't exist."""
    from dbt_core_mcp.server import DbtCoreMcpServer

    project_dir = Path(__file__).parent.parent / "examples" / "jaffle_shop"
    server = DbtCoreMcpServer(str(project_dir))
    server.project_dir = project_dir
    server.runner = MagicMock()

    # Mock manifest path to simulate it doesn't exist
    with patch.object(Path, "exists", return_value=False):
        is_stale = server._is_manifest_stale()
        assert is_stale, "Manifest should be stale when it doesn't exist"


async def test_manifest_stale_when_project_file_newer(tmp_path: Path) -> None:
    """Test that staleness check returns True when dbt_project.yml is newer."""
    from dbt_core_mcp.server import DbtCoreMcpServer

    # Create a minimal test project
    project_dir = tmp_path / "test_project"
    project_dir.mkdir()

    # Create dbt_project.yml
    project_file = project_dir / "dbt_project.yml"
    project_file.write_text("""
name: 'test_project'
version: '1.0.0'
config-version: 2
profile: 'test_profile'
model-paths: ["models"]
""")

    # Create target directory and manifest
    target_dir = project_dir / "target"
    target_dir.mkdir()
    manifest_path = target_dir / "manifest.json"
    manifest_path.write_text('{"metadata": {}, "nodes": {}}')

    # Wait a bit and touch the project file to make it newer
    await asyncio.sleep(0.01)
    project_file.touch()

    # Initialize server
    server = DbtCoreMcpServer(str(project_dir))
    server.project_dir = project_dir
    server.runner = MagicMock()

    # Check staleness - should be True since project file is newer
    is_stale = server._is_manifest_stale()
    assert is_stale, "Manifest should be stale when dbt_project.yml is newer"


async def test_manifest_stale_when_model_file_newer(tmp_path: Path) -> None:
    """Test that staleness check returns True when a model file is newer."""
    from dbt_core_mcp.server import DbtCoreMcpServer

    # Create a minimal test project
    project_dir = tmp_path / "test_project"
    project_dir.mkdir()

    # Create dbt_project.yml
    project_file = project_dir / "dbt_project.yml"
    project_file.write_text("""
name: 'test_project'
version: '1.0.0'
config-version: 2
profile: 'test_profile'
model-paths: ["models"]
""")

    # Create models directory and a model file
    models_dir = project_dir / "models"
    models_dir.mkdir()
    model_file = models_dir / "test_model.sql"
    model_file.write_text("SELECT 1 as id")

    # Create target directory and manifest
    target_dir = project_dir / "target"
    target_dir.mkdir()
    manifest_path = target_dir / "manifest.json"
    manifest_path.write_text('{"metadata": {}, "nodes": {}}')

    # Wait a bit and touch the model file to make it newer
    await asyncio.sleep(0.01)
    model_file.touch()

    # Initialize server
    server = DbtCoreMcpServer(str(project_dir))
    server.project_dir = project_dir
    server.runner = MagicMock()

    # Check staleness - should be True since model file is newer
    is_stale = server._is_manifest_stale()
    assert is_stale, "Manifest should be stale when model file is newer"


async def test_staleness_check_before_runner_initialized() -> None:
    """Test that _ensure_initialized_with_context doesn't parse when manifest is fresh.

    This is the key test that demonstrates the bug: currently, the first initialization
    always parses even if a fresh manifest exists, because _is_manifest_stale() checks
    'if not self.runner' before checking if the manifest exists.
    """
    from dbt_core_mcp.server import create_server

    project_dir = Path(__file__).parent.parent / "examples" / "jaffle_shop"

    # Verify manifest exists and is fresh
    manifest_path = project_dir / "target" / "manifest.json"
    assert manifest_path.exists(), "Test requires existing manifest"

    # Get manifest timestamp before initialization
    manifest_mtime_before = manifest_path.stat().st_mtime

    # Create server and initialize
    server = create_server(str(project_dir))

    # Mock the runner's invoke method to track if parse is called
    parse_called = False
    original_invoke = None

    async def track_invoke(args: list[str]):
        nonlocal parse_called
        if args == ["parse"] or (len(args) > 0 and args[0] == "parse"):
            parse_called = True
        # Call original if it exists
        if original_invoke:
            return await original_invoke(args)
        from dbt_core_mcp.dbt.runner import DbtRunnerResult

        return DbtRunnerResult(success=True)

    # Patch the BridgeRunner.invoke method before initialization
    with patch("dbt_core_mcp.dbt.bridge_runner.BridgeRunner.invoke", new=track_invoke):
        await server._ensure_initialized_with_context(None)

    # Get manifest timestamp after initialization
    manifest_mtime_after = manifest_path.stat().st_mtime

    # The manifest should not have been regenerated (timestamps should match)
    assert manifest_mtime_before == manifest_mtime_after, "Manifest should not be regenerated when it's fresh"

    # Parse should not have been called
    # NOTE: This assertion will FAIL with current code due to the bug
    assert not parse_called, "dbt parse should not be called when manifest is fresh"


async def test_staleness_check_independent_of_runner_state() -> None:
    """Verify that _is_manifest_stale() checks timestamps regardless of runner state.

    After the fix, _is_manifest_stale() should check manifest existence and timestamps
    even when runner is None, ensuring we don't unnecessarily parse when a fresh
    manifest already exists.
    """
    from dbt_core_mcp.server import DbtCoreMcpServer

    project_dir = Path(__file__).parent.parent / "examples" / "jaffle_shop"

    # Verify manifest exists
    manifest_path = project_dir / "target" / "manifest.json"
    assert manifest_path.exists(), "Test requires existing manifest in jaffle_shop"

    # Create server - runner will be None
    server = DbtCoreMcpServer(str(project_dir))
    server.project_dir = project_dir
    assert server.runner is None, "Runner should be None before initialization"

    # After fix: this should check timestamps even when runner is None
    is_stale = server._is_manifest_stale()

    # Should return False because manifest exists and is fresh
    assert is_stale is False, "Should return False when manifest is fresh, regardless of runner state"


async def test_integration_parse_triggered_when_file_changes(tmp_path: Path) -> None:
    """Integration test: verify parse is triggered when source files change."""
    from unittest.mock import AsyncMock

    from dbt_core_mcp.dbt.runner import DbtRunnerResult
    from dbt_core_mcp.server import DbtCoreMcpServer

    # Create a minimal test project
    project_dir = tmp_path / "test_project"
    project_dir.mkdir()

    # Create dbt_project.yml
    project_file = project_dir / "dbt_project.yml"
    project_file.write_text("""
name: 'test_project'
version: '1.0.0'
config-version: 2
profile: 'test_profile'
model-paths: ["models"]
""")

    # Create profiles.yml
    profiles_file = project_dir / "profiles.yml"
    profiles_file.write_text("""
test_profile:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: test.duckdb
""")

    # Create models directory and a model file
    models_dir = project_dir / "models"
    models_dir.mkdir()
    model_file = models_dir / "test_model.sql"
    model_file.write_text("SELECT 1 as id")

    # Create target directory and manifest
    target_dir = project_dir / "target"
    target_dir.mkdir()
    manifest_path = target_dir / "manifest.json"
    manifest_path.write_text('{"metadata": {}, "nodes": {}, "sources": {}}')

    # Create server
    server = DbtCoreMcpServer(str(project_dir))

    # Track parse calls
    parse_count = 0

    async def mock_invoke(args: list[str]):
        nonlocal parse_count
        if args and args[0] == "parse":
            parse_count += 1
        return DbtRunnerResult(success=True)

    # First initialization - manifest is fresh, should NOT parse
    with patch("dbt_core_mcp.dbt.bridge_runner.BridgeRunner.invoke", new_callable=AsyncMock) as mock:
        mock.side_effect = mock_invoke
        await server._ensure_initialized_with_context(None)
        assert parse_count == 0, "Should not parse when manifest is fresh"

    # Now touch the model file to make it newer than manifest
    await asyncio.sleep(0.01)
    model_file.touch()

    # Reset server state to simulate a new session
    server.runner = None
    server.manifest = None

    # Second initialization - file changed, SHOULD parse
    with patch("dbt_core_mcp.dbt.bridge_runner.BridgeRunner.invoke", new_callable=AsyncMock) as mock:
        mock.side_effect = mock_invoke
        await server._ensure_initialized_with_context(None)
        assert parse_count == 1, "Should parse when model file is newer than manifest"


async def test_integration_parse_skipped_on_subsequent_calls(tmp_path: Path) -> None:
    """Integration test: verify parse is skipped on subsequent calls when nothing changed."""
    from unittest.mock import AsyncMock

    from dbt_core_mcp.dbt.runner import DbtRunnerResult
    from dbt_core_mcp.server import DbtCoreMcpServer

    # Create a minimal test project
    project_dir = tmp_path / "test_project"
    project_dir.mkdir()

    # Create dbt_project.yml
    project_file = project_dir / "dbt_project.yml"
    project_file.write_text("""
name: 'test_project'
version: '1.0.0'
config-version: 2
profile: 'test_profile'
model-paths: ["models"]
""")

    # Create profiles.yml
    profiles_file = project_dir / "profiles.yml"
    profiles_file.write_text("""
test_profile:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: test.duckdb
""")

    # Create models directory
    models_dir = project_dir / "models"
    models_dir.mkdir()

    # Create target directory and manifest
    target_dir = project_dir / "target"
    target_dir.mkdir()
    manifest_path = target_dir / "manifest.json"
    manifest_path.write_text('{"metadata": {}, "nodes": {}, "sources": {}}')

    # Create server
    server = DbtCoreMcpServer(str(project_dir))

    # Track parse calls
    parse_count = 0

    async def mock_invoke(args: list[str]):
        nonlocal parse_count
        if args and args[0] == "parse":
            parse_count += 1
        return DbtRunnerResult(success=True)

    # First initialization - manifest is fresh, should NOT parse
    with patch("dbt_core_mcp.dbt.bridge_runner.BridgeRunner.invoke", new_callable=AsyncMock) as mock:
        mock.side_effect = mock_invoke
        await server._ensure_initialized_with_context(None)
        initial_count = parse_count

    # Second call without any changes - should NOT parse again
    with patch("dbt_core_mcp.dbt.bridge_runner.BridgeRunner.invoke", new_callable=AsyncMock) as mock:
        mock.side_effect = mock_invoke
        await server._ensure_initialized_with_context(None)
        assert parse_count == initial_count, "Should not parse again when nothing changed"
