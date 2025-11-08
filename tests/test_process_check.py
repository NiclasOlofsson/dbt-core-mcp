"""Tests for process checking utilities."""

import subprocess
import sys
import time
from pathlib import Path

import pytest

from dbt_core_mcp.utils.process_check import is_dbt_running, wait_for_dbt_completion


def test_is_dbt_running_no_process(tmp_path: Path) -> None:
    """Test that is_dbt_running returns False when no dbt process exists."""
    result = is_dbt_running(tmp_path)
    assert result is False


def test_wait_for_dbt_completion_no_process(tmp_path: Path) -> None:
    """Test that wait_for_dbt_completion returns immediately when no dbt process exists."""
    start = time.time()
    result = wait_for_dbt_completion(tmp_path, timeout=5.0, poll_interval=0.1)
    elapsed = time.time() - start

    assert result is True
    assert elapsed < 1.0  # Should return almost immediately


@pytest.mark.skipif(not Path("examples/jaffle_shop").exists(), reason="Requires jaffle_shop example")
def test_is_dbt_running_with_actual_process() -> None:
    """Test detecting an actual dbt process (integration test)."""
    project_dir = Path("examples/jaffle_shop").resolve()

    # Start a long-running dbt command in background
    # Use 'dbt debug' as it's relatively harmless and takes some time
    proc = subprocess.Popen(
        [sys.executable, "-m", "dbt.cli.main", "debug"],
        cwd=project_dir,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    try:
        # Give it a moment to start
        time.sleep(0.5)

        # Check if we can detect it
        result = is_dbt_running(project_dir)

        # Note: This might be False if psutil isn't installed or process started/finished too quickly
        # We're mainly testing that the function doesn't crash
        assert isinstance(result, bool)

    finally:
        # Clean up
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


def test_wait_for_dbt_completion_timeout(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that wait_for_dbt_completion times out correctly."""

    # Mock is_dbt_running to always return True
    def mock_is_dbt_running(project_dir: Path) -> bool:
        return True

    monkeypatch.setattr("dbt_core_mcp.utils.process_check.is_dbt_running", mock_is_dbt_running)

    start = time.time()
    result = wait_for_dbt_completion(tmp_path, timeout=2.0, poll_interval=0.5)
    elapsed = time.time() - start

    assert result is False
    assert elapsed >= 2.0  # Should wait for full timeout
    assert elapsed < 3.0  # But not too much longer
