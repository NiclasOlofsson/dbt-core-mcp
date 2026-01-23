#!/usr/bin/env python3
"""
Main entry point for the dbt Core MCP Server.

This script provides the command-line interface to run the MCP server
for interacting with dbt projects.
"""

import argparse
import asyncio
import contextlib
import logging
import os
import signal
import sys
from pathlib import Path
from typing import Any

from .server import create_server

# Platform-specific imports for process management
if sys.platform == "win32":
    import ctypes

# Unix-specific imports
if sys.platform != "win32":
    import ctypes.util


def setup_logging(debug: bool = False) -> None:
    """Set up logging configuration."""
    import tempfile

    level = logging.DEBUG if debug else logging.INFO

    # Simpler format for stderr (VS Code adds timestamps)
    stderr_formatter = logging.Formatter("%(name)s - %(levelname)s - %(message)s")
    # Full format for file logging
    file_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setFormatter(stderr_formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.addHandler(stderr_handler)

    # Suppress FastMCP's internal INFO logs unless debug is enabled
    fastmcp_level = logging.DEBUG if debug else logging.WARNING
    logging.getLogger("fastmcp").setLevel(fastmcp_level)
    logging.getLogger("fakeredis").setLevel(logging.WARNING)
    logging.getLogger("docket").setLevel(logging.WARNING)

    # Add file logging
    try:
        temp_log_dir = os.path.join(tempfile.gettempdir(), "dbt_core_mcp_logs")
        os.makedirs(temp_log_dir, exist_ok=True)
        log_path = os.path.join(temp_log_dir, "dbt_core_mcp.log")

        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setFormatter(file_formatter)
        root_logger.addHandler(file_handler)

        print(f"[dbt Core MCP] Log file: {log_path}", file=sys.stderr)
    except Exception:
        pass


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments."""
    from . import __version__

    parser = argparse.ArgumentParser(
        description="dbt Core MCP Server - Interact with dbt projects via MCP",
        prog="dbt-core-mcp",
    )

    parser.add_argument("--debug", action="store_true", help="Enable debug logging")

    parser.add_argument(
        "--project-dir",
        type=str,
        help="Optional: Path to dbt project directory (auto-detects from workspace if not provided)",
    )

    parser.add_argument(
        "--dbt-command-timeout",
        type=float,
        default=None,
        help="Timeout in seconds for dbt commands (default: None for no timeout; 0 or negative values also mean no timeout)",
    )

    parser.add_argument(
        "--reload",
        action="store_true",
        help="Enable auto-reload on file changes (development mode)",
    )

    parser.add_argument(
        "--reload-dir",
        type=str,
        action="append",
        help="Directories to watch for changes (default: src/dbt_core_mcp)",
    )

    parser.add_argument(
        "--stateless",
        action="store_true",
        help="Enable stateless mode (automatically enabled with --reload)",
    )

    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")

    return parser.parse_args()


def _source_file_filter(change: object, path: str) -> bool:
    """Filter for source files (Python and HTML)."""
    # Test reload - iteration 6 (checking loop entry)
    return path.endswith(".py") or path.endswith(".html")


def _setup_windows_job_object() -> Any:
    """Create a Windows job object that kills all processes when closed.

    Returns:
        Job handle (or None on error)
    """
    if sys.platform != "win32":
        return None

    try:
        # Windows API constants
        JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x2000
        JOB_OBJECT_EXTENDED_LIMIT_INFORMATION = 9

        # Create job object
        job = ctypes.windll.kernel32.CreateJobObjectW(None, None)
        if not job:
            logging.warning("Failed to create Windows job object")
            return None

        # Use a simple byte buffer approach for the job info structure
        # JOBOBJECT_EXTENDED_LIMIT_INFORMATION is 144 bytes on 64-bit Windows
        job_info = ctypes.create_string_buffer(144)

        # LimitFlags is at offset 16 in JOBOBJECT_BASIC_LIMIT_INFORMATION
        # which is the first member of JOBOBJECT_EXTENDED_LIMIT_INFORMATION
        limit_flags_offset = 16
        ctypes.c_ulong.from_buffer(job_info, limit_flags_offset).value = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE

        result = ctypes.windll.kernel32.SetInformationJobObject(
            job,
            JOB_OBJECT_EXTENDED_LIMIT_INFORMATION,
            ctypes.byref(job_info),
            len(job_info),
        )

        if not result:
            error_code = ctypes.windll.kernel32.GetLastError()
            logging.warning(f"Failed to set job object information (error code: {error_code})")
            ctypes.windll.kernel32.CloseHandle(job)
            return None

        logging.debug("Created Windows job object for automatic child process cleanup")
        return job
    except Exception as e:
        logging.warning(f"Error setting up Windows job object: {e}")
        return None


def _add_process_to_job(job: Any, process_handle: int) -> bool:
    """Add a process to a Windows job object.

    Args:
        job: Job object handle
        process_handle: Process handle

    Returns:
        True if successful
    """
    if not job or sys.platform != "win32":
        return False

    try:
        result = ctypes.windll.kernel32.AssignProcessToJobObject(job, process_handle)
        if result:
            logging.debug("Added process to job object")
            return True
        else:
            logging.warning("Failed to add process to job object")
            return False
    except Exception as e:
        logging.warning(f"Error adding process to job: {e}")
        return False


def _set_pdeathsig_preexec() -> None:
    """Preexec function for Unix to set parent death signal.

    This function is called in the child process before exec.
    It sets the process to receive SIGKILL when the parent dies.
    """
    if sys.platform == "win32":
        return

    try:
        # Load libc
        libc_name = ctypes.util.find_library("c")
        if not libc_name:
            return

        libc = ctypes.CDLL(libc_name)

        # PR_SET_PDEATHSIG = 1, SIGKILL = 9
        PR_SET_PDEATHSIG = 1
        SIGKILL = 9

        # Set parent death signal
        libc.prctl(PR_SET_PDEATHSIG, SIGKILL)
    except Exception:
        # Silently fail - this is a best-effort cleanup mechanism
        pass


async def _terminate_process(process: asyncio.subprocess.Process) -> None:
    """Terminate a subprocess gracefully."""
    try:
        process.terminate()
        await asyncio.wait_for(process.wait(), timeout=5.0)
    except asyncio.TimeoutError:
        process.kill()
        await process.wait()


async def run_with_reload(
    cmd: list[str],
    reload_dirs: list[Path] | None = None,
) -> None:
    """Run a command with file watching and auto-reload.

    Args:
        cmd: Command to run as subprocess
        reload_dirs: Directories to watch for changes (default: src/dbt_core_mcp)
    """
    from watchfiles import awatch

    # Default to watching src/dbt_core_mcp directory
    if reload_dirs is None:
        src_dir = Path(__file__).parent
        watch_paths = [src_dir]
    else:
        watch_paths = reload_dirs

    process: asyncio.subprocess.Process | None = None
    first_run = True

    # Create Windows job object for automatic child process cleanup
    job = _setup_windows_job_object() if sys.platform == "win32" else None

    logging.info("Reload mode enabled - watching for file changes...")
    for watch_path in watch_paths:
        logging.info(f"  Watching: {watch_path}")

    # Handle SIGTERM/SIGINT gracefully with proper asyncio integration
    shutdown_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def signal_handler() -> None:
        logging.info("Received shutdown signal, stopping...")
        shutdown_event.set()

    # Windows doesn't support add_signal_handler
    if sys.platform != "win32":
        loop.add_signal_handler(signal.SIGTERM, signal_handler)
        loop.add_signal_handler(signal.SIGINT, signal_handler)

    try:
        while not shutdown_event.is_set():
            # Start the subprocess with platform-specific cleanup mechanisms
            if sys.platform == "win32":
                # On Windows, start process and add to job object
                process = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdin=None,
                    stdout=None,
                    stderr=None,
                )
                # Add to job object for automatic cleanup
                if job and process.pid:
                    # Get process handle (Windows-specific)
                    PROCESS_ALL_ACCESS = 0x1F0FFF
                    process_handle = ctypes.windll.kernel32.OpenProcess(PROCESS_ALL_ACCESS, False, process.pid)
                    if process_handle:
                        _add_process_to_job(job, process_handle)
                        ctypes.windll.kernel32.CloseHandle(process_handle)
                    else:
                        logging.warning(f"Failed to open process handle for PID {process.pid}")
            else:
                # On Unix, use prctl to set parent death signal
                process = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdin=None,
                    stdout=None,
                    stderr=None,
                    preexec_fn=_set_pdeathsig_preexec,
                )

            if first_run:
                logging.info("Server started - watching for changes...")
                first_run = False

            # Watch for either: file changes OR process death
            async def watch_for_changes() -> set[Any]:
                return await anext(aiter(awatch(*watch_paths, watch_filter=_source_file_filter)))

            watch_task = asyncio.create_task(watch_for_changes())
            wait_task = asyncio.create_task(process.wait())
            shutdown_task = asyncio.create_task(shutdown_event.wait())

            done, pending = await asyncio.wait(
                [watch_task, wait_task, shutdown_task],
                return_when=asyncio.FIRST_COMPLETED,
            )

            for task in pending:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

            if shutdown_task in done:
                # User requested shutdown
                break

            if wait_task in done:
                # Server died on its own - wait for file change before restart
                code = wait_task.result()
                if code != 0:
                    logging.error(f"Server exited with code {code}, waiting for file change...")
                else:
                    logging.info("Server exited, waiting for file change...")

                # Wait for file change or shutdown (avoid hot loop on crash)
                async def watch_for_changes() -> set[Any]:
                    return await anext(aiter(awatch(*watch_paths, watch_filter=_source_file_filter)))

                watch_task = asyncio.create_task(watch_for_changes())
                shutdown_task = asyncio.create_task(shutdown_event.wait())
                done, pending = await asyncio.wait(
                    [watch_task, shutdown_task],
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for task in pending:
                    task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await task
                if shutdown_task in done:
                    break
                logging.info("Detected changes, restarting...")
            else:
                # File changed - restart server
                changes = watch_task.result()
                logging.info(f"Detected changes in {len(changes)} file(s), restarting...")
                await _terminate_process(process)

    except KeyboardInterrupt:
        # Handle Ctrl+C on Windows (where add_signal_handler isn't available)
        logging.info("Received shutdown signal, stopping...")

    finally:
        # Clean up signal handlers
        if sys.platform != "win32":
            loop.remove_signal_handler(signal.SIGTERM)
            loop.remove_signal_handler(signal.SIGINT)
        if process and process.returncode is None:
            await _terminate_process(process)
        # Clean up Windows job object
        if job and sys.platform == "win32":
            try:
                ctypes.windll.kernel32.CloseHandle(job)
                logging.debug("Closed Windows job object")
            except Exception:
                pass


def main() -> None:
    """Main entry point."""
    args = parse_arguments()
    setup_logging(args.debug)

    from . import __version__

    logging.info(f"Running version {__version__}")

    # Handle reload mode
    if args.reload:
        # Build command to run without reload flag (prevent infinite spawning)
        # Always include --stateless for seamless restarts
        cmd = [sys.executable, "-m", "dbt_core_mcp", "--stateless"]
        if args.debug:
            cmd.append("--debug")
        if args.project_dir:
            cmd.extend(["--project-dir", args.project_dir])
        if args.dbt_command_timeout is not None:
            cmd.extend(["--dbt-command-timeout", str(args.dbt_command_timeout)])

        # Parse reload directories
        reload_dirs = None
        if args.reload_dir:
            reload_dirs = [Path(d).resolve() for d in args.reload_dir]

        # Run with reload
        try:
            asyncio.run(run_with_reload(cmd, reload_dirs))
        except KeyboardInterrupt:
            logging.info("Server stopped by user")
        return

    # Normal run mode (no reload)
    # Pass project_dir if specified, otherwise let server auto-detect from workspace roots
    # Treat timeout <= 0 as None (no timeout)
    timeout = args.dbt_command_timeout if args.dbt_command_timeout and args.dbt_command_timeout > 0 else None
    server = create_server(project_dir=args.project_dir, timeout=timeout)

    try:
        # Enable stateless mode if requested (automatically enabled by --reload)
        server.run(stateless=args.stateless)
    except KeyboardInterrupt:
        logging.info("Server stopped by user")
    except Exception as e:
        logging.error(f"Server error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
