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
import signal
import sys
from pathlib import Path
from typing import Any

from .server import create_server


def setup_logging(debug: bool = False) -> None:
    """Set up logging configuration."""
    import os
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
    return path.endswith(".py") or path.endswith(".html")


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
            # Start the subprocess
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdin=None,
                stdout=None,
                stderr=None,
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
