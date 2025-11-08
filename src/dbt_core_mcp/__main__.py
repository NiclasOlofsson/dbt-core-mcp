#!/usr/bin/env python3
"""
Main entry point for the dbt Core MCP Server.

This script provides the command-line interface to run the MCP server
for interacting with dbt projects.
"""

import argparse
import logging
import sys

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

    # Suppress FastMCP's internal INFO logs (they use structlog formatting)
    logging.getLogger("fastmcp").setLevel(logging.WARNING)

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

    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")

    return parser.parse_args()


def main() -> None:
    """Main entry point."""
    args = parse_arguments()
    setup_logging(args.debug)

    from . import __version__

    logging.info(f"Running version {__version__}")

    # Pass project_dir if specified, otherwise let server auto-detect from workspace roots
    # Treat timeout <= 0 as None (no timeout)
    timeout = args.dbt_command_timeout if args.dbt_command_timeout and args.dbt_command_timeout > 0 else None
    server = create_server(project_dir=args.project_dir, timeout=timeout)

    try:
        server.run()
    except KeyboardInterrupt:
        logging.info("Server stopped by user")
    except Exception as e:
        logging.error(f"Server error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
