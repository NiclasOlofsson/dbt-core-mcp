"""
Warehouse Adapter Protocol.

Provides an interface for database-specific warehouse operations like pre-warming,
with implementations for different database platforms (Databricks, Snowflake, etc.).
"""

import logging
from pathlib import Path
from typing import Any, Callable, Protocol

logger = logging.getLogger(__name__)


class WarehouseAdapter(Protocol):
    """Protocol for warehouse-specific operations."""

    async def prewarm(self, progress_callback: Callable[[int, int, str], Any] | None = None) -> None:
        """
        Pre-warm the warehouse/cluster before executing dbt commands.

        This method is called before dbt operations that require database access.
        For serverless warehouses, this starts the warehouse and waits for it to be ready.
        For other databases, this may be a no-op.

        Multiple calls to prewarm() should be safe - if the warehouse is already running,
        the operation should be idempotent.

        Args:
            progress_callback: Optional callback for progress updates (current, total, message)
        """
        ...


class NoOpWarehouseAdapter:
    """
    Default no-op warehouse adapter for databases that don't need pre-warming.

    Used for databases like Postgres, DuckDB, BigQuery, etc. that don't have
    cold-start delays or where pre-warming isn't beneficial.
    """

    async def prewarm(self, progress_callback: Callable[[int, int, str], Any] | None = None) -> None:
        """No-op pre-warm for databases that don't need it."""
        logger.debug("No warehouse pre-warming needed for this database type")


def create_warehouse_adapter(project_dir: Path, adapter_type: str) -> WarehouseAdapter:
    """
    Factory function to create the appropriate warehouse adapter.

    Args:
        project_dir: Path to the dbt project directory
        adapter_type: The dbt adapter type (e.g., 'databricks', 'snowflake', 'postgres')

    Returns:
        WarehouseAdapter instance for the specified database type

    Examples:
        >>> adapter = create_warehouse_adapter(Path("/project"), "databricks")
        >>> await adapter.prewarm()  # Starts Databricks serverless warehouse

        >>> adapter = create_warehouse_adapter(Path("/project"), "postgres")
        >>> await adapter.prewarm()  # No-op for Postgres
    """
    adapter_type_lower = adapter_type.lower()

    if adapter_type_lower == "databricks":
        # Import here to avoid dependency issues if databricks libs not installed
        from .warehouse_databricks import DatabricksWarehouseAdapter

        logger.info(f"Creating Databricks warehouse adapter for {project_dir}")
        return DatabricksWarehouseAdapter(project_dir)

    # TODO: Add Snowflake adapter when needed
    # elif adapter_type_lower == "snowflake":
    #     from .warehouse_snowflake import SnowflakeWarehouseAdapter
    #     return SnowflakeWarehouseAdapter(project_dir)

    # Default to no-op for all other databases
    logger.info(f"Using no-op warehouse adapter for {adapter_type}")
    return NoOpWarehouseAdapter()
