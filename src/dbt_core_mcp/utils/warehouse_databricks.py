"""
Databricks Warehouse Adapter.

Provides pre-warming capabilities for Databricks serverless SQL warehouses
to eliminate cold-start delays before dbt command execution.
"""

import asyncio
import logging
from pathlib import Path
from typing import Any, Callable

logger = logging.getLogger(__name__)


class DatabricksProfileError(Exception):
    """Raised when Databricks profile configuration is invalid or missing."""

    pass


class DatabricksWarehouseAdapter:
    """
    Warehouse adapter for Databricks serverless SQL warehouses.

    This adapter pre-warms Databricks warehouses by starting them via API
    and polling until they reach RUNNING state. This eliminates the ~30s
    cold-start delay that dbt would otherwise experience.

    The adapter reads connection info from dbt profiles (profiles.yml) and
    uses the Databricks SQL Warehouses API for control operations.
    """

    def __init__(self, project_dir: Path):
        """
        Initialize Databricks warehouse adapter.

        Args:
            project_dir: Path to the dbt project directory
        """
        self.project_dir = project_dir
        self._connection_info = None  # Lazy-loaded connection details
        self._is_running = False  # Track if we've already started the warehouse

    async def prewarm(self, progress_callback: Callable[[int, int, str], Any] | None = None) -> None:
        """
        Pre-warm the Databricks serverless warehouse.

        Starts the warehouse if not already running and waits for it to reach
        RUNNING state. This operation is idempotent - calling it multiple times
        is safe and won't cause issues.

        Args:
            progress_callback: Optional callback for progress updates (current, total, message)

        Raises:
            DatabricksProfileError: If profile configuration is invalid
            RuntimeError: If warehouse fails to start or times out
        """
        # If we've already started the warehouse in this session, skip
        if self._is_running:
            logger.debug("Warehouse already pre-warmed in this session, skipping")
            return

        logger.info("Pre-warming Databricks serverless warehouse...")

        # Report initial progress
        logger.info(f"Progress callback is: {progress_callback}")
        if progress_callback:
            logger.info("Invoking initial progress callback: 'Initializing warehouse...'")
            try:
                result = progress_callback(0, 1, "Initializing warehouse...")
                logger.info(f"Progress callback result type: {type(result)}")
                if asyncio.iscoroutine(result):
                    await result
                logger.info("Initial progress callback completed")
            except Exception as e:
                logger.warning(f"Progress callback error: {e}")
        else:
            logger.warning("No progress callback provided to prewarm")

        # Get connection info from dbt profile
        try:
            instance, token, warehouse_id = await self._get_connection_info()
        except DatabricksProfileError as e:
            logger.error(f"Failed to get Databricks connection info: {e}")
            raise

        # Import requests here to avoid dependency at module level
        try:
            import requests
        except ImportError:
            logger.error("requests library not available, cannot pre-warm Databricks warehouse")
            raise RuntimeError("requests library required for Databricks pre-warming")

        headers = {"Authorization": f"Bearer {token}"}
        warehouse_url = f"https://{instance}/api/2.0/sql/warehouses/{warehouse_id}"

        # Check current warehouse state
        try:
            resp = await asyncio.to_thread(requests.get, warehouse_url, headers=headers)
            if resp.status_code != 200:
                raise RuntimeError(f"Failed to fetch warehouse info: {resp.text}")

            warehouse = resp.json()
            current_state = warehouse.get("state")
            warehouse_name = warehouse.get("name", warehouse_id)

            logger.info(f"Warehouse '{warehouse_name}' current state: {current_state}")

            # If already running, we're done
            if current_state == "RUNNING":
                logger.info("Warehouse already running, no pre-warming needed")
                self._is_running = True
                return

            # Verify it's a serverless warehouse
            is_serverless = warehouse.get("warehouse_type") == "PRO" and warehouse.get("enable_serverless_compute", False)
            if not is_serverless:
                logger.warning("Warehouse is not serverless, pre-warming may not be beneficial")

            # Start the warehouse
            start_url = f"https://{instance}/api/2.0/sql/warehouses/{warehouse_id}/start"
            start_resp = await asyncio.to_thread(requests.post, start_url, headers=headers)

            if start_resp.status_code != 200:
                raise RuntimeError(f"Failed to start warehouse: {start_resp.text}")

            logger.info(f"Started warehouse '{warehouse_name}', waiting for RUNNING state...")

            # Poll for RUNNING state with progress reporting
            max_wait = 300  # 5 minutes
            poll_interval = 5  # seconds
            waited = 0

            while waited < max_wait:
                await asyncio.sleep(poll_interval)
                waited += poll_interval

                # Report progress
                if progress_callback:
                    try:
                        result = progress_callback(waited, max_wait, f"Pre-warming warehouse '{warehouse_name}'... ({waited}s)")
                        if asyncio.iscoroutine(result):
                            await result
                    except Exception as e:
                        logger.warning(f"Progress callback error: {e}")

                state_resp = await asyncio.to_thread(requests.get, warehouse_url, headers=headers)
                if state_resp.status_code == 200:
                    state = state_resp.json().get("state")
                    logger.info(f"Warehouse state after {waited}s: {state}")

                    if state == "RUNNING":
                        logger.info(f"Warehouse is RUNNING after {waited}s")
                        # Final progress update
                        if progress_callback:
                            try:
                                result = progress_callback(max_wait, max_wait, f"Warehouse '{warehouse_name}' ready")
                                if asyncio.iscoroutine(result):
                                    await result
                            except Exception as e:
                                logger.warning(f"Progress callback error: {e}")
                        self._is_running = True
                        return
                else:
                    logger.warning(f"Failed to get warehouse state: {state_resp.text}")

            # Timeout
            raise RuntimeError(f"Timed out waiting for warehouse to start after {max_wait}s")

        except requests.RequestException as e:
            logger.error(f"Network error during warehouse pre-warming: {e}")
            raise RuntimeError(f"Failed to pre-warm warehouse: {e}")

    async def _get_connection_info(self) -> tuple[str, str, str]:
        """
        Extract Databricks connection info from dbt profile.

        Returns:
            Tuple of (instance, token, warehouse_id)

        Raises:
            DatabricksProfileError: If required configuration is missing
        """
        if self._connection_info:
            return self._connection_info

        # Get dbt profile configuration
        profile = await self._get_dbt_profile()

        # Extract connection details
        instance = profile.get("host", "").replace("https://", "").replace("/", "")
        token = profile.get("token")
        warehouse_id = profile.get("http_path", "").replace("/sql/1.0/warehouses/", "")

        if not instance or not token:
            raise DatabricksProfileError("Could not find Databricks instance or token in dbt profile")

        if not warehouse_id:
            raise DatabricksProfileError("No warehouse ID found in dbt profile config (http_path)")

        logger.debug(f"Using Databricks instance: {instance}, warehouse: {warehouse_id}")

        # Cache for future calls
        self._connection_info = (instance, token, warehouse_id)
        return self._connection_info

    async def _get_dbt_profile(self) -> dict[str, Any]:
        """
        Load dbt profile configuration from profiles.yml.

        Searches for profiles.yml in:
        1. Project directory (profiles.yml)
        2. User home directory (~/.dbt/profiles.yml)

        Returns:
            Dictionary with dbt profile target configuration

        Raises:
            DatabricksProfileError: If profiles not found or invalid
        """
        # Run YAML loading in thread to avoid blocking
        return await asyncio.to_thread(self._load_profile_sync)

    def _load_profile_sync(self) -> dict[str, Any]:
        """Synchronous helper to load profile from YAML files."""
        import yaml

        # First check project directory for profiles.yml
        local_profiles_path = self.project_dir / "profiles.yml"
        if local_profiles_path.exists():
            profiles_path = local_profiles_path
            logger.debug(f"Using local profiles.yml at {profiles_path}")
        else:
            # Fall back to ~/.dbt/profiles.yml
            dbt_dir = Path.home() / ".dbt"
            profiles_path = dbt_dir / "profiles.yml"
            if not profiles_path.exists():
                raise DatabricksProfileError(f"Could not find profiles.yml at {profiles_path}")
            logger.debug(f"Using user profiles.yml at {profiles_path}")

        # Load profiles.yml
        try:
            with open(profiles_path) as f:
                profiles = yaml.safe_load(f)
        except Exception as e:
            raise DatabricksProfileError(f"Failed to parse profiles.yml: {e}")

        # Get profile name from dbt_project.yml
        project_yml_path = self.project_dir / "dbt_project.yml"
        if not project_yml_path.exists():
            raise DatabricksProfileError(f"Could not find dbt_project.yml at {project_yml_path}")

        try:
            with open(project_yml_path) as f:
                project = yaml.safe_load(f)
        except Exception as e:
            raise DatabricksProfileError(f"Failed to parse dbt_project.yml: {e}")

        profile_name = project.get("profile")
        if not profile_name:
            raise DatabricksProfileError("No 'profile' key found in dbt_project.yml")

        # Get profile
        profile = profiles.get(profile_name)
        if not profile:
            raise DatabricksProfileError(f"Profile '{profile_name}' not found in profiles.yml")

        # Get target
        target_name = profile.get("target", "default")
        target = profile.get("outputs", {}).get(target_name)

        if not target:
            raise DatabricksProfileError(f"Target '{target_name}' not found in profile '{profile_name}'")

        logger.debug(f"Using profile '{profile_name}', target '{target_name}'")
        return target
