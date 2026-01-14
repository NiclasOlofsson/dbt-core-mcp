"""
dbt Core MCP Server Context.

Application-scoped context initialized once at server startup and shared with all tools.
"""

import asyncio
import json
import logging
import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml
from fastmcp import FastMCP
from fastmcp.server.context import Context

if TYPE_CHECKING:
    from .dbt.bridge_runner import BridgeRunner
    from .dbt.manifest import ManifestLoader

logger = logging.getLogger(__name__)


@dataclass
class DbtCoreServerContext:
    """Application-scoped context accessible to all tools.

    This context is created once at server startup and injected into every tool
    via their setup() function. It provides access to:
    - Project configuration (directory, profiles)
    - dbt runner (for executing commands)
    - Manifest (for querying metadata)
    - Helper methods (parsing results, managing state, querying DB)
    """

    app: FastMCP
    project_dir: Path | None
    profiles_dir: str
    timeout: float | None
    runner: "BridgeRunner | None"
    manifest: "ManifestLoader | None"
    adapter_type: str | None
    force_fresh_runner: bool
    experimental_features: bool
    _init_lock: asyncio.Lock
    _explicit_project_dir: Path | None
    server: Any = None  # Type is DbtCoreMcpServer but use Any to avoid circular import

    async def ensure_initialized(self, ctx: Any, force_parse: bool = False) -> None:
        """Ensure server is initialized (delegates to server instance)."""
        if self.server:
            await self.server.ensure_initialized_with_context(ctx, force_parse=force_parse)

    async def get_runner(self) -> "BridgeRunner":
        """Get BridgeRunner instance (delegates to server instance)."""
        if self.server:
            return await self.server.get_runner()
        raise RuntimeError("Server not initialized")

    def parse_run_results(self) -> dict[str, Any]:
        """Parse target/run_results.json after dbt run/test/build.

        Returns:
            Dictionary with results array and metadata
        """
        if not self.project_dir:
            return {"results": [], "elapsed_time": 0}

        run_results_path = self.project_dir / "target" / "run_results.json"
        if not run_results_path.exists():
            return {"results": [], "elapsed_time": 0}

        try:
            with open(run_results_path, encoding="utf-8") as f:
                data = json.load(f)

            # Simplify results for output
            simplified_results = []
            for result in data.get("results", []):
                simplified_result = {
                    "unique_id": result.get("unique_id"),
                    "status": result.get("status"),
                    "message": result.get("message"),
                    "execution_time": result.get("execution_time"),
                    "failures": result.get("failures"),
                }

                # Include additional diagnostic fields for failed tests
                if result.get("status") in ("fail", "error"):
                    simplified_result["compiled_code"] = result.get("compiled_code")
                    simplified_result["adapter_response"] = result.get("adapter_response")

                simplified_results.append(simplified_result)

            return {
                "results": simplified_results,
                "elapsed_time": data.get("elapsed_time", 0),
            }
        except Exception as e:
            logger.warning(f"Failed to parse run_results.json: {e}")
            return {"results": [], "elapsed_time": 0}

    def validate_and_parse_results(self, result: Any, command_name: str) -> dict[str, Any]:
        """Parse run_results.json and validate execution succeeded.

        Args:
            result: The execution result from dbt runner
            command_name: Name of dbt command (e.g., "run", "test", "build", "seed")

        Returns:
            Parsed run_results dictionary

        Raises:
            RuntimeError: If dbt failed before execution (parse error, connection failure, etc.)
        """
        run_results = self.parse_run_results()

        if not run_results.get("results"):
            # No results means dbt failed before execution
            if result and not result.success:
                error_msg = str(result.exception) if result.exception else f"dbt {command_name} execution failed"
                # Extract specific error from stdout if available
                if result.stdout and "Error" in result.stdout:
                    lines = result.stdout.split("\n")
                    for i, line in enumerate(lines):
                        if "Error" in line or "error" in line:
                            error_msg = "\n".join(lines[i : min(i + 5, len(lines))]).strip()
                            break
                else:
                    # Include full stdout/stderr for debugging when no specific error found
                    stdout_preview = (result.stdout[:500] + "...") if result.stdout and len(result.stdout) > 500 else (result.stdout or "(no stdout)")
                    stderr_preview = (result.stderr[:500] + "...") if result.stderr and len(result.stderr) > 500 else (result.stderr or "(no stderr)")
                    error_msg = f"{error_msg}\nstdout: {stdout_preview}\nstderr: {stderr_preview}"
                raise RuntimeError(f"dbt {command_name} failed to execute: {error_msg}")

        return run_results

    async def report_final_progress(
        self,
        ctx: Context | None,
        results_list: list[dict[str, Any]],
        command_name: str,
        resource_type: str,
    ) -> None:
        """Report final progress with status breakdown.

        Args:
            ctx: MCP context for progress reporting
            results_list: List of result dictionaries from dbt execution
            command_name: Command prefix for message (e.g., "Run", "Test", "Build")
            resource_type: Resource type for message (e.g., "models", "tests", "resources")
        """
        if not ctx:
            return

        if not results_list:
            await ctx.report_progress(progress=0, total=0, message=f"0 {resource_type} matched selector")
            return

        # Count statuses - different commands use different status values
        total = len(results_list)
        passed_count = sum(1 for r in results_list if r.get("status") in ("success", "pass"))
        failed_count = sum(1 for r in results_list if r.get("status") in ("error", "fail"))
        skip_count = sum(1 for r in results_list if r.get("status") in ("skipped", "skip"))
        warn_count = sum(1 for r in results_list if r.get("status") == "warn")

        # Build status parts
        parts = []
        if passed_count > 0:
            # Use "All passed" only if no other statuses present
            has_other_statuses = failed_count > 0 or warn_count > 0 or skip_count > 0
            parts.append(f"✅ {passed_count} passed" if has_other_statuses else "✅ All passed")
        if failed_count > 0:
            parts.append(f"❌ {failed_count} failed")
        if warn_count > 0:
            parts.append(f"⚠️ {warn_count} warned")
        if skip_count > 0:
            parts.append(f"⏭️ {skip_count} skipped")

        summary = f"{command_name}: {total}/{total} {resource_type} completed ({', '.join(parts)})"
        await ctx.report_progress(progress=total, total=total, message=summary)

    async def get_table_schema_from_db(self, model_name: str, source_name: str | None = None) -> list[dict[str, Any]]:
        """Get full table schema from database using DESCRIBE.

        Args:
            model_name: Name of the model/table
            source_name: If provided, treat as source and use source() instead of ref()

        Returns:
            List of column dictionaries with details (column_name, column_type, null, etc.)
            Empty list if query fails or table doesn't exist
        """
        try:
            if source_name:
                sql = f"DESCRIBE {{{{ source('{source_name}', '{model_name}') }}}}"
            else:
                sql = f"DESCRIBE {{{{ ref('{model_name}') }}}}"

            runner = await self.get_runner()
            result = await runner.invoke_query(sql)  # type: ignore

            if not result.success or not result.stdout:
                return []

            # Parse JSON output using robust regex + JSONDecoder
            json_match = re.search(r'\{\s*"show"\s*:\s*\[', result.stdout)
            if not json_match:
                return []

            decoder = json.JSONDecoder()
            data, _ = decoder.raw_decode(result.stdout, json_match.start())

            if "show" in data:
                return data["show"]  # type: ignore[no-any-return]

            return []
        except Exception as e:
            logger.warning(f"Failed to query table schema for {model_name}: {e}")
            return []

    async def get_table_columns_from_db(self, model_name: str) -> list[str]:
        """Get actual column names from database table.

        Args:
            model_name: Name of the model

        Returns:
            List of column names from the actual table
        """
        schema = await self.get_table_schema_from_db(model_name)
        if not schema:
            return []

        # Extract column names from schema
        columns: list[str] = []
        for row in schema:
            # Try common column name fields
            col_name = row.get("column_name") or row.get("Field") or row.get("name") or row.get("COLUMN_NAME")
            if col_name and isinstance(col_name, str):
                columns.append(col_name)

        logger.info(f"Extracted {len(columns)} columns for {model_name}: {columns}")
        return sorted(columns)

    def clear_stale_run_results(self) -> None:
        """Delete stale run_results.json before command execution.

        This prevents reading cached results from previous runs.
        """
        if not self.project_dir:
            return

        run_results_path = self.project_dir / "target" / "run_results.json"
        if run_results_path.exists():
            try:
                run_results_path.unlink()
                logger.debug("Deleted stale run_results.json before execution")
            except OSError as e:
                logger.warning(f"Could not delete stale run_results.json: {e}")

    async def save_execution_state(self) -> None:
        """Save current manifest as state for future state-based runs.

        After successful execution, saves manifest.json to target/state_last_run/
        so future runs can use --state to detect modifications.
        """
        if not self.project_dir or not self.runner:
            return

        state_dir = self.project_dir / "target" / "state_last_run"
        state_dir.mkdir(parents=True, exist_ok=True)

        manifest_path = self.runner.get_manifest_path()  # type: ignore

        try:
            shutil.copy(manifest_path, state_dir / "manifest.json")
            logger.debug(f"Saved execution state to {state_dir}")
        except OSError as e:
            logger.warning(f"Failed to save execution state: {e}")

    def get_project_paths(self) -> dict[str, list[str]]:
        """Read configured paths from dbt_project.yml.

        Returns:
            Dictionary with path types as keys and lists of paths as values
        """
        if not self.project_dir:
            return {}

        project_file = self.project_dir / "dbt_project.yml"
        if not project_file.exists():
            return {}

        try:
            with open(project_file, encoding="utf-8") as f:
                config = yaml.safe_load(f)

            return {
                "model-paths": config.get("model-paths", ["models"]),
                "seed-paths": config.get("seed-paths", ["seeds"]),
                "snapshot-paths": config.get("snapshot-paths", ["snapshots"]),
                "analysis-paths": config.get("analysis-paths", ["analyses"]),
                "macro-paths": config.get("macro-paths", ["macros"]),
                "test-paths": config.get("test-paths", ["tests"]),
            }
        except Exception as e:
            logger.warning(f"Failed to parse dbt_project.yml: {e}")
            return {}

    def compare_model_schemas(
        self,
        model_unique_ids: list[str],
        state_manifest_path: Path,
    ) -> dict[str, Any]:
        """Compare schemas of models before and after run.

        Args:
            model_unique_ids: List of model unique IDs that were run
            state_manifest_path: Path to the saved state manifest.json

        Returns:
            Dictionary with schema changes per model
        """
        if not state_manifest_path.exists() or not self.manifest:
            return {}

        try:
            # Load state (before) manifest
            with open(state_manifest_path, encoding="utf-8") as f:
                state_manifest = json.load(f)

            current_manifest_data = self.manifest.get_manifest_dict()
            schema_changes: dict[str, dict[str, Any]] = {}

            for unique_id in model_unique_ids:
                # Skip non-model nodes (like tests)
                if not unique_id.startswith("model."):
                    continue

                # Get before and after column definitions
                before_node = state_manifest.get("nodes", {}).get(unique_id, {})
                after_node = current_manifest_data.get("nodes", {}).get(unique_id, {})

                before_columns = before_node.get("columns", {})
                after_columns = after_node.get("columns", {})

                # Skip if no column definitions exist (not in schema.yml)
                if not before_columns and not after_columns:
                    continue

                # Compare columns
                before_names = set(before_columns.keys())
                after_names = set(after_columns.keys())

                added = sorted(after_names - before_names)
                removed = sorted(before_names - after_names)

                # Check for type changes in common columns
                changed_types = {}
                for col in before_names & after_names:
                    before_type = before_columns[col].get("data_type")
                    after_type = after_columns[col].get("data_type")
                    if before_type != after_type and before_type is not None and after_type is not None:
                        changed_types[col] = {"from": before_type, "to": after_type}

                # Only record if there are actual changes
                if added or removed or changed_types:
                    model_name = after_node.get("name", unique_id.split(".")[-1])
                    schema_changes[model_name] = {
                        "changed": True,
                        "added_columns": added,
                        "removed_columns": removed,
                        "changed_types": changed_types,
                    }

            return schema_changes

        except Exception as e:
            logger.warning(f"Failed to compare schemas: {e}")
            return {}

    def manifest_exists(self) -> bool:
        """Check if manifest.json exists.

        Simple check - tools will handle their own parsing as needed.
        """
        if self.project_dir is None:
            return False
        manifest_path = self.project_dir / "target" / "manifest.json"
        return manifest_path.exists()

    async def prepare_state_based_selection(
        self,
        select_state_modified: bool,
        select_state_modified_plus_downstream: bool,
        select: str | None,
    ) -> str | None:
        """Validate and prepare state-based selection.

        Args:
            select_state_modified: Use state:modified selector
            select_state_modified_plus_downstream: Extend to state:modified+
            select: Manual selector (conflicts with state-based)

        Returns:
            The dbt selector string to use ("state:modified" or "state:modified+"), or None if:
            - Not using state-based selection
            - No previous state exists (cannot determine modifications)

        Raises:
            ValueError: If validation fails
        """
        # Validate: hierarchical requirement
        if select_state_modified_plus_downstream and not select_state_modified:
            raise ValueError("select_state_modified_plus_downstream requires select_state_modified=True")

        # Validate: can't use both state-based and manual selection
        if select_state_modified and select:
            raise ValueError("Cannot use both select_state_modified* flags and select parameter")

        # If not using state-based selection, return None
        if not select_state_modified:
            return None

        # Check if state exists
        if not self.project_dir:
            return None

        state_dir = self.project_dir / "target" / "state_last_run"
        if not state_dir.exists():
            # No state - cannot determine modifications
            return None

        # Return selector (state exists)
        return "state:modified+" if select_state_modified_plus_downstream else "state:modified"
