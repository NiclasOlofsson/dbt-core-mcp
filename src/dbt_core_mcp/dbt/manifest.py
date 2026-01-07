"""
DBT Manifest Loader.

Reads and parses DBT's manifest.json file to provide structured access
to models, sources, tests, and other DBT entities.
"""

import asyncio
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class DbtModel:
    """Represents a dbt model from the manifest."""

    name: str
    unique_id: str
    resource_type: str
    schema: str
    database: str
    alias: str
    description: str
    materialization: str
    tags: list[str]
    depends_on: list[str]
    package_name: str
    original_file_path: str


@dataclass
class DbtSource:
    """Represents a dbt source from the manifest."""

    name: str
    unique_id: str
    source_name: str
    schema: str
    database: str
    identifier: str
    description: str
    tags: list[str]
    package_name: str


class ManifestLoader:
    """
    Load and parse DBT manifest.json.

    Provides structured access to models, sources, and other DBT entities.
    """

    def __init__(self, manifest_path: Path):
        """
        Initialize the manifest loader.

        Args:
            manifest_path: Path to manifest.json file
        """
        self.manifest_path = manifest_path
        self._manifest: dict[str, Any] | None = None
        self._manifest_mtime: float | None = None  # Track last modification time

    async def load(self, force: bool = False) -> None:
        """
        Load the manifest from disk.

        Args:
            force: If True, reload even if already loaded. If False, only reload if file changed.
        """
        if not self.manifest_path.exists():
            raise FileNotFoundError(f"Manifest not found: {self.manifest_path}")

        # Check if reload is needed
        current_mtime = self.manifest_path.stat().st_mtime

        if not force and self._manifest is not None and self._manifest_mtime == current_mtime:
            logger.debug("Manifest already loaded and unchanged, skipping reload")
            return

        logger.debug(f"Loading manifest from {self.manifest_path}")

        def _read_manifest() -> dict[str, Any]:
            with open(self.manifest_path, "r") as f:
                return json.load(f)

        self._manifest = await asyncio.to_thread(_read_manifest)
        self._manifest_mtime = current_mtime
        logger.info("Manifest loaded successfully")

    def is_loaded(self) -> bool:
        """Check if the manifest data has been loaded.

        Returns:
            True if manifest data is loaded in memory, False otherwise
        """
        return self._manifest is not None

    def get_resources(self, resource_type: str | None = None) -> list[dict[str, Any]]:
        """
        Get all resources from the manifest, optionally filtered by type.

        Returns simplified resource information across all types (models, sources, seeds, etc.).
        Designed for LLM consumption with consistent structure across resource types.

        Args:
            resource_type: Optional filter (model, source, seed, snapshot, test, analysis).
                          If None, returns all resources.

        Returns:
            List of resource dictionaries with consistent structure:
            {
                "name": str,
                "unique_id": str,
                "resource_type": str,
                "schema": str (if applicable),
                "database": str (if applicable),
                "description": str,
                "tags": list[str],
                "package_name": str,
                ...additional type-specific fields
            }

        Raises:
            RuntimeError: If manifest not loaded
            ValueError: If invalid resource_type provided
        """
        if not self._manifest:
            raise RuntimeError("Manifest not loaded. Call load() first.")

        # Validate resource_type if provided
        valid_types = {"model", "source", "seed", "snapshot", "test", "analysis"}
        if resource_type is not None and resource_type not in valid_types:
            raise ValueError(f"Invalid resource_type '{resource_type}'. Must be one of: {', '.join(sorted(valid_types))}")

        resources: list[dict[str, Any]] = []

        # Collect from nodes (models, tests, seeds, snapshots, analyses)
        nodes = self._manifest.get("nodes", {})
        for unique_id, node in nodes.items():
            if not isinstance(node, dict):
                continue

            node_type = node.get("resource_type")

            # Filter by type if specified
            if resource_type is not None and node_type != resource_type:
                continue

            # Build consistent resource dict
            resource: dict[str, Any] = {
                "name": node.get("name", ""),
                "unique_id": unique_id,
                "resource_type": node_type,
                "package_name": node.get("package_name", ""),
                "description": node.get("description", ""),
                "tags": node.get("tags", []),
            }

            # Add common fields for materialized resources
            if node_type in ("model", "seed", "snapshot"):
                resource["schema"] = node.get("schema", "")
                resource["database"] = node.get("database", "")
                resource["alias"] = node.get("alias", "")

            # Add type-specific fields
            if node_type == "model":
                resource["materialization"] = node.get("config", {}).get("materialized", "")
                resource["file_path"] = node.get("original_file_path", "")
            elif node_type == "seed":
                resource["file_path"] = node.get("original_file_path", "")
            elif node_type == "snapshot":
                resource["file_path"] = node.get("original_file_path", "")
            elif node_type == "test":
                resource["test_metadata"] = node.get("test_metadata", {})
                resource["column_name"] = node.get("column_name")

            resources.append(resource)

        # Collect from sources (if not filtered out)
        if resource_type is None or resource_type == "source":
            sources = self._manifest.get("sources", {})
            for unique_id, source in sources.items():
                if not isinstance(source, dict):
                    continue

                resource = {
                    "name": source.get("name", ""),
                    "unique_id": unique_id,
                    "resource_type": "source",
                    "source_name": source.get("source_name", ""),
                    "schema": source.get("schema", ""),
                    "database": source.get("database", ""),
                    "identifier": source.get("identifier", ""),
                    "package_name": source.get("package_name", ""),
                    "description": source.get("description", ""),
                    "tags": source.get("tags", []),
                }

                resources.append(resource)

        logger.debug(f"Found {len(resources)} resources" + (f" of type '{resource_type}'" if resource_type else ""))
        return resources

    def get_compiled_code(self, name: str) -> str | None:
        """
        Get the compiled SQL code for a model.

        Args:
            name: Model name

        Returns:
            Compiled SQL string if available, None if not compiled yet

        Raises:
            RuntimeError: If manifest not loaded
            ValueError: If model not found
        """
        node = self.get_resource_node(name, "model")  # Will raise ValueError if not found
        return node.get("compiled_code")

    def get_resource_node(self, name: str, resource_type: str | None = None) -> dict[str, Any]:
        """
        Get a resource node by name with auto-detection across all resource types.

        This method searches for resources across models, sources, seeds, snapshots, tests, etc.
        Designed for LLM consumption - returns all matches when ambiguous rather than raising errors.

        Args:
            name: Resource name. For sources, can be "source_name.table_name" or just "table_name"
            resource_type: Optional filter (model, source, seed, snapshot, test, analysis).
                          If None, searches all types.

        Returns:
            Single resource dict if exactly one match found, or dict with multiple_matches=True
            containing all matching resources for LLM to process.

        Raises:
            RuntimeError: If manifest not loaded
            ValueError: If resource not found (only case that raises)

        Examples:
            get_resource_node("customers") -> single model dict
            get_resource_node("customers", "source") -> single source dict
            get_resource_node("customers") with multiple matches -> {"multiple_matches": True, ...}
        """
        if not self._manifest:
            raise RuntimeError("Manifest not loaded. Call load() first.")

        # Validate resource_type if provided
        valid_types = {"model", "source", "seed", "snapshot", "test", "analysis"}
        if resource_type is not None and resource_type not in valid_types:
            raise ValueError(f"Invalid resource_type '{resource_type}'. Must be one of: {', '.join(sorted(valid_types))}")

        matches: list[dict[str, Any]] = []

        # For sources, try "source_name.table_name" format first
        if "." in name and (resource_type is None or resource_type == "source"):
            parts = name.split(".", 1)
            if len(parts) == 2:
                # Search sources dict directly
                sources_dict = self._manifest.get("sources", {})
                for _, source in sources_dict.items():
                    if isinstance(source, dict) and source.get("source_name") == parts[0] and source.get("name") == parts[1]:
                        matches.append(dict(source))
                        break

        # Search nodes (models, tests, snapshots, seeds, analyses, etc.)
        nodes = self._manifest.get("nodes", {})
        for unique_id, node in nodes.items():
            if not isinstance(node, dict):
                continue

            node_type = node.get("resource_type")
            node_name = node.get("name")

            # Type filter if specified
            if resource_type is not None and node_type != resource_type:
                continue

            if node_name == name:
                matches.append(dict(node))

        # Search sources by table name only (fallback when no dot in name)
        if resource_type is None or resource_type == "source":
            sources = self._manifest.get("sources", {})
            for unique_id, source in sources.items():
                if not isinstance(source, dict):
                    continue

                if source.get("name") == name:
                    # Avoid duplicates if already matched via source_name.table_name
                    if not any(m.get("unique_id") == unique_id for m in matches):
                        matches.append(dict(source))

        # Handle results based on match count
        if len(matches) == 0:
            type_label = resource_type.title() if resource_type else "Resource"
            list_hint = f"Use list_resources(type='{resource_type}') to see all available {resource_type}s." if resource_type else "Use list_resources() to see all available resources."
            raise ValueError(f"{type_label} '{name}' not found.\n{list_hint}")
        elif len(matches) == 1:
            # Single match - return the resource directly
            return matches[0]
        else:
            # Multiple matches - return all with metadata for LLM to process
            return {
                "multiple_matches": True,
                "name": name,
                "match_count": len(matches),
                "matches": matches,
                "message": f"Found {len(matches)} resources named '{name}'. Returning all matches for context.",
            }

    def get_resource_info(
        self,
        name: str,
        resource_type: str | None = None,
        include_database_schema: bool = True,
        include_compiled_sql: bool = True,
    ) -> dict[str, Any]:
        """Get detailed resource information with optional enrichments.

        This method extends get_resource_node() with optional enrichments:
        - include_database_schema: Query actual database schema
        - include_compiled_sql: Include compiled SQL (models only, requires compilation)

        Note: This method does NOT trigger compilation. If compiled SQL is requested but
        not available in the manifest, the 'compiled_sql' field will be None. The caller
        (e.g., server tool) is responsible for triggering compilation if needed.

        Args:
            name: Resource name
            resource_type: Optional resource type filter
            include_database_schema: Include database schema information (default: True)
            include_compiled_sql: Include compiled SQL for models (default: True)

        Returns:
            Resource dictionary with optional enrichments
        """
        result = self.get_resource_node(name, resource_type)

        # Handle multiple matches case - return as-is
        if result.get("multiple_matches"):
            return result

        # Single match - enrich with additional data if requested
        node_type = result.get("resource_type")

        # Create a copy without heavy fields
        result_copy = dict(result)
        result_copy.pop("raw_code", None)
        result_copy.pop("compiled_code", None)

        # Include compiled SQL for models if requested and available
        if include_compiled_sql and node_type == "model":
            compiled_code = result.get("compiled_code")

            if compiled_code:
                result_copy["compiled_sql"] = compiled_code
                result_copy["compiled_sql_cached"] = True
            else:
                # Not compiled yet - set to None to indicate it's not available
                result_copy["compiled_sql"] = None
                result_copy["compiled_sql_cached"] = False

        return result_copy

    def get_project_info(self) -> dict[str, Any]:
        """
        Get high-level project information from the manifest.

        Returns:
            Dictionary with project metadata
        """
        if not self._manifest:
            raise RuntimeError("Manifest not loaded. Call load() first.")

        metadata: dict[str, Any] = self._manifest.get("metadata", {})  # type: ignore[assignment]

        # Count resources directly from manifest
        nodes = self._manifest.get("nodes", {})
        model_count = sum(1 for node in nodes.values() if isinstance(node, dict) and node.get("resource_type") == "model")
        source_count = len(self._manifest.get("sources", {}))

        return {
            "project_name": metadata.get("project_name", ""),
            "dbt_version": metadata.get("dbt_version", ""),
            "adapter_type": metadata.get("adapter_type", ""),
            "generated_at": metadata.get("generated_at", ""),
            "model_count": model_count,
            "source_count": source_count,
        }

    def get_manifest_dict(self) -> dict[str, Any]:
        """Get the raw manifest dictionary.

        Returns:
            Raw manifest dictionary

        Raises:
            RuntimeError: If manifest not loaded
        """
        if not self._manifest:
            raise RuntimeError("Manifest not loaded. Call load() first.")
        return self._manifest

    def get_node_by_unique_id(self, unique_id: str) -> dict[str, Any] | None:
        """Get a node (model, test, etc.) by its unique_id.

        Args:
            unique_id: The unique identifier (e.g., 'model.package.model_name')

        Returns:
            Node dictionary or None if not found
        """
        if not self._manifest:
            raise RuntimeError("Manifest not loaded. Call load() first.")

        # Check nodes first (models, tests, snapshots, etc.)
        nodes = self._manifest.get("nodes", {})
        if unique_id in nodes:
            return dict(nodes[unique_id])

        # Check sources
        sources = self._manifest.get("sources", {})
        if unique_id in sources:
            return dict(sources[unique_id])

        return None

    def get_upstream_nodes(self, unique_id: str, max_depth: int | None = None, current_depth: int = 0) -> list[dict[str, Any]]:
        """Get all upstream dependencies of a node recursively.

        Args:
            unique_id: The unique identifier of the node
            max_depth: Maximum depth to traverse (None for unlimited)
            current_depth: Current recursion depth (internal use)

        Returns:
            List of dictionaries with upstream node info:
            {"unique_id": str, "name": str, "type": str, "distance": int}
        """
        if not self._manifest:
            raise RuntimeError("Manifest not loaded. Call load() first.")

        if max_depth is not None and current_depth >= max_depth:
            return []

        parent_map = self._manifest.get("parent_map", {})
        parents = parent_map.get(unique_id, [])

        upstream: list[dict[str, Any]] = []
        seen: set[str] = set()

        for parent_id in parents:
            if parent_id in seen:
                continue
            seen.add(parent_id)

            node = self.get_node_by_unique_id(parent_id)
            if node:
                resource_type = node.get("resource_type", "unknown")
                upstream.append(
                    {
                        "unique_id": parent_id,
                        "name": node.get("name", ""),
                        "type": resource_type,
                        "distance": current_depth + 1,
                    }
                )

                # Recurse
                if max_depth is None or current_depth + 1 < max_depth:
                    grandparents = self.get_upstream_nodes(parent_id, max_depth, current_depth + 1)
                    for gp in grandparents:
                        if gp["unique_id"] not in seen:
                            seen.add(str(gp["unique_id"]))
                            upstream.append(gp)

        return upstream

    def get_lineage(
        self,
        name: str,
        resource_type: str | None = None,
        direction: str = "both",
        depth: int | None = None,
    ) -> dict[str, Any]:
        """
        Get lineage (dependency tree) for any resource type with auto-detection.

        This unified method works across all resource types (models, sources, seeds, etc.)
        and provides upstream, downstream, or bidirectional dependency traversal.

        Args:
            name: Resource name. For sources, use "source_name.table_name" or just "table_name"
            resource_type: Optional filter (model, source, seed, snapshot, test, analysis).
                          If None, auto-detects resource type.
            direction: Lineage direction:
                - "upstream": Show where data comes from (parents)
                - "downstream": Show what depends on this resource (children)
                - "both": Show full lineage (default)
            depth: Maximum levels to traverse (None for unlimited)
                - depth=1: Immediate dependencies only
                - depth=2: Dependencies + their dependencies
                - None: Full dependency tree

        Returns:
            Dictionary with lineage information:
            {
                "resource": {...},  # The target resource info
                "upstream": [...],  # List of upstream dependencies (if direction in ["upstream", "both"])
                "downstream": [...],  # List of downstream dependents (if direction in ["downstream", "both"])
                "stats": {
                    "upstream_count": int,
                    "downstream_count": int,
                    "total_dependencies": int
                }
            }

            If multiple matches found, returns:
            {"multiple_matches": True, "matches": [...], "message": "..."}

        Raises:
            RuntimeError: If manifest not loaded
            ValueError: If resource not found or invalid direction

        Examples:
            get_lineage("customers") -> auto-detect and show full lineage
            get_lineage("customers", "model", "upstream") -> show where customers model gets data
            get_lineage("customers", direction="downstream", depth=2) -> 2 levels of dependents
        """
        if not self._manifest:
            raise RuntimeError("Manifest not loaded. Call load() first.")

        # Validate direction
        valid_directions = {"upstream", "downstream", "both"}
        if direction not in valid_directions:
            raise ValueError(f"Invalid direction '{direction}'. Must be one of: {', '.join(sorted(valid_directions))}")

        # Get the resource (auto-detect if resource_type not specified)
        resource = self.get_resource_node(name, resource_type)

        # Handle multiple matches - return for LLM to process
        if resource.get("multiple_matches"):
            return resource

        # Extract unique_id for lineage traversal
        unique_id = resource.get("unique_id")
        if not unique_id:
            raise ValueError(f"Resource '{name}' does not have a unique_id")

        # Build lineage based on direction
        result: dict[str, Any] = {
            "resource": {
                "name": resource.get("name"),
                "unique_id": unique_id,
                "resource_type": resource.get("resource_type"),
                "package_name": resource.get("package_name"),
            }
        }

        upstream: list[dict[str, Any]] = []
        downstream: list[dict[str, Any]] = []

        if direction in ("upstream", "both"):
            upstream = self.get_upstream_nodes(unique_id, max_depth=depth)
            result["upstream"] = upstream

        if direction in ("downstream", "both"):
            downstream = self.get_downstream_nodes(unique_id, max_depth=depth)
            result["downstream"] = downstream

        # Add statistics
        result["stats"] = {
            "upstream_count": len(upstream),
            "downstream_count": len(downstream),
            "total_dependencies": len(upstream) + len(downstream),
        }

        return result

    def analyze_impact(
        self,
        name: str,
        resource_type: str | None = None,
    ) -> dict[str, Any]:
        """
        Analyze the impact of changing a resource across all resource types.

        Shows all downstream dependencies that would be affected by changes,
        including models, tests, and other resources. Provides actionable
        recommendations for running affected resources.

        Args:
            name: Resource name. For sources, use "source_name.table_name" or just "table_name"
            resource_type: Optional filter (model, source, seed, snapshot, test, analysis).
                          If None, auto-detects resource type.

        Returns:
            Dictionary with impact analysis:
            {
                "resource": {...},  # The target resource info
                "impact": {
                    "models_affected": [...],  # Downstream models by distance
                    "models_affected_count": int,
                    "tests_affected_count": int,
                    "other_affected_count": int,
                    "total_affected": int
                },
                "affected_by_distance": {
                    "1": [...],  # Immediate dependents
                    "2": [...],  # Second-level dependents
                    ...
                },
                "recommendation": str,  # Suggested dbt command
                "message": str  # Human-readable impact assessment
            }

            If multiple matches found, returns:
            {"multiple_matches": True, "matches": [...], "message": "..."}

        Raises:
            RuntimeError: If manifest not loaded
            ValueError: If resource not found

        Examples:
            analyze_impact("stg_customers") -> impact of changing staging model
            analyze_impact("jaffle_shop.orders", "source") -> impact of source change
            analyze_impact("raw_customers", "seed") -> impact of seed change
        """
        if not self._manifest:
            raise RuntimeError("Manifest not loaded. Call load() first.")

        # Get the resource (auto-detect if resource_type not specified)
        resource = self.get_resource_node(name, resource_type)

        # Handle multiple matches - return for LLM to process
        if resource.get("multiple_matches"):
            return resource

        # Extract unique_id for impact traversal
        unique_id = resource.get("unique_id")
        if not unique_id:
            raise ValueError(f"Resource '{name}' does not have a unique_id")

        # Get all downstream dependencies (no depth limit for impact)
        downstream = self.get_downstream_nodes(unique_id, max_depth=None)

        # Categorize by resource type
        models_affected: list[dict[str, Any]] = []
        tests_affected: list[dict[str, Any]] = []
        other_affected: list[dict[str, Any]] = []
        affected_by_distance: dict[str, list[dict[str, Any]]] = {}

        for dep in downstream:
            dep_type = str(dep["type"])
            distance = str(dep["distance"])

            # Group by distance
            if distance not in affected_by_distance:
                affected_by_distance[distance] = []
            affected_by_distance[distance].append(dep)

            # Categorize by type
            if dep_type == "model":
                models_affected.append(dep)
            elif dep_type == "test":
                tests_affected.append(dep)
            else:
                other_affected.append(dep)

        # Sort models by distance for better readability
        models_affected_sorted = sorted(models_affected, key=lambda x: (int(x["distance"]), str(x["name"])))

        # Build recommendation based on resource type
        resource_name = resource.get("name", name)
        current_resource_type = resource.get("resource_type")

        if current_resource_type == "source":
            # For sources, recommend running downstream models
            if len(models_affected) == 0:
                recommendation = f"dbt test -s source:{resource.get('source_name')}.{resource_name}"
            else:
                recommendation = f"dbt run -s {resource_name}+"
        elif current_resource_type == "seed":
            # For seeds, recommend seeding + downstream
            if len(models_affected) == 0:
                recommendation = f"dbt seed -s {resource_name} && dbt test -s {resource_name}"
            else:
                recommendation = f"dbt seed -s {resource_name} && dbt run -s {resource_name}+"
        else:
            # For models, snapshots, etc.
            if len(models_affected) == 0:
                recommendation = f"dbt run -s {resource_name}"
            else:
                recommendation = f"dbt run -s {resource_name}+"

        # Build result
        result: dict[str, Any] = {
            "resource": {
                "name": resource_name,
                "unique_id": unique_id,
                "resource_type": current_resource_type,
                "package_name": resource.get("package_name"),
            },
            "impact": {
                "models_affected": models_affected_sorted,
                "models_affected_count": len(models_affected),
                "tests_affected_count": len(tests_affected),
                "other_affected_count": len(other_affected),
                "total_affected": len(downstream),
            },
            "affected_by_distance": affected_by_distance,
            "recommendation": recommendation,
        }

        # Add helpful message based on impact size
        if len(models_affected) == 0:
            result["message"] = "No downstream models affected. Only this resource needs to be run/tested."
        elif len(models_affected) <= 3:
            result["message"] = f"Low impact: {len(models_affected)} downstream model(s) affected."
        elif len(models_affected) <= 10:
            result["message"] = f"Medium impact: {len(models_affected)} downstream models affected."
        else:
            result["message"] = f"High impact: {len(models_affected)} downstream models affected. Consider incremental changes."

        return result

    def get_downstream_nodes(self, unique_id: str, max_depth: int | None = None, current_depth: int = 0) -> list[dict[str, Any]]:
        """Get all downstream dependents of a node recursively.

        Args:
            unique_id: The unique identifier of the node
            max_depth: Maximum depth to traverse (None for unlimited)
            current_depth: Current recursion depth (internal use)

        Returns:
            List of dictionaries with downstream node info:
            {"unique_id": str, "name": str, "type": str, "distance": int}
        """
        if not self._manifest:
            raise RuntimeError("Manifest not loaded. Call load() first.")

        if max_depth is not None and current_depth >= max_depth:
            return []

        child_map = self._manifest.get("child_map", {})
        children = child_map.get(unique_id, [])

        downstream: list[dict[str, Any]] = []
        seen: set[str] = set()

        for child_id in children:
            if child_id in seen:
                continue
            seen.add(child_id)

            node = self.get_node_by_unique_id(child_id)
            if node:
                resource_type = node.get("resource_type", "unknown")
                downstream.append(
                    {
                        "unique_id": child_id,
                        "name": node.get("name", ""),
                        "type": resource_type,
                        "distance": current_depth + 1,
                    }
                )

                # Recurse
                if max_depth is None or current_depth + 1 < max_depth:
                    grandchildren = self.get_downstream_nodes(child_id, max_depth, current_depth + 1)
                    for gc in grandchildren:
                        if gc["unique_id"] not in seen:
                            seen.add(str(gc["unique_id"]))
                            downstream.append(gc)

        return downstream
