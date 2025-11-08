"""
DBT Manifest Loader.

Reads and parses DBT's manifest.json file to provide structured access
to models, sources, tests, and other DBT entities.
"""

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

    def load(self) -> None:
        """Load the manifest from disk."""
        if not self.manifest_path.exists():
            raise FileNotFoundError(f"Manifest not found: {self.manifest_path}")

        logger.debug(f"Loading manifest from {self.manifest_path}")
        with open(self.manifest_path, "r") as f:
            self._manifest = json.load(f)
        logger.info("Manifest loaded successfully")

    def get_models(self) -> list[DbtModel]:
        """
        Get all models from the manifest.

        Returns:
            List of DbtModel instances
        """
        if not self._manifest:
            raise RuntimeError("Manifest not loaded. Call load() first.")

        models = []
        nodes = self._manifest.get("nodes", {})

        for unique_id, node in nodes.items():
            if node.get("resource_type") == "model":
                models.append(
                    DbtModel(
                        name=node.get("name", ""),
                        unique_id=unique_id,
                        resource_type=node.get("resource_type", ""),
                        schema=node.get("schema", ""),
                        database=node.get("database", ""),
                        alias=node.get("alias", ""),
                        description=node.get("description", ""),
                        materialization=node.get("config", {}).get("materialized", ""),
                        tags=node.get("tags", []),
                        depends_on=node.get("depends_on", {}).get("nodes", []),
                        package_name=node.get("package_name", ""),
                        original_file_path=node.get("original_file_path", ""),
                    )
                )

        logger.debug(f"Found {len(models)} models in manifest")
        return models

    def get_sources(self) -> list[DbtSource]:
        """
        Get all sources from the manifest.

        Returns:
            List of DbtSource instances
        """
        if not self._manifest:
            raise RuntimeError("Manifest not loaded. Call load() first.")

        sources = []
        source_nodes = self._manifest.get("sources", {})

        for unique_id, node in source_nodes.items():
            sources.append(
                DbtSource(
                    name=node.get("name", ""),
                    unique_id=unique_id,
                    source_name=node.get("source_name", ""),
                    schema=node.get("schema", ""),
                    database=node.get("database", ""),
                    identifier=node.get("identifier", ""),
                    description=node.get("description", ""),
                    tags=node.get("tags", []),
                    package_name=node.get("package_name", ""),
                )
            )

        logger.debug(f"Found {len(sources)} sources in manifest")
        return sources

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

    def get_model_by_name(self, name: str) -> DbtModel | None:
        """
        Get a specific model by name.

        Args:
            name: Model name

        Returns:
            DbtModel instance or None if not found
        """
        models = self.get_models()
        for model in models:
            if model.name == name:
                return model
        return None

    def get_model_node(self, name: str) -> dict[str, Any]:
        """
        Get the raw manifest node for a model by name.

        Returns the complete node dictionary from the manifest with all ~40 fields,
        including columns, raw_code, compiled_path, config, meta, etc.

        Args:
            name: Model name

        Returns:
            Complete manifest node dictionary

        Raises:
            RuntimeError: If manifest not loaded
            ValueError: If model not found
        """
        if not self._manifest:
            raise RuntimeError("Manifest not loaded. Call load() first.")

        nodes: dict[str, Any] = self._manifest.get("nodes", {})  # type: ignore[assignment]
        for _, node in nodes.items():
            if isinstance(node, dict) and node.get("resource_type") == "model" and node.get("name") == name:
                return dict(node)  # type cast to satisfy type checker

        raise ValueError(f"Model '{name}' not found in manifest")

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
        node = self.get_model_node(name)  # Will raise ValueError if not found
        return node.get("compiled_code")

    def get_source_node(self, source_name: str, table_name: str) -> dict[str, Any]:
        """
        Get the raw manifest node for a source by source name and table name.

        Returns the complete source dictionary from the manifest with all fields,
        including columns, freshness, config, meta, etc.

        Args:
            source_name: Source name (e.g., 'jaffle_shop')
            table_name: Table name within the source (e.g., 'customers')

        Returns:
            Complete manifest source dictionary

        Raises:
            RuntimeError: If manifest not loaded
            ValueError: If source not found
        """
        if not self._manifest:
            raise RuntimeError("Manifest not loaded. Call load() first.")

        sources: dict[str, Any] = self._manifest.get("sources", {})  # type: ignore[assignment]
        for _, source in sources.items():
            if isinstance(source, dict) and source.get("source_name") == source_name and source.get("name") == table_name:
                return dict(source)  # type cast to satisfy type checker

        raise ValueError(f"Source '{source_name}.{table_name}' not found in manifest")

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
                try:
                    source = self.get_source_node(parts[0], parts[1])
                    matches.append(source)
                except ValueError:
                    pass  # Not a source, continue searching

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
            type_hint = f" of type '{resource_type}'" if resource_type else ""
            raise ValueError(f"Resource '{name}'{type_hint} not found in manifest")
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

    def get_project_info(self) -> dict[str, Any]:
        """
        Get high-level project information from the manifest.

        Returns:
            Dictionary with project metadata
        """
        if not self._manifest:
            raise RuntimeError("Manifest not loaded. Call load() first.")

        metadata: dict[str, Any] = self._manifest.get("metadata", {})  # type: ignore[assignment]

        return {
            "project_name": metadata.get("project_name", ""),
            "dbt_version": metadata.get("dbt_version", ""),
            "adapter_type": metadata.get("adapter_type", ""),
            "generated_at": metadata.get("generated_at", ""),
            "model_count": len(self.get_models()),
            "source_count": len(self.get_sources()),
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
