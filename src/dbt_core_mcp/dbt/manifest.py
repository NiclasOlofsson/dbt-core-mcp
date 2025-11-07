"""
DBT Manifest Loader.

Reads and parses DBT's manifest.json file to provide structured access
to models, sources, tests, and other DBT entities.
"""

import json
import logging
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class DbtModel:
    """Represents a DBT model from the manifest."""

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
    """Represents a DBT source from the manifest."""

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
        self._manifest: dict | None = None

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

    def get_model_node(self, name: str) -> dict:
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

        nodes = self._manifest.get("nodes", {})
        for unique_id, node in nodes.items():
            if node.get("resource_type") == "model" and node.get("name") == name:
                return dict(node)  # type cast to satisfy mypy

        raise ValueError(f"Model '{name}' not found in manifest")

    def get_source_node(self, source_name: str, table_name: str) -> dict:
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

        sources = self._manifest.get("sources", {})
        for unique_id, source in sources.items():
            if source.get("source_name") == source_name and source.get("name") == table_name:
                return dict(source)  # type cast to satisfy mypy

        raise ValueError(f"Source '{source_name}.{table_name}' not found in manifest")

    def get_project_info(self) -> dict:
        """
        Get high-level project information from the manifest.

        Returns:
            Dictionary with project metadata
        """
        if not self._manifest:
            raise RuntimeError("Manifest not loaded. Call load() first.")

        metadata = self._manifest.get("metadata", {})

        return {
            "project_name": metadata.get("project_name", ""),
            "dbt_version": metadata.get("dbt_version", ""),
            "adapter_type": metadata.get("adapter_type", ""),
            "generated_at": metadata.get("generated_at", ""),
            "model_count": len(self.get_models()),
            "source_count": len(self.get_sources()),
        }
