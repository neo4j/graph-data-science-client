from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.base_result import BaseResult


class GraphExportEndpoints(ABC):
    @abstractmethod
    def __call__(
        self,
        G: Graph,
        db_name: str,
        *,
        additional_node_properties: str | list[str] | dict[str, Any] | None = None,
        batch_size: int = 10000,
        db_format: str = "block",
        default_relationship_type: str = "__ALL__",
        enable_debug_log: bool = False,
        job_id: str | None = None,
        log_progress: bool = True,
        sudo: bool = False,
        username: str | None = None,
        write_concurrency: int | None = None,
    ) -> GraphExportResult:
        """
        Export a graph from the graph catalog to a new Neo4j database.

        The new database must not exist yet. After the export, the database
        can be brought online using `CREATE DATABASE <db_name>`.

        Parameters
        ----------
        G
            Graph object to use
        db_name
            The name of the new database to export the graph to.
        additional_node_properties
            Allows for exporting additional node properties from the original graph backing the in-memory graph.
        batch_size
            Number of entities to process in each batch.
        db_format
            Database format. Valid values are `block`, `standard`, `aligned` and `high_limit`.
        default_relationship_type
            Relationship type used for `*` relationship projections.
        enable_debug_log
            Prints debug information to the log files.
        job_id
            Identifier for the computation.
        log_progress
            Display progress logging.
        sudo
            Disable the memory guard.
        username
            As an administrator, impersonate a different user for accessing their graphs.
        write_concurrency
            Number of concurrent threads to use for writing.

        Returns
        -------
        GraphExportResult
            The result of the export, including the name of the new database and counts of the exported entities.
        """
        pass

    @abstractmethod
    def csv(
        self,
        G: Graph,
        export_name: str,
        *,
        additional_node_properties: str | list[str] | dict[str, Any] | None = None,
        batch_size: int = 10000,
        default_relationship_type: str = "__ALL__",
        include_meta_data: bool = False,
        log_progress: bool = True,
        sudo: bool = False,
        use_label_mapping: bool = False,
        username: str | None = None,
        write_concurrency: int | None = None,
    ) -> GraphExportCsvResult:
        """
        Export a graph from the graph catalog to CSV files.

        The files are written to a subdirectory of the export location
        configured on the server via `gds.export.location`.

        Parameters
        ----------
        G
            Graph object to use
        export_name
            Name of the directory to which the CSV files are exported, relative to the configured export location.
        additional_node_properties
            Allows for exporting additional node properties from the original graph backing the in-memory graph.
        batch_size
            Number of entities to process in each batch.
        default_relationship_type
            Relationship type used for `*` relationship projections.
        include_meta_data
            Include meta data files such as node labels and relationship types in the export.
        log_progress
            Display progress logging.
        sudo
            Disable the memory guard.
        use_label_mapping
            Map node labels to numeric identifiers in the exported files.
        username
            As an administrator, impersonate a different user for accessing their graphs.
        write_concurrency
            Number of concurrent threads to use for writing.

        Returns
        -------
        GraphExportCsvResult
            The result of the export, including the export name and counts of the exported entities.
        """
        pass


class GraphExportResult(BaseResult):
    db_name: str
    graph_name: str
    node_count: int
    node_property_count: int
    relationship_count: int
    relationship_property_count: int
    relationship_type_count: int
    write_millis: int


class GraphExportCsvResult(BaseResult):
    export_name: str
    graph_name: str
    node_count: int
    node_property_count: int
    relationship_count: int
    relationship_property_count: int
    relationship_type_count: int
    write_millis: int
