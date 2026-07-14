from __future__ import annotations

from typing import Any

from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.catalog.graph_export_endpoints import (
    GraphExportCsvResult,
    GraphExportEndpoints,
    GraphExportResult,
)

_NOT_IMPLEMENTED_MESSAGE = "Graph export is not available in AGA sessions."


class GraphExportArrowEndpoints(GraphExportEndpoints):
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
        raise NotImplementedError(_NOT_IMPLEMENTED_MESSAGE)

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
        raise NotImplementedError(_NOT_IMPLEMENTED_MESSAGE)
