from __future__ import annotations

from typing import Any

from graphdatascience.call_parameters import CallParameters
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.catalog.graph_export_endpoints import (
    GraphExportCsvResult,
    GraphExportEndpoints,
    GraphExportResult,
)
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter
from graphdatascience.query_runner.query_runner import QueryRunner


class GraphExportCypherEndpoints(GraphExportEndpoints):
    def __init__(self, cypher_runner: QueryRunner):
        self._cypher_runner = cypher_runner

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
        config = ConfigConverter.convert_to_gds_config(
            db_name=db_name,
            additional_node_properties=additional_node_properties,
            batch_size=batch_size,
            db_format=db_format,
            default_relationship_type=default_relationship_type,
            enable_debug_log=enable_debug_log,
            job_id=job_id,
            log_progress=log_progress,
            sudo=sudo,
            username=username,
            write_concurrency=write_concurrency,
        )

        params = CallParameters(
            graph_name=G.name(),
            config=config,
        )
        params.ensure_job_id_in_config()

        result = self._cypher_runner.call_procedure(
            endpoint="gds.graph.export", params=params, logging=log_progress
        ).iloc[0]
        return GraphExportResult(**result)

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
        config = ConfigConverter.convert_to_gds_config(
            export_name=export_name,
            additional_node_properties=additional_node_properties,
            batch_size=batch_size,
            default_relationship_type=default_relationship_type,
            include_meta_data=include_meta_data,
            log_progress=log_progress,
            sudo=sudo,
            use_label_mapping=use_label_mapping,
            username=username,
            write_concurrency=write_concurrency,
        )

        params = CallParameters(
            graph_name=G.name(),
            config=config,
        )

        result = self._cypher_runner.call_procedure(
            endpoint="gds.graph.export.csv", params=params, logging=log_progress
        ).iloc[0]
        return GraphExportCsvResult(**result)
