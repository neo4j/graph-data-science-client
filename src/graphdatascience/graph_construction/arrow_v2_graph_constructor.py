from __future__ import annotations

import logging

from pandas import DataFrame

from graphdatascience.arrow_client.v2.gds_arrow_client import GdsArrowClient

from ..arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from ..arrow_client.v2.job_client import JobClient
from ..query_runner.progress.progress_bar import NoOpProgressBar, ProgressBar, TqdmProgressBar
from ..query_runner.termination_flag import TerminationFlag
from .graph_constructor import GraphConstructor


class ArrowV2GraphConstructor(GraphConstructor):
    def __init__(
        self,
        authenticated_arrow_client: AuthenticatedArrowClient,
        graph_name: str,
        concurrency: int | None = None,
        undirected_relationship_types: list[str] | None = None,
        inverse_indexed_relationship_types: list[str] | None = None,
        batch_size: int = 100_000,
        show_progress: bool = True,
    ):
        self._arrow_client = authenticated_arrow_client
        self._graph_name = graph_name
        self._concurrency = concurrency
        self._client = authenticated_arrow_client
        self._undirected_relationship_types = undirected_relationship_types or []
        self._inverse_indexed_relationship_types = inverse_indexed_relationship_types or []
        self._batch_size = batch_size
        self._show_progress = show_progress
        self._logger = logging.getLogger()

    def run(self, node_dfs: list[DataFrame], relationship_dfs: list[DataFrame]) -> None:
        gds_arrow_client = GdsArrowClient(self._arrow_client)
        job_client = JobClient()
        termination_flag = TerminationFlag.create()

        if self._show_progress:
            progress_bar: ProgressBar = TqdmProgressBar(task_name="Constructing graph", relative_progress=0.0)
        else:
            progress_bar = NoOpProgressBar()

        with progress_bar:
            create_job_id: str = gds_arrow_client.create_graph(
                graph_name=self._graph_name,
                undirected_relationship_types=self._undirected_relationship_types,
                inverse_indexed_relationship_types=self._inverse_indexed_relationship_types,
                concurrency=self._concurrency,
            )
            node_count = node_dfs.shape[0] if isinstance(node_dfs, DataFrame) else sum(df.shape[0] for df in node_dfs)
            if isinstance(relationship_dfs, DataFrame):
                rel_count = relationship_dfs.shape[0]
            elif relationship_dfs is None:
                rel_count = 0
            else:
                rel_count = sum(df.shape[0] for df in relationship_dfs)
            total_count = node_count + rel_count

            gds_arrow_client.upload_nodes(
                create_job_id,
                node_dfs,
                batch_size=self._batch_size,
                progress_callback=lambda rows_imported: progress_bar.update(
                    sub_tasks_description="Uploading nodes", progress=rows_imported / total_count, status="Running"
                ),
                termination_flag=termination_flag,
            )

            gds_arrow_client.node_load_done(create_job_id)

            # skipping progress bar here as we have our own for the overall process
            job_client.wait_for_job(
                self._arrow_client,
                create_job_id,
                expected_status="RELATIONSHIP_LOADING",
                termination_flag=termination_flag,
                show_progress=False,
            )

            if rel_count > 0:
                gds_arrow_client.upload_relationships(
                    create_job_id,
                    relationship_dfs,
                    batch_size=self._batch_size,
                    progress_callback=lambda rows_imported: progress_bar.update(
                        sub_tasks_description="Uploading relationships",
                        progress=rows_imported / total_count,
                        status="Running",
                    ),
                    termination_flag=termination_flag,
                )

            gds_arrow_client.relationship_load_done(create_job_id)

        # will produce a second progress bar to show graph construction on the server side
        job_client.wait_for_job(
            self._arrow_client, create_job_id, termination_flag=termination_flag, show_progress=True
        )
