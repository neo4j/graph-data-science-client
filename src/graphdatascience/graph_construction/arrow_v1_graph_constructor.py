from __future__ import annotations

import concurrent
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from pandas import DataFrame
from tqdm.auto import tqdm

from graphdatascience.arrow_client.v1.gds_arrow_client import GdsArrowClient

from .graph_constructor import GraphConstructor


class ArrowV1GraphConstructor(GraphConstructor):
    def __init__(
        self,
        database: str,
        graph_name: str,
        flight_client: GdsArrowClient,
        concurrency: int | None = None,
        undirected_relationship_types: list[str] | None = None,
        inverse_indexed_relationship_types: list[str] | None = None,
        batch_size: int = 100_000,
    ):
        self._database = database
        self._concurrency = concurrency
        self._graph_name = graph_name
        self._client = flight_client
        self._undirected_relationship_types = undirected_relationship_types or []
        self._inverse_indexed_relationship_types = inverse_indexed_relationship_types or []
        self._batch_size = batch_size
        self._min_partition_size = batch_size * 10
        self._logger = logging.getLogger()

    def run(self, node_dfs: list[DataFrame], relationship_dfs: list[DataFrame]) -> None:
        try:
            config: dict[str, Any] = {
                "name": self._graph_name,
                "database_name": self._database,
            }

            if self._undirected_relationship_types:
                config["undirected_relationship_types"] = self._undirected_relationship_types

            if self._inverse_indexed_relationship_types:
                config["inverse_indexed_relationship_types"] = self._inverse_indexed_relationship_types

            self._client.create_graph(
                graph_name=self._graph_name,
                database=self._database,
                undirected_relationship_types=self._undirected_relationship_types,
                inverse_indexed_relationship_types=None,
                concurrency=self._concurrency,
            )

            self._send_dfs(node_dfs, "node")

            self._client.node_load_done(self._graph_name)

            self._send_dfs(relationship_dfs, "relationship")

            self._client.relationship_load_done(self._graph_name)
        except (Exception, KeyboardInterrupt) as e:
            try:
                self._client.abort(self._graph_name)
            except Exception as abort_exception:
                if "No arrow process" not in str(abort_exception):
                    self._logger.warning(f"error aborting graph creation: {abort_exception}")
            raise e

    def _partition_dfs(self, dfs: list[DataFrame]) -> list[DataFrame]:
        partitioned_dfs: list[DataFrame] = []

        for df in dfs:
            i = 0
            while i < len(df):
                partitioned_dfs.append(df.iloc[i : i + self._min_partition_size].copy())
                i += self._min_partition_size

        return partitioned_dfs

    def _send_dfs(self, dfs: list[DataFrame], entity_type: str) -> None:
        desc = "Uploading Nodes" if entity_type == "node" else "Uploading Relationships"
        pbar = tqdm(total=sum([df.shape[0] for df in dfs]), unit="Records", desc=desc)

        partitioned_dfs = self._partition_dfs(dfs)

        with ThreadPoolExecutor(self._concurrency) as executor:

            def run_upload(df: DataFrame) -> None:
                def progress_callback(num_rows: int) -> None:
                    pbar.update(num_rows)

                if entity_type == "node":
                    self._client.upload_nodes(
                        self._graph_name,
                        node_data=df,
                        batch_size=self._batch_size,
                        progress_callback=progress_callback,
                    )
                else:
                    self._client.upload_relationships(
                        self._graph_name,
                        relationship_data=df,
                        batch_size=self._batch_size,
                        progress_callback=progress_callback,
                    )
                pbar.refresh()

            futures = [executor.submit(run_upload, df) for df in partitioned_dfs]
            for future in concurrent.futures.as_completed(futures):
                if not future.exception():
                    continue
                raise future.exception()  # type: ignore
