from __future__ import annotations

import concurrent
import math
import warnings
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Optional

import numpy
from pandas import DataFrame
from tqdm.auto import tqdm

from .gds_arrow_client import GdsArrowClient
from .graph_constructor import GraphConstructor


class ArrowGraphConstructor(GraphConstructor):
    def __init__(
        self,
        database: str,
        graph_name: str,
        flight_client: GdsArrowClient,
        concurrency: int,
        undirected_relationship_types: Optional[list[str]],
        chunk_size: int = 10_000,
    ):
        self._database = database
        self._concurrency = concurrency
        self._graph_name = graph_name
        self._client = flight_client
        self._undirected_relationship_types = (
            [] if undirected_relationship_types is None else undirected_relationship_types
        )
        self._chunk_size = chunk_size
        self._min_batch_size = chunk_size * 10

    def run(self, node_dfs: list[DataFrame], relationship_dfs: list[DataFrame]) -> None:
        try:
            config: dict[str, Any] = {
                "name": self._graph_name,
                "database_name": self._database,
            }

            if self._undirected_relationship_types:
                config["undirected_relationship_types"] = self._undirected_relationship_types

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
            self._client.abort(self._graph_name)

            raise e

    def _partition_dfs(self, dfs: list[DataFrame]) -> list[DataFrame]:
        partitioned_dfs: list[DataFrame] = []

        for df in dfs:
            num_rows = df.shape[0]
            num_batches = math.ceil(num_rows / self._min_batch_size)

            # pandas 2.1.0 deprecates swapaxes, but numpy did not catch up yet.
            warnings.filterwarnings(
                "ignore",
                message=(
                    r"^'DataFrame.swapaxes' is deprecated and will be removed in a future version. "
                    + r"Please use 'DataFrame.transpose' instead.$"
                ),
            )
            partitioned_dfs += numpy.array_split(df, num_batches)  # type: ignore

        return partitioned_dfs

    def _send_dfs(self, dfs: list[DataFrame], entity_type: str) -> None:
        desc = "Uploading Nodes" if entity_type == "node" else "Uploading Relationships"
        pbar = tqdm(total=sum([df.shape[0] for df in dfs]), unit="Records", desc=desc)

        partitioned_dfs = self._partition_dfs(dfs)

        with ThreadPoolExecutor(self._concurrency) as executor:

            def run_upload(df: DataFrame) -> None:
                def progress_callback(rows: int) -> None:
                    pbar.update(rows)  # pbar would

                if entity_type == "node":
                    self._client.upload_nodes(
                        self._graph_name,
                        node_data=df,
                        batch_size=self._min_batch_size,
                        progress_callback=progress_callback,
                    )
                else:
                    self._client.upload_relationships(
                        self._graph_name,
                        relationship_data=df,
                        batch_size=self._min_batch_size,
                        progress_callback=progress_callback,
                    )
                pbar.refresh()

            futures = [executor.submit(run_upload, df) for df in partitioned_dfs]
            for future in concurrent.futures.as_completed(futures):
                if not future.exception():
                    continue
                raise future.exception()  # type: ignore
