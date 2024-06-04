from __future__ import annotations

import concurrent
import math
import warnings
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, NoReturn, Optional

import numpy
from pandas import DataFrame
from pyarrow import Table
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
        undirected_relationship_types: Optional[List[str]],
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

    def run(self, node_dfs: List[DataFrame], relationship_dfs: List[DataFrame]) -> None:
        try:
            config: Dict[str, Any] = {
                "name": self._graph_name,
                "database_name": self._database,
            }

            if self._undirected_relationship_types:
                config["undirected_relationship_types"] = self._undirected_relationship_types

            self._client.send_action(
                "CREATE_GRAPH",
                config,
            )

            self._send_dfs(node_dfs, "node")

            self._client.send_action("NODE_LOAD_DONE", {"name": self._graph_name})

            self._send_dfs(relationship_dfs, "relationship")

            self._client.send_action("RELATIONSHIP_LOAD_DONE", {"name": self._graph_name})
        except (Exception, KeyboardInterrupt) as e:
            self._client.send_action("ABORT", {"name": self._graph_name})

            raise e

    def _partition_dfs(self, dfs: List[DataFrame]) -> List[DataFrame]:
        partitioned_dfs: List[DataFrame] = []

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

    def _send_df(self, df: DataFrame, entity_type: str, pbar: tqdm[NoReturn]) -> None:
        table = Table.from_pandas(df)
        batches = table.to_batches(self._chunk_size)
        flight_descriptor = {"name": self._graph_name, "entity_type": entity_type}

        writer, _ = self._client.start_put(flight_descriptor, table.schema)

        with writer:
            # Write table in chunks
            for partition in batches:
                writer.write_batch(partition)
                pbar.update(partition.num_rows)
        # Force a refresh to avoid the progress bar getting stuck at 0%
        pbar.refresh()

    def _send_dfs(self, dfs: List[DataFrame], entity_type: str) -> None:
        desc = "Uploading Nodes" if entity_type == "node" else "Uploading Relationships"
        pbar = tqdm(total=sum([df.shape[0] for df in dfs]), unit="Records", desc=desc)

        partitioned_dfs = self._partition_dfs(dfs)

        with ThreadPoolExecutor(self._concurrency) as executor:
            futures = [executor.submit(self._send_df, df, entity_type, pbar) for df in partitioned_dfs]

            for future in concurrent.futures.as_completed(futures):
                if not future.exception():
                    continue
                raise future.exception()  # type: ignore
