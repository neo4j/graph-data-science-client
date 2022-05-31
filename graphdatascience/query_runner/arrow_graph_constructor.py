import json
import math
from concurrent.futures import ThreadPoolExecutor, wait
from typing import Dict, List

import numpy
import pyarrow.flight as flight
from pandas.core.frame import DataFrame
from pyarrow import Table

from .graph_constructor import GraphConstructor


class ArrowGraphConstructor(GraphConstructor):
    def __init__(
        self,
        database: str,
        graph_name: str,
        flight_client: flight.FlightClient,
        concurrency: int,
        chunk_size: int = 10_000,
    ):
        self._database = database
        self._concurrency = concurrency
        self._graph_name = graph_name
        self._client = flight_client
        self._chunk_size = chunk_size
        self._min_batch_size = chunk_size * 10

    def run(self, node_dfs: List[DataFrame], relationship_dfs: List[DataFrame]) -> None:
        try:
            self._send_action("CREATE_GRAPH", {"name": self._graph_name, "database_name": self._database})

            self._send_dfs(node_dfs, "node")

            self._send_action("NODE_LOAD_DONE", {"name": self._graph_name})

            self._send_dfs(relationship_dfs, "relationship")

            self._send_action("RELATIONSHIP_LOAD_DONE", {"name": self._graph_name})
        except Exception as e:
            self._send_action("ABORT", {"name": self._graph_name})

            raise e

    def _partition_dfs(self, dfs: List[DataFrame]) -> List[DataFrame]:
        partitioned_dfs: List[DataFrame] = []

        for df in dfs:
            num_rows = df.shape[0]
            num_batches = math.ceil(num_rows / self._min_batch_size)
            partitioned_dfs += numpy.array_split(df, num_batches)  # type: ignore

        return partitioned_dfs

    def _send_action(self, action_type: str, meta_data: Dict[str, str]) -> None:
        result = self._client.do_action(flight.Action(action_type, json.dumps(meta_data).encode("utf-8")))

        json.loads(next(result).body.to_pybytes().decode())

    def _send_df(self, df: DataFrame, entity_type: str) -> None:
        table = Table.from_pandas(df)
        flight_descriptor = {"name": self._graph_name, "entity_type": entity_type}

        # Write schema
        upload_descriptor = flight.FlightDescriptor.for_command(json.dumps(flight_descriptor).encode("utf-8"))

        writer, _ = self._client.do_put(upload_descriptor, table.schema)

        with writer:
            # Write table in chunks
            writer.write_table(table, max_chunksize=self._chunk_size)

    def _send_dfs(self, dfs: List[DataFrame], entity_type: str) -> None:
        partitioned_dfs = self._partition_dfs(dfs)

        with ThreadPoolExecutor(self._concurrency) as executor:
            futures = [executor.submit(self._send_df, df, entity_type) for df in partitioned_dfs]

            wait(futures)

            for future in futures:
                if not future.exception():
                    continue
                raise future.exception()  # type: ignore
