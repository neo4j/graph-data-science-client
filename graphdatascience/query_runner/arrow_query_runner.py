import json
import math
from concurrent.futures import ThreadPoolExecutor, wait
from typing import Any, Dict, List, Optional, Tuple

import pyarrow.flight as flight
from pandas.core.frame import DataFrame
from pyarrow import Table

from .graph_constructor import GraphConstructor
from .query_runner import QueryRunner


class ArrowGraphConstructor(GraphConstructor):
    def __init__(
        self,
        query_runner: QueryRunner,
        graph_name: str,
        flight_client: flight.FlightClient,
        flight_options: flight.FlightCallOptions,
        concurrency: int,
        chunk_size: int = 10_000,
    ):
        self._query_runner = query_runner
        self._concurrency = concurrency
        self._graph_name = graph_name
        self._client = flight_client
        self._flight_options = flight_options
        self._chunk_size = chunk_size
        self._min_batch_size = chunk_size * 10

    def run(self, node_dfs: List[DataFrame], relationship_dfs: List[DataFrame]) -> None:
        try:
            self._send_action(
                "CREATE_GRAPH", {"name": self._graph_name, "database_name": self._query_runner.database()}
            )

            partitioned_node_dfs = self._partition_dfs(node_dfs)
            with ThreadPoolExecutor(self._concurrency) as executor:
                futures = [executor.submit(self._send_df, df, "node") for df in partitioned_node_dfs]

                wait(futures)

                for future in futures:
                    if not future.exception():
                        continue
                    raise future.exception()  # type: ignore

            self._send_action("NODE_LOAD_DONE", {"name": self._graph_name})

            partitioned_rel_dfs = self._partition_dfs(relationship_dfs)
            with ThreadPoolExecutor(self._concurrency) as executor:
                futures = [executor.submit(self._send_df, df, "relationship") for df in partitioned_rel_dfs]

                wait(futures)

                for future in futures:
                    if not future.exception():
                        continue
                    raise future.exception()  # type: ignore

            self._send_action("RELATIONSHIP_LOAD_DONE", {"name": self._graph_name})
        except Exception as e:
            self._send_action("ABORT", {"name": self._graph_name})
            raise e

    def _partition_dfs(self, dfs: List[DataFrame]) -> List[DataFrame]:
        partitioned_dfs = []

        for df in dfs:
            num_rows = df.shape[0]

            if num_rows <= self._min_batch_size:
                partitioned_dfs.append(df)
                continue

            num_batches = math.ceil(num_rows / self._min_batch_size)
            for i in range(num_batches):
                row_offset = i * self._min_batch_size
                partitioned_dfs.append(
                    df.iloc[row_offset : min(num_rows, row_offset + self._min_batch_size), :]  # noqa: E203
                )

        return partitioned_dfs

    def _send_action(self, action_type: str, meta_data: Dict[str, str]) -> None:
        result = self._client.do_action(
            flight.Action(action_type, json.dumps(meta_data).encode("utf-8")), self._flight_options
        )

        json.loads(next(result).body.to_pybytes().decode())

    def _send_df(self, df: DataFrame, entity_type: str) -> None:
        table = Table.from_pandas(df)
        flight_descriptor = {"name": self._graph_name, "entity_type": entity_type}

        # Write schema
        upload_descriptor = flight.FlightDescriptor.for_command(json.dumps(flight_descriptor).encode("utf-8"))

        writer, _ = self._client.do_put(upload_descriptor, table.schema, self._flight_options)

        with writer:
            # Write table in chunks
            writer.write_table(table, max_chunksize=self._chunk_size)


class ArrowQueryRunner(QueryRunner):
    def __init__(
        self,
        uri: str,
        fallback_query_runner: QueryRunner,
        auth: Optional[Tuple[str, str]] = None,
        encrypted: bool = False,
        disable_server_verification: bool = False,
    ):
        self.fallback_query_runner = fallback_query_runner

        host, port_string = uri.split(":")

        location = (
            flight.Location.for_grpc_tls(host, int(port_string))
            if encrypted
            else flight.Location.for_grpc_tcp(host, int(port_string))
        )

        self._flight_client = flight.FlightClient(location, disable_server_verification=disable_server_verification)
        self._flight_options = flight.FlightCallOptions()

        if auth:
            username, password = auth
            header, token = self._flight_client.authenticate_basic_token(username, password)
            if header:
                self._flight_options = flight.FlightCallOptions(headers=[(header, token)])

    def run_query(self, query: str, params: Dict[str, Any] = {}) -> DataFrame:
        if "gds.graph.streamNodeProperty" in query:
            graph_name = params["graph_name"]
            property_name = params["properties"]
            return self._run_arrow_property_get(
                graph_name, "gds.graph.streamNodeProperty", {"nodeProperty": property_name}
            )
        elif "gds.graph.streamRelationshipProperty" in query:
            graph_name = params["graph_name"]
            property_name = params["properties"]
            return self._run_arrow_property_get(
                graph_name, "gds.graph.streamRelationshipProperty", {"relationshipProperty": property_name}
            )

        return self.fallback_query_runner.run_query(query, params)

    def set_database(self, db: str) -> None:
        self.fallback_query_runner.set_database(db)

    def database(self) -> str:
        return self.fallback_query_runner.database()

    def _run_arrow_property_get(self, graph_name: str, procedure_name: str, configuration: Dict[str, Any]) -> DataFrame:
        payload = {
            "database_name": "neo4j",
            "graph_name": graph_name,
            "procedure_name": procedure_name,
            "configuration": configuration,
        }
        ticket = flight.Ticket(json.dumps(payload).encode("utf-8"))

        result: DataFrame = self._flight_client.do_get(ticket, self._flight_options).read_pandas()

        return result

    def create_graph_constructor(self, graph_name: str, concurrency: int) -> GraphConstructor:
        return ArrowGraphConstructor(self, graph_name, self._flight_client, self._flight_options, concurrency)
