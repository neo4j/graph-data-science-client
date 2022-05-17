import json
from typing import Any, Dict, Optional, Tuple

import pyarrow.flight as flight
from pandas.core.frame import DataFrame

from .arrow_graph_constructor import ArrowGraphConstructor
from .graph_constructor import GraphConstructor
from .query_runner import QueryRunner


class ArrowQueryRunner(QueryRunner):
    def __init__(
        self,
        uri: str,
        fallback_query_runner: QueryRunner,
        auth: Optional[Tuple[str, str]] = None,
        encrypted: bool = False,
        disable_server_verification: bool = False,
    ):
        self._fallback_query_runner = fallback_query_runner

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
            node_labels = params["entities"]
            return self._run_arrow_property_get(
                graph_name, "gds.graph.streamNodeProperty", {"node_property": property_name, "node_labels": node_labels}
            )
        elif "gds.graph.streamNodeProperties" in query:
            graph_name = params["graph_name"]
            property_names = params["properties"]
            node_labels = params["entities"]
            return self._run_arrow_property_get(
                graph_name,
                "gds.graph.streamNodeProperties",
                {"node_properties": property_names, "node_labels": node_labels},
            )
        elif "gds.graph.streamRelationshipProperty" in query:
            graph_name = params["graph_name"]
            property_name = params["properties"]
            relationship_types = params["entities"]
            return self._run_arrow_property_get(
                graph_name,
                "gds.graph.streamRelationshipProperty",
                {"relationship_property": property_name, "relationship_types": relationship_types},
            )
        elif "gds.graph.streamRelationshipProperties" in query:
            graph_name = params["graph_name"]
            property_names = params["properties"]
            relationship_types = params["entities"]
            return self._run_arrow_property_get(
                graph_name,
                "gds.graph.streamRelationshipProperties",
                {"relationship_properties": property_names, "relationship_types": relationship_types},
            )

        return self._fallback_query_runner.run_query(query, params)

    def run_query_with_logging(self, query: str, params: Dict[str, Any] = {}) -> DataFrame:
        # For now there's no logging support with Arrow queries.
        return self._fallback_query_runner.run_query_with_logging(query, params)

    def set_database(self, db: str) -> None:
        self._fallback_query_runner.set_database(db)

    def database(self) -> str:
        return self._fallback_query_runner.database()

    def close(self) -> None:
        self._fallback_query_runner.close()

    def _run_arrow_property_get(self, graph_name: str, procedure_name: str, configuration: Dict[str, Any]) -> DataFrame:
        payload = {
            "database_name": self.database(),
            "graph_name": graph_name,
            "procedure_name": procedure_name,
            "configuration": configuration,
        }
        ticket = flight.Ticket(json.dumps(payload).encode("utf-8"))

        result: DataFrame = self._flight_client.do_get(ticket, self._flight_options).read_pandas()

        return result

    def create_graph_constructor(self, graph_name: str, concurrency: int) -> GraphConstructor:
        return ArrowGraphConstructor(self, graph_name, self._flight_client, self._flight_options, concurrency)
