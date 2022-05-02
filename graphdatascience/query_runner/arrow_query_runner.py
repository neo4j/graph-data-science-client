import json
from typing import Any, Dict, Optional, Tuple

import pyarrow.flight as flight
from pandas.core.frame import DataFrame

from .query_runner import QueryRunner


class ArrowQueryRunner(QueryRunner):
    def __init__(self, uri: str, fallback_query_runner: QueryRunner, auth: Optional[Tuple[str, str]] = None):
        self.fallback_query_runner = fallback_query_runner

        host, port_string = uri.split(":")

        location = flight.Location.for_grpc_tcp(host, int(port_string))

        self._flight_client = flight.FlightClient(location)
        self._flight_options = flight.FlightCallOptions()

        if auth:
            username, password = auth
            (header, token) = self._flight_client.authenticate_basic_token(username, password)
            if header:
                self._flight_options = flight.FlightCallOptions(headers=[(header, token)])

    def run_query(self, query: str, params: Dict[str, Any] = {}) -> DataFrame:
        if "gds.graph.streamNodeProperty" in query:
            graph_name = params["graph_name"]
            property_name = params["properties"]
            return self._run_arrow_property_get(graph_name, "NODE", property_name)
        elif "gds.graph.streamRelationshipProperty" in query:
            graph_name = params["graph_name"]
            property_name = params["properties"]
            return self._run_arrow_property_get(graph_name, "RELATIONSHIP", property_name)

        return self.fallback_query_runner.run_query(query, params)

    def set_database(self, db: str) -> None:
        self.fallback_query_runner.set_database(db)

    def _run_arrow_property_get(self, graph_name: str, entity_type: str, property_name: str) -> DataFrame:
        payload = {
            "database_name": "neo4j",
            "graph_name": graph_name,
            "entity_type": entity_type,
            "property_name": property_name,
        }
        ticket = flight.Ticket(json.dumps(payload).encode("utf-8"))

        result: DataFrame = self._flight_client.do_get(ticket, self._flight_options).read_pandas()

        return result
