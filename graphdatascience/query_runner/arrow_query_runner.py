import base64
import json
import time
from typing import Any, Dict, Optional, Tuple

import pyarrow.flight as flight
from pandas.core.frame import DataFrame
from pyarrow.flight import ClientMiddleware, ClientMiddlewareFactory

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

        client_options: Dict[str, Any] = {"disable_server_verification": disable_server_verification}
        if auth:
            client_options["middleware"] = [AuthFactory(auth)]

        self._flight_client = flight.FlightClient(location, **client_options)

    def run_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> DataFrame:
        if params is None:
            params = {}

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

    def run_query_with_logging(self, query: str, params: Optional[Dict[str, Any]] = None) -> DataFrame:
        # For now there's no logging support with Arrow queries.
        if params is None:
            params = {}

        return self._fallback_query_runner.run_query_with_logging(query, params)

    def set_database(self, db: str) -> None:
        self._fallback_query_runner.set_database(db)

    def database(self) -> Optional[str]:
        return self._fallback_query_runner.database()

    def close(self) -> None:
        self._fallback_query_runner.close()

    def _run_arrow_property_get(self, graph_name: str, procedure_name: str, configuration: Dict[str, Any]) -> DataFrame:
        if not self.database():
            raise ValueError(
                "For this call you must have explicitly specified a valid Neo4j database to execute on, "
                "using `GraphDataScience.set_database`."
            )

        payload = {
            "database_name": self.database(),
            "graph_name": graph_name,
            "procedure_name": procedure_name,
            "configuration": configuration,
        }
        ticket = flight.Ticket(json.dumps(payload).encode("utf-8"))

        get = self._flight_client.do_get(ticket)
        result: DataFrame = get.read_pandas()

        return result

    def create_graph_constructor(self, graph_name: str, concurrency: int) -> GraphConstructor:
        database = self.database()
        if not database:
            raise ValueError(
                "For this call you must have explicitly specified a valid Neo4j database to target, "
                "using `GraphDataScience.set_database`."
            )

        return ArrowGraphConstructor(database, graph_name, self._flight_client, concurrency)


class AuthFactory(ClientMiddlewareFactory):  # type: ignore
    def __init__(self, auth: Tuple[str, str], *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._auth = auth
        self._token: Optional[str] = None
        self._token_timestamp = 0

    def start_call(self, info: Any) -> "AuthMiddleware":
        return AuthMiddleware(self)

    def token(self) -> Optional[str]:
        # check whether the token is older than 10 minutes. If so, reset it.
        if self._token and int(time.time()) - self._token_timestamp > 600:
            self._token = None

        return self._token

    def set_token(self, token: str) -> None:
        self._token = token
        self._token_timestamp = int(time.time())

    @property
    def auth(self) -> Tuple[str, str]:
        return self._auth


class AuthMiddleware(ClientMiddleware):  # type: ignore
    def __init__(self, factory: AuthFactory, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._factory = factory

    def received_headers(self, headers: Dict[str, Any]) -> None:
        auth_header: str = headers.get("Authorization", None)
        if not auth_header:
            return
        [auth_type, token] = auth_header.split(" ", 1)
        if auth_type == "Bearer":
            self._factory.set_token(token)

    def sending_headers(self) -> Dict[str, str]:
        token = self._factory.token()
        if not token:
            username, password = self._factory.auth
            auth_token = f"{username}:{password}"
            auth_token = "Basic " + base64.b64encode(auth_token.encode("utf-8")).decode("ASCII")
            # There seems to be a bug, `authorization` must be lower key
            return {"authorization": auth_token}
        else:
            return {"authorization": "Bearer " + token}
