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
from graphdatascience.server_version.compatible_with import (
    IncompatibleServerVersionError,
)
from graphdatascience.server_version.server_version import ServerVersion


class ArrowQueryRunner(QueryRunner):
    def __init__(
        self,
        uri: str,
        fallback_query_runner: QueryRunner,
        server_version: ServerVersion,
        auth: Optional[Tuple[str, str]] = None,
        encrypted: bool = False,
        disable_server_verification: bool = False,
        tls_root_certs: Optional[bytes] = None,
    ):
        self._fallback_query_runner = fallback_query_runner
        self._server_version = server_version

        host, port_string = uri.split(":")

        location = (
            flight.Location.for_grpc_tls(host, int(port_string))
            if encrypted
            else flight.Location.for_grpc_tcp(host, int(port_string))
        )

        client_options: Dict[str, Any] = {"disable_server_verification": disable_server_verification}
        if auth:
            client_options["middleware"] = [AuthFactory(auth)]
        if tls_root_certs:
            client_options["tls_root_certs"] = tls_root_certs

        self._flight_client = flight.FlightClient(location, **client_options)

    def run_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> DataFrame:
        if params is None:
            params = {}

        new_endpoint_server_version = ServerVersion(2, 2, 0)

        # We need to support the deprecated endpoints until they get removed on the server side
        if "gds.graph.streamNodeProperty" in query or "gds.graph.nodeProperty.stream" in query:
            graph_name = params["graph_name"]
            property_name = params["properties"]
            node_labels = params["entities"]

            if self._server_version < new_endpoint_server_version:
                endpoint = "gds.graph.streamNodeProperty"
            else:
                endpoint = "gds.graph.nodeProperty.stream"

            return self._run_arrow_property_get(
                graph_name, endpoint, {"node_property": property_name, "node_labels": node_labels}
            )
        elif "gds.graph.streamNodeProperties" in query or "gds.graph.nodeProperties.stream" in query:
            graph_name = params["graph_name"]
            property_names = params["properties"]
            node_labels = params["entities"]

            if self._server_version < new_endpoint_server_version:
                endpoint = "gds.graph.streamNodeProperties"
            else:
                endpoint = "gds.graph.nodeProperties.stream"
            return self._run_arrow_property_get(
                graph_name,
                endpoint,
                {"node_properties": property_names, "node_labels": node_labels},
            )
        elif "gds.graph.streamRelationshipProperty" in query or "gds.graph.relationshipProperty.stream" in query:
            graph_name = params["graph_name"]
            property_name = params["properties"]
            relationship_types = params["entities"]

            if self._server_version < new_endpoint_server_version:
                endpoint = "gds.graph.streamRelationshipProperty"
            else:
                endpoint = "gds.graph.relationshipProperty.stream"

            return self._run_arrow_property_get(
                graph_name,
                endpoint,
                {"relationship_property": property_name, "relationship_types": relationship_types},
            )
        elif "gds.graph.streamRelationshipProperties" in query or "gds.graph.relationshipProperties.stream" in query:
            graph_name = params["graph_name"]
            property_names = params["properties"]
            relationship_types = params["entities"]

            if self._server_version < new_endpoint_server_version:
                endpoint = "gds.graph.streamRelationshipProperties"
            else:
                endpoint = "gds.graph.relationshipProperties.stream"

            return self._run_arrow_property_get(
                graph_name,
                endpoint,
                {"relationship_properties": property_names, "relationship_types": relationship_types},
            )
        elif "gds.beta.graph.relationships.stream" in query:
            graph_name = params["graph_name"]
            relationship_types = params["relationship_types"]

            if self._server_version < new_endpoint_server_version:
                raise IncompatibleServerVersionError(
                    f"The call gds.beta.graph.relationships.stream with parameters {params} via Arrow requires GDS "
                    f"server version >= 2.2.0. The current version is {self._server_version}"
                )
            else:
                endpoint = "gds.beta.graph.relationships.stream"

            return self._run_arrow_property_get(graph_name, endpoint, {"relationship_types": relationship_types})

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
