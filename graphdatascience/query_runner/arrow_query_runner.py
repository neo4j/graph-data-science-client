from __future__ import annotations

import base64
import json
import time
import warnings
from typing import Any, Dict, List, Optional, Tuple

import pyarrow.flight as flight
from pandas import DataFrame
from pyarrow import ChunkedArray, Table, chunked_array
from pyarrow.flight import ClientMiddleware, ClientMiddlewareFactory
from pyarrow.types import is_dictionary  # type: ignore

from ..call_parameters import CallParameters
from ..server_version.server_version import ServerVersion
from .arrow_endpoint_version import ArrowEndpointVersion
from .arrow_graph_constructor import ArrowGraphConstructor
from .graph_constructor import GraphConstructor
from .query_runner import QueryRunner
from graphdatascience.server_version.compatible_with import (
    IncompatibleServerVersionError,
)


class ArrowQueryRunner(QueryRunner):
    @staticmethod
    def create(
        fallback_query_runner: QueryRunner,
        auth: Optional[Tuple[str, str]] = None,
        encrypted: bool = False,
        disable_server_verification: bool = False,
        tls_root_certs: Optional[bytes] = None,
        connection_string_override: Optional[str] = None,
    ) -> QueryRunner:
        arrow_info = (
            fallback_query_runner.call_procedure(endpoint="gds.debug.arrow", custom_error=False).squeeze().to_dict()
        )
        server_version = fallback_query_runner.server_version()
        connection_string: str
        if connection_string_override is not None:
            connection_string = connection_string_override
        else:
            connection_string = arrow_info.get("advertisedListenAddress", arrow_info["listenAddress"])
        arrow_endpoint_version = ArrowEndpointVersion.from_arrow_info(arrow_info.get("versions", []))

        if arrow_info["running"]:
            return ArrowQueryRunner(
                connection_string,
                fallback_query_runner,
                server_version,
                auth,
                encrypted,
                disable_server_verification,
                tls_root_certs,
                arrow_endpoint_version,
            )
        else:
            return fallback_query_runner

    def __init__(
        self,
        uri: str,
        fallback_query_runner: QueryRunner,
        server_version: ServerVersion,
        auth: Optional[Tuple[str, str]] = None,
        encrypted: bool = False,
        disable_server_verification: bool = False,
        tls_root_certs: Optional[bytes] = None,
        arrow_endpoint_version: ArrowEndpointVersion = ArrowEndpointVersion.ALPHA,
    ):
        self._fallback_query_runner = fallback_query_runner
        self._server_version = server_version
        self._arrow_endpoint_version = arrow_endpoint_version

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

    def warn_about_deprecation(self, old_endpoint: str, new_endpoint: str) -> None:
        warnings.warn(
            DeprecationWarning(f"The endpoint '{old_endpoint}' is deprecated. Please use '{new_endpoint}' instead.")
        )

    def run_cypher(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        database: Optional[str] = None,
        custom_error: bool = True,
    ) -> DataFrame:
        return self._fallback_query_runner.run_cypher(query, params, database, custom_error)

    def call_procedure(
        self,
        endpoint: str,
        params: Optional[CallParameters] = None,
        yields: Optional[List[str]] = None,
        database: Optional[str] = None,
        logging: bool = False,
        custom_error: bool = True,
    ) -> DataFrame:
        if params is None:
            params = CallParameters()

        new_endpoint_server_version = ServerVersion(2, 2, 0)
        no_tier_in_namespace_server_version = ServerVersion(2, 5, 0)

        # We need to support the deprecated endpoints until they get removed on the server side
        if (
            old_endpoint := ("gds.graph.streamNodeProperty" == endpoint)
        ) or "gds.graph.nodeProperty.stream" == endpoint:
            graph_name = params["graph_name"]
            property_name = params["properties"]
            node_labels = params["entities"]

            config = {"node_property": property_name, "node_labels": node_labels}

            if "listNodeLabels" in params["config"]:
                config["list_node_labels"] = params["config"]["listNodeLabels"]

            if self._server_version < new_endpoint_server_version:
                endpoint = "gds.graph.streamNodeProperty"
            else:
                endpoint = "gds.graph.nodeProperty.stream"
                if old_endpoint:
                    self.warn_about_deprecation(
                        old_endpoint="gds.graph.streamNodeProperty", new_endpoint="gds.graph.nodeProperty.stream"
                    )

            return self._run_arrow_property_get(graph_name, endpoint, config)
        elif (
            old_endpoint := ("gds.graph.streamNodeProperties" == endpoint)
        ) or "gds.graph.nodeProperties.stream" == endpoint:
            graph_name = params["graph_name"]

            config = {"node_properties": params["properties"], "node_labels": params["entities"]}

            if "listNodeLabels" in params["config"]:
                config["list_node_labels"] = params["config"]["listNodeLabels"]

            if self._server_version < new_endpoint_server_version:
                endpoint = "gds.graph.streamNodeProperties"
            else:
                endpoint = "gds.graph.nodeProperties.stream"
                if old_endpoint:
                    self.warn_about_deprecation(
                        old_endpoint="gds.graph.streamNodeProperties", new_endpoint="gds.graph.nodeProperties.stream"
                    )
            return self._run_arrow_property_get(
                graph_name,
                endpoint,
                config,
            )
        elif (
            old_endpoint := ("gds.graph.streamRelationshipProperty" == endpoint)
        ) or "gds.graph.relationshipProperty.stream" == endpoint:
            graph_name = params["graph_name"]
            property_name = params["properties"]
            relationship_types = params["entities"]

            if self._server_version < new_endpoint_server_version:
                endpoint = "gds.graph.streamRelationshipProperty"
            else:
                endpoint = "gds.graph.relationshipProperty.stream"
                if old_endpoint:
                    self.warn_about_deprecation(
                        old_endpoint="gds.graph.streamRelationshipProperty",
                        new_endpoint="gds.graph.relationshipProperty.stream",
                    )
            return self._run_arrow_property_get(
                graph_name,
                endpoint,
                {"relationship_property": property_name, "relationship_types": relationship_types},
            )
        elif (
            old_endpoint := ("gds.graph.streamRelationshipProperties" == endpoint)
        ) or "gds.graph.relationshipProperties.stream" == endpoint:
            graph_name = params["graph_name"]
            property_names = params["properties"]
            relationship_types = params["entities"]

            if self._server_version < new_endpoint_server_version:
                endpoint = "gds.graph.streamRelationshipProperties"
            else:
                endpoint = "gds.graph.relationshipProperties.stream"
                if old_endpoint:
                    self.warn_about_deprecation(
                        old_endpoint="gds.graph.streamRelationshipProperties",
                        new_endpoint="gds.graph.relationshipProperties.stream",
                    )

            return self._run_arrow_property_get(
                graph_name,
                endpoint,
                {"relationship_properties": property_names, "relationship_types": relationship_types},
            )
        elif (
            old_endpoint := ("gds.beta.graph.relationships.stream" == endpoint)
        ) or "gds.graph.relationships.stream" == endpoint:
            graph_name = params["graph_name"]
            relationship_types = params["relationship_types"]

            if self._server_version < new_endpoint_server_version:
                raise IncompatibleServerVersionError(
                    f"The call gds.beta.graph.relationships.stream with parameters {params} via Arrow requires GDS "
                    f"server version >= 2.2.0. The current version is {self._server_version}"
                )
            else:
                if self._server_version < no_tier_in_namespace_server_version:
                    endpoint = "gds.beta.graph.relationships.stream"
                else:
                    endpoint = "gds.graph.relationships.stream"
                    if old_endpoint:
                        self.warn_about_deprecation(
                            old_endpoint="gds.beta.graph.relationships.stream",
                            new_endpoint="gds.graph.relationships.stream",
                        )

            return self._run_arrow_property_get(graph_name, endpoint, {"relationship_types": relationship_types})

        return self._fallback_query_runner.call_procedure(endpoint, params, yields, database, logging, custom_error)

    def server_version(self) -> ServerVersion:
        return self._fallback_query_runner.server_version()

    def driver_config(self) -> Dict[str, Any]:
        return self._fallback_query_runner.driver_config()

    def encrypted(self) -> bool:
        return self._fallback_query_runner.encrypted()

    def set_database(self, database: str) -> None:
        self._fallback_query_runner.set_database(database)

    def set_bookmarks(self, bookmarks: Optional[Any]) -> None:
        self._fallback_query_runner.set_bookmarks(bookmarks)

    def database(self) -> Optional[str]:
        return self._fallback_query_runner.database()

    def bookmarks(self) -> Optional[Any]:
        return self._fallback_query_runner.bookmarks()

    def last_bookmarks(self) -> Optional[Any]:
        return self._fallback_query_runner.last_bookmarks()

    def close(self) -> None:
        self._fallback_query_runner.close()
        # PyArrow 7 did not expose a close method yet
        if hasattr(self._flight_client, "close"):
            self._flight_client.close()

    def fallback_query_runner(self) -> QueryRunner:
        return self._fallback_query_runner

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

        if self._arrow_endpoint_version == ArrowEndpointVersion.V1:
            payload = {
                "name": "GET_MESSAGE",
                "version": ArrowEndpointVersion.V1.version(),
                "body": payload,
            }

        ticket = flight.Ticket(json.dumps(payload).encode("utf-8"))
        get = self._flight_client.do_get(ticket)
        arrow_table = get.read_all()

        if configuration.get("list_node_labels", False):
            # GDS 2.5 had an inconsistent naming of the node labels column
            new_colum_names = ["nodeLabels" if i == "labels" else i for i in arrow_table.column_names]
            arrow_table = arrow_table.rename_columns(new_colum_names)

        # Pandas 2.2.0 deprecated an API used by ArrowTable.to_pandas() (< pyarrow 15.0)
        warnings.filterwarnings(
            "ignore",
            category=DeprecationWarning,
            message=r"Passing a BlockManager to DataFrame is deprecated",
        )

        return self._sanitize_arrow_table(arrow_table).to_pandas()  # type: ignore

    def create_graph_constructor(
        self, graph_name: str, concurrency: int, undirected_relationship_types: Optional[List[str]]
    ) -> GraphConstructor:
        database = self.database()
        if not database:
            raise ValueError(
                "For this call you must have explicitly specified a valid Neo4j database to target, "
                "using `GraphDataScience.set_database`."
            )

        return ArrowGraphConstructor(
            database,
            graph_name,
            self._flight_client,
            concurrency,
            self._arrow_endpoint_version,
            undirected_relationship_types,
        )

    def _sanitize_arrow_table(self, arrow_table: Table) -> Table:
        # empty columns cannot be used to build a chunked_array in pyarrow
        if len(arrow_table) == 0:
            return arrow_table

        dict_encoded_fields = [
            (idx, field) for idx, field in enumerate(arrow_table.schema) if is_dictionary(field.type)
        ]
        for idx, field in dict_encoded_fields:
            try:
                field.type.to_pandas_dtype()
            except NotImplementedError:
                # we need to decode the dictionary column before transforming to pandas
                if isinstance(arrow_table[field.name], ChunkedArray):
                    decoded_col = chunked_array([chunk.dictionary_decode() for chunk in arrow_table[field.name].chunks])
                else:
                    decoded_col = arrow_table[field.name].dictionary_decode()
                arrow_table = arrow_table.set_column(idx, field.name, decoded_col)
        return arrow_table


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
