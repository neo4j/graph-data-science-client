from __future__ import annotations

import base64
import json
import logging
import re
import time
import warnings
from dataclasses import dataclass
from types import TracebackType
from typing import Any, Callable, Dict, Iterable, Optional, Type, Union

import pandas
import pyarrow
from neo4j.exceptions import ClientError
from pyarrow import Array, ChunkedArray, DictionaryArray, RecordBatch, Schema, Table, chunked_array, flight
from pyarrow import __version__ as arrow_version
from pyarrow.flight import (
    ClientMiddleware,
    ClientMiddlewareFactory,
    FlightDescriptor,
    FlightInternalError,
    FlightMetadataReader,
    FlightStreamWriter,
    FlightTimedOutError,
    FlightUnavailableError,
)
from pyarrow.types import is_dictionary
from tenacity import (
    retry,
    retry_any,
    retry_if_exception_type,
    stop_after_attempt,
    stop_after_delay,
    wait_exponential,
)

from graphdatascience.retry_utils.retry_config import RetryConfig
from graphdatascience.retry_utils.retry_utils import before_log

from ..semantic_version.semantic_version import SemanticVersion
from ..version import __version__
from .arrow_endpoint_version import ArrowEndpointVersion
from .arrow_info import ArrowInfo


class GdsArrowClient:
    @staticmethod
    def create(
        arrow_info: ArrowInfo,
        auth: Optional[tuple[str, str]] = None,
        encrypted: bool = False,
        disable_server_verification: bool = False,
        tls_root_certs: Optional[bytes] = None,
        connection_string_override: Optional[str] = None,
        retry_config: Optional[RetryConfig] = None,
    ) -> GdsArrowClient:
        connection_string: str
        if connection_string_override is not None:
            connection_string = connection_string_override
        else:
            connection_string = arrow_info.listenAddress

        host, port = connection_string.split(":")

        arrow_endpoint_version = ArrowEndpointVersion.from_arrow_info(arrow_info.versions)

        if retry_config is None:
            retry_config = RetryConfig(
                retry=retry_any(
                    retry_if_exception_type(FlightTimedOutError),
                    retry_if_exception_type(FlightUnavailableError),
                    retry_if_exception_type(FlightInternalError),
                ),
                stop=(stop_after_delay(10) | stop_after_attempt(5)),
                wait=wait_exponential(multiplier=1, min=1, max=10),
            )

        return GdsArrowClient(
            host,
            retry_config,
            int(port),
            auth,
            encrypted,
            disable_server_verification,
            tls_root_certs,
            arrow_endpoint_version,
        )

    def __init__(
        self,
        host: str,
        retry_config: RetryConfig,
        port: int = 8491,
        auth: Optional[tuple[str, str]] = None,
        encrypted: bool = False,
        disable_server_verification: bool = False,
        tls_root_certs: Optional[bytes] = None,
        arrow_endpoint_version: ArrowEndpointVersion = ArrowEndpointVersion.V1,
        user_agent: Optional[str] = None,
    ):
        """Creates a new GdsArrowClient instance.

        Parameters
        ----------
        host: str
            The host address of the GDS Arrow server
        port: int
            The host port of the GDS Arrow server (default is 8491)
        auth: Optional[tuple[str, str]]
            A tuple containing the username and password for authentication
        encrypted: bool
            A flag that indicates whether the connection should be encrypted (default is False)
        disable_server_verification: bool
            A flag that disables server verification for TLS connections (default is False)
        tls_root_certs: Optional[bytes]
            PEM-encoded certificates that are used for the connection to the GDS Arrow Flight server
        arrow_endpoint_version:
            The version of the Arrow endpoint to use (default is ArrowEndpointVersion.V1)
        user_agent: Optional[str]
            The user agent string to use for the connection. (default is `neo4j-graphdatascience-v[VERSION] pyarrow-v[PYARROW_VERSION])
        retry_config: Optional[RetryConfig]
            The retry configuration to use for the Arrow requests send by the client.
        """
        self._arrow_endpoint_version = arrow_endpoint_version
        self._host = host
        self._port = port
        self._auth = auth
        self._encrypted = encrypted
        self._disable_server_verification = disable_server_verification
        self._tls_root_certs = tls_root_certs
        self._user_agent = user_agent
        self._retry_config = retry_config
        self._logger = logging.getLogger("gds_arrow_client")

        if auth:
            self._auth_middleware = AuthMiddleware(auth)

        self._flight_client = self._instantiate_flight_client()

    def _instantiate_flight_client(self) -> flight.FlightClient:
        location = (
            flight.Location.for_grpc_tls(self._host, self._port)
            if self._encrypted
            else flight.Location.for_grpc_tcp(self._host, self._port)
        )
        client_options: dict[str, Any] = {"disable_server_verification": self._disable_server_verification}
        if self._auth:
            user_agent = f"neo4j-graphdatascience-v{__version__} pyarrow-v{arrow_version}"
            if self._user_agent:
                user_agent = self._user_agent

            client_options["middleware"] = [
                AuthFactory(self._auth_middleware),
                UserAgentFactory(useragent=user_agent),
            ]
        if self._tls_root_certs:
            client_options["tls_root_certs"] = self._tls_root_certs
        return flight.FlightClient(location, **client_options)

    def connection_info(self) -> tuple[str, int]:
        """
        Returns the host and port of the GDS Arrow server.

        Returns
        -------
        tuple[str, int]
            the host and port of the GDS Arrow server
        """
        return self._host, self._port

    def request_token(self) -> Optional[str]:
        """
        Requests a token from the server and returns it.

        Returns
        -------
        Optional[str]
            a token from the server and returns it.
        """

        @retry(
            reraise=True,
            before=before_log("Request token", self._logger, logging.DEBUG),
            retry=self._retry_config.retry,
            stop=self._retry_config.stop,
            wait=self._retry_config.wait,
        )
        def auth_with_retry() -> None:
            client = self._client()
            if self._auth:
                client.authenticate_basic_token(self._auth[0], self._auth[1])

        if self._auth:
            auth_with_retry()
            return self._auth_middleware.token()
        else:
            return "IGNORED"

    def get_node_properties(
        self,
        graph_name: str,
        database: str,
        node_properties: Union[str, list[str]],
        node_labels: Optional[list[str]] = None,
        list_node_labels: bool = False,
        concurrency: Optional[int] = None,
    ) -> pandas.DataFrame:
        """
        Get node properties from the graph.

        Parameters
        ----------
        graph_name : str
            The name of the graph
        database : str
            The name of the database to which the graph belongs
        node_properties : Union[str, List[str]]
            The name of the node properties to retrieve
        node_labels : Optional[List[str]]
            A list of node labels to filter the nodes
        list_node_labels : bool
            A flag that indicates whether the node labels should be included in the result
        concurrency : Optional[int]
            The number of threads used on the server side when serving the data

        Returns
        -------
        DataFrame
            The requested node property as a DataFrame
        """
        config: dict[str, Any] = {
            "list_node_labels": list_node_labels,
        }

        if isinstance(node_properties, str):
            config["node_property"] = node_properties
            proc = "gds.graph.nodeProperty.stream"
        else:
            config["node_properties"] = node_properties
            proc = "gds.graph.nodeProperties.stream"

        if node_labels:
            config["node_labels"] = node_labels

        return self._do_get_with_retry(database, graph_name, proc, concurrency, config)

    def get_node_labels(self, graph_name: str, database: str, concurrency: Optional[int] = None) -> pandas.DataFrame:
        """
        Get all nodes and their labels from the graph.

        Parameters
        ----------
        graph_name : str
            The name of the graph
        database : str
            The name of the database to which the graph belongs
        concurrency : Optional[int]
            The number of threads used on the server side when serving the data

        Returns
        -------
        DataFrame
            The requested nodes as a DataFrame
        """
        return self._do_get_with_retry(database, graph_name, "gds.graph.nodeLabels.stream", concurrency, {})

    def get_relationships(
        self, graph_name: str, database: str, relationship_types: list[str], concurrency: Optional[int] = None
    ) -> pandas.DataFrame:
        """
        Get relationships from the graph.

        Parameters
        ----------
        graph_name : str
            The name of the graph
        database : str
            The name of the database to which the graph belongs
        relationship_types : List[str]
            The name of the relationship types to retrieve
        concurrency : Optional[int]
            The number of threads used on the server side when serving the data

        Returns
        -------
        DataFrame
            The requested relationships as a DataFrame
        """
        return self._do_get_with_retry(
            database,
            graph_name,
            "gds.graph.relationships.stream",
            concurrency,
            {"relationship_types": relationship_types},
        )

    def get_relationship_properties(
        self,
        graph_name: str,
        database: str,
        relationship_properties: Union[str, list[str]],
        relationship_types: list[str],
        concurrency: Optional[int] = None,
    ) -> pandas.DataFrame:
        """
        Get relationships and their properties from the graph.

        Parameters
        ----------
        graph_name : str
            The name of the graph
        database : str
            The name of the database to which the graph belongs
        relationship_properties : Union[str, List[str]]
            The name of the relationship properties to retrieve
        relationship_types : List[str]
            The name of the relationship types to retrieve
        concurrency : Optional[int]
            The number of threads used on the server side when serving the data

        Returns
        -------
        DataFrame
            The requested relationships as a DataFrame
        """
        config: dict[str, Any] = {}
        if isinstance(relationship_properties, str):
            config = {"relationship_property": relationship_properties}
            proc = "gds.graph.relationshipProperty.stream"
        else:
            config = {"relationship_properties": relationship_properties}
            proc = "gds.graph.relationshipProperties.stream"

        if relationship_types:
            config["relationship_types"] = relationship_types

        return self._do_get_with_retry(database, graph_name, proc, concurrency, config)

    def create_graph(
        self,
        graph_name: str,
        database: str,
        undirected_relationship_types: Optional[list[str]] = None,
        inverse_indexed_relationship_types: Optional[list[str]] = None,
        concurrency: Optional[int] = None,
    ) -> None:
        """
        Starts a new graph import process on the GDS server.

        The import process accepts separate node and relationship stream uploads.

        Parameters
        ----------
        graph_name : str
            The name used to identify the graph in the catalog and the import process
        database: str
            The name of the database from which the graph will be accessible
        undirected_relationship_types : Optional[List[str]]
            A list of relationship types that should be treated as undirected
        inverse_indexed_relationship_types : Optional[List[str]]
            A list of relationship types that should be indexed in reverse direction as well
        concurrency : Optional[int]
            The number of threads used on the server side when importing the graph
        """

        config: dict[str, Any] = {
            "name": graph_name,
            "database_name": database,
        }

        if concurrency:
            config["concurrency"] = concurrency
        if undirected_relationship_types:
            config["undirected_relationship_types"] = undirected_relationship_types
        if inverse_indexed_relationship_types:
            config["inverse_indexed_relationship_types"] = inverse_indexed_relationship_types

        self._send_action("CREATE_GRAPH", config)

    def create_graph_from_triplets(
        self,
        graph_name: str,
        database: str,
        undirected_relationship_types: Optional[list[str]] = None,
        inverse_indexed_relationship_types: Optional[list[str]] = None,
        concurrency: Optional[int] = None,
    ) -> None:
        """
        Starts a new graph import process on the GDS server.

        The import process accepts triplets as input data.

        Parameters
        ----------
        graph_name : str
            The name used to identify the graph in the catalog and the import process
        database: str
            The name of the database from which the graph will be accessible
        undirected_relationship_types : Optional[List[str]]
            A list of relationship types that should be treated as undirected
        inverse_indexed_relationship_types : Optional[List[str]]
            A list of relationship types that should be indexed in reverse direction as well
        concurrency : Optional[int]
            The number of threads used on the server side when importing the graph
        """

        config: dict[str, Any] = {
            "name": graph_name,
            "database_name": database,
        }

        if concurrency:
            config["concurrency"] = concurrency
        if undirected_relationship_types:
            config["undirected_relationship_types"] = undirected_relationship_types
        if inverse_indexed_relationship_types:
            config["inverse_indexed_relationship_types"] = inverse_indexed_relationship_types

        self._send_action("CREATE_GRAPH_FROM_TRIPLETS", config)

    def create_database(
        self,
        database: str,
        id_type: Optional[str] = None,
        id_property: Optional[str] = None,
        db_format: Optional[str] = None,
        concurrency: Optional[int] = None,
        force: bool = False,
        high_io: bool = False,
        use_bad_collector: bool = False,
    ) -> None:
        """
        Starts a new graph import process on the GDS server.

        The import process accepts triplets as input data.

        Parameters
        ----------
        database: str
            The name used to identify the database and the import process
        id_type : Optional[str]
            Sets the node id type used in the input data. Can be either `INTEGER` or `STRING` (default is `INTEGER`)
        id_property : Optional[str]
            The node property key which stores the node id of the input data (default is `originalId`)
        db_format
            Database format. Valid values standard, aligned, high_limit or block (default is controlled by the db setting `db.db_format`)
        concurrency : Optional[int]
            The number of threads used on the server side when importing the graph
        force: bool
            Force deletes any existing database files prior to the import (default is False)
        high_io: bool
            Ignore environment-based heuristics, and specify whether the target storage subsystem can support parallel IO with high throughput (default is False)
        use_bad_collector: bool
            Collects bad node and relationship records during import and writes them into the log (default is false)
        """

        config: dict[str, Any] = {
            "name": database,
            "force": force,
            "high_io": high_io,
            "use_bad_collector": use_bad_collector,
        }

        if concurrency:
            config["concurrency"] = concurrency
        if id_type:
            config["id_type"] = id_type
        if id_property:
            config["id_property"] = id_property
        if db_format:
            config["db_format"] = db_format

        self._send_action("CREATE_DATABASE", config)

    def node_load_done(self, graph_name: str) -> NodeLoadDoneResult:
        """
        Notifies the server that all node data has been sent.

        Parameters
        ----------
        graph_name : str
            The name of the import process

        Returns
        -------
        NodeLoadDoneResult
            A result object containing the name of the import process and the number of nodes loaded
        """
        return NodeLoadDoneResult.from_json(self._send_action("NODE_LOAD_DONE", {"name": graph_name}))

    def relationship_load_done(self, graph_name: str) -> RelationshipLoadDoneResult:
        """
        Notifies the server that all relationship data has been sent.

        This will trigger the finalization of the import process and make the graph or database available.

        Parameters
        ----------
        graph_name : str
            The name of the import process

         Returns
        -------
        RelationshipLoadDoneResult
            A result object containing the name of the import process and the number of relationships loaded
        """
        return RelationshipLoadDoneResult.from_json(self._send_action("RELATIONSHIP_LOAD_DONE", {"name": graph_name}))

    def triplet_load_done(self, graph_name: str) -> TripletLoadDoneResult:
        """
        Notifies the server that all triplet data has been sent.

        This will trigger the finalization of the import process and make the graph available in the graph catalog.

        Parameters
        ----------
        graph_name : str
            The name of the import process

        Returns
        -------
        TripletLoadDoneResult
            A result object containing the name of the import process and the number of nodes and relationships loaded
        """
        return TripletLoadDoneResult.from_json(self._send_action("TRIPLET_LOAD_DONE", {"name": graph_name}))

    def abort(self, graph_name: str) -> None:
        """
        Aborts the specified import process.

        Parameters
        ----------
        graph_name : str
            The name of the import process
        """
        self._send_action("ABORT", {"name": graph_name})

    def upload_nodes(
        self,
        graph_name: str,
        node_data: Union[pyarrow.Table, Iterable[pyarrow.RecordBatch], pandas.DataFrame],
        batch_size: int = 10_000,
        progress_callback: Callable[[int], None] = lambda x: None,
    ) -> None:
        """
        Uploads node data to the server.

        Parameters
        ----------
        graph_name : str
            The name of the import process
        node_data : Union[pyarrow.Table, Iterable[pyarrow.RecordBatch], DataFrame]
            The node data to upload
        batch_size : int
            The number of rows per batch
        progress_callback : Callable[[int], None]
            A callback function that is called with the number of rows uploaded after each batch
        """
        self._upload_data(graph_name, "node", node_data, batch_size, progress_callback)

    def upload_relationships(
        self,
        graph_name: str,
        relationship_data: Union[pyarrow.Table, Iterable[pyarrow.RecordBatch], pandas.DataFrame],
        batch_size: int = 10_000,
        progress_callback: Callable[[int], None] = lambda x: None,
    ) -> None:
        """
        Uploads relationship data to the server.

        Parameters
        ----------
        graph_name : str
            The name of the import process
        relationship_data : Union[pyarrow.Table, Iterable[pyarrow.RecordBatch], DataFrame]
            The relationship data to upload
        batch_size : int
            The number of rows per batch
        progress_callback : Callable[[int], None]
            A callback function that is called with the number of rows uploaded after each batch
        """
        self._upload_data(graph_name, "relationship", relationship_data, batch_size, progress_callback)

    def upload_triplets(
        self,
        graph_name: str,
        triplet_data: Union[pyarrow.Table, Iterable[pyarrow.RecordBatch], pandas.DataFrame],
        batch_size: int = 10_000,
        progress_callback: Callable[[int], None] = lambda x: None,
    ) -> None:
        """
        Uploads triplet data to the server.

        Parameters
        ----------
        graph_name : str
            The name of the import process
        triplet_data : Union[pyarrow.Table, Iterable[pyarrow.RecordBatch], DataFrame]
            The triplet data to upload
        batch_size : int
            The number of rows per batch
        progress_callback : Callable[[int], None]
            A callback function that is called with the number of rows uploaded after each batch
        """
        self._upload_data(graph_name, "triplet", triplet_data, batch_size, progress_callback)

    def __getstate__(self) -> Dict[str, Any]:
        state = self.__dict__.copy()
        # Remove the FlightClient as it isn't serializable
        if "_flight_client" in state:
            del state["_flight_client"]
        return state

    def _client(self) -> flight.FlightClient:
        """
        Lazy client construction to help pickle this class because a PyArrow
        FlightClient is not serializable.
        """
        if not hasattr(self, "_flight_client") or not self._flight_client:
            self._flight_client = self._instantiate_flight_client()
        return self._flight_client

    def _send_action(self, action_type: str, meta_data: dict[str, Any]) -> dict[str, Any]:
        action_type = self._versioned_action_type(action_type)
        client = self._client()

        @retry(
            reraise=True,
            before=before_log("Send action", self._logger, logging.DEBUG),
            retry=self._retry_config.retry,
            stop=self._retry_config.stop,
            wait=self._retry_config.wait,
        )
        def send_with_retry() -> dict[str, Any]:
            try:
                result = client.do_action(flight.Action(action_type, json.dumps(meta_data).encode("utf-8")))

                # Consume result fully to sanity check and avoid cancelled streams
                collected_result = list(result)
                assert len(collected_result) == 1

                return json.loads(collected_result[0].body.to_pybytes().decode())  # type: ignore
            except Exception as e:
                self.handle_flight_error(e)
                raise e  # unreachable

        return send_with_retry()

    def _upload_data(
        self,
        graph_name: str,
        entity_type: str,
        data: Union[pyarrow.Table, list[pyarrow.RecordBatch], pandas.DataFrame],
        batch_size: int,
        progress_callback: Callable[[int], None],
    ) -> None:
        if isinstance(data, pyarrow.Table):
            batches = data.to_batches(batch_size)
        elif isinstance(data, pandas.DataFrame):
            batches = pyarrow.Table.from_pandas(data).to_batches(batch_size)
        else:
            batches = data

        flight_descriptor = self._versioned_flight_descriptor({"name": graph_name, "entity_type": entity_type})
        upload_descriptor = flight.FlightDescriptor.for_command(json.dumps(flight_descriptor).encode("utf-8"))

        @retry(
            reraise=True,
            before=before_log("Do put", self._logger, logging.DEBUG),
            retry=self._retry_config.retry,
            stop=self._retry_config.stop,
            wait=self._retry_config.wait,
        )
        def safe_do_put(
            upload_descriptor: FlightDescriptor, schema: Schema
        ) -> tuple[FlightStreamWriter, FlightMetadataReader]:
            return self._client().do_put(upload_descriptor, schema)  # type: ignore

        put_stream, ack_stream = safe_do_put(upload_descriptor, batches[0].schema)

        @retry(
            reraise=True,
            before=before_log("Upload batch", self._logger, logging.DEBUG),
            retry=self._retry_config.retry,
            stop=self._retry_config.stop,
            wait=self._retry_config.wait,
        )
        def upload_batch(p: RecordBatch) -> None:
            put_stream.write_batch(p)

        try:
            with put_stream:
                for partition in batches:
                    upload_batch(partition)
                    ack_stream.read()
                    progress_callback(partition.num_rows)
        except Exception as e:
            GdsArrowClient.handle_flight_error(e)

    def _do_get_with_retry(
        self,
        database: str,
        graph_name: str,
        procedure_name: str,
        concurrency: Optional[int],
        configuration: dict[str, Any],
    ) -> pandas.DataFrame:
        @retry(
            reraise=True,
            before=before_log("Do get", self._logger, logging.DEBUG),
            retry=self._retry_config.retry,
            stop=self._retry_config.stop,
            wait=self._retry_config.wait,
        )
        def safe_do_get() -> pandas.DataFrame:
            return self._do_get(database, graph_name, procedure_name, concurrency, configuration)

        return safe_do_get()

    def _do_get(
        self,
        database: str,
        graph_name: str,
        procedure_name: str,
        concurrency: Optional[int],
        configuration: dict[str, Any],
    ) -> pandas.DataFrame:
        payload: dict[str, Any] = {
            "database_name": database,
            "graph_name": graph_name,
            "procedure_name": procedure_name,
            "configuration": configuration,
        }

        if concurrency:
            payload["concurrency"] = concurrency

        if self._arrow_endpoint_version == ArrowEndpointVersion.V1:
            payload = {
                "name": "GET_COMMAND",
                "version": ArrowEndpointVersion.V1.version(),
                "body": payload,
            }

        ticket = flight.Ticket(json.dumps(payload).encode("utf-8"))

        client = self._client()
        try:
            get = client.do_get(ticket)
            arrow_table = get.read_all()
        except Exception as e:
            self.handle_flight_error(e)

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

        if SemanticVersion.from_string(pandas.__version__) >= SemanticVersion(2, 0, 0):
            return arrow_table.to_pandas(types_mapper=pandas.ArrowDtype)  # type: ignore
        else:
            arrow_table = self._sanitize_arrow_table(arrow_table)
            return arrow_table.to_pandas()  # type: ignore

    def __enter__(self) -> GdsArrowClient:
        return self

    def __exit__(
        self,
        exception_type: Optional[Type[BaseException]],
        exception_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self.close()

    def close(self) -> None:
        if self._flight_client:
            self._flight_client.close()

    def _versioned_action_type(self, action_type: str) -> str:
        return self._arrow_endpoint_version.prefix() + action_type

    def _versioned_flight_descriptor(self, flight_descriptor: dict[str, Any]) -> dict[str, Any]:
        return {
            "name": "PUT_COMMAND",
            "version": ArrowEndpointVersion.V1.version(),
            "body": flight_descriptor,
        }

    @staticmethod
    def _sanitize_arrow_table(arrow_table: Table) -> Table:
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
                    decoded_col: Array = chunked_array(
                        [GdsArrowClient._decode_pyarrow_array(chunk) for chunk in arrow_table[field.name].chunks]
                    )
                else:
                    col = arrow_table[field.name]
                    decoded_col = GdsArrowClient._decode_pyarrow_array(col)
                arrow_table = arrow_table.set_column(idx, field.name, decoded_col)
        return arrow_table

    @staticmethod
    def _decode_pyarrow_array(array: Array) -> Array:
        if isinstance(array, DictionaryArray):
            dictArr = array
            return dictArr.dictionary_decode()
        else:
            return array

    @staticmethod
    def handle_flight_error(e: Exception) -> None:
        if (
            isinstance(e, flight.FlightServerError)
            or isinstance(e, flight.FlightInternalError)
            or isinstance(e, ClientError)
        ):
            original_message = e.args[0]
            improved_message = original_message.replace(
                "Flight RPC failed with message: org.apache.arrow.flight.FlightRuntimeException: ", ""
            )
            improved_message = improved_message.replace(
                "Flight returned internal error, with message: org.apache.arrow.flight.FlightRuntimeException: ", ""
            )
            improved_message = improved_message.replace(
                "Failed to invoke procedure `gds.arrow.project`: Caused by: org.apache.arrow.flight.FlightRuntimeException: ",
                "",
            )
            improved_message = re.sub(r"(\. )?gRPC client debug context: .+$", "", improved_message)

            raise flight.FlightServerError(improved_message)
        else:
            raise e


class UserAgentFactory(ClientMiddlewareFactory):  # type: ignore
    def __init__(self, useragent: str, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._middleware = UserAgentMiddleware(useragent)

    def start_call(self, info: Any) -> ClientMiddleware:
        return self._middleware


class UserAgentMiddleware(ClientMiddleware):  # type: ignore
    def __init__(self, useragent: str, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._useragent = useragent

    def sending_headers(self) -> dict[str, str]:
        return {"x-gds-user-agent": self._useragent}

    def received_headers(self, headers: dict[str, Any]) -> None:
        pass


class AuthFactory(ClientMiddlewareFactory):  # type: ignore
    def __init__(self, middleware: AuthMiddleware, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._middleware = middleware

    def start_call(self, info: Any) -> AuthMiddleware:
        return self._middleware


class AuthMiddleware(ClientMiddleware):  # type: ignore
    def __init__(self, auth: tuple[str, str], *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._auth = auth
        self._token: Optional[str] = None
        self._token_timestamp = 0

    def token(self) -> Optional[str]:
        # check whether the token is older than 10 minutes. If so, reset it.
        if self._token and int(time.time()) - self._token_timestamp > 600:
            self._token = None

        return self._token

    def _set_token(self, token: str) -> None:
        self._token = token
        self._token_timestamp = int(time.time())

    def received_headers(self, headers: dict[str, Any]) -> None:
        auth_header = headers.get("authorization", None)
        if not auth_header:
            return

        # the result is always a list
        header_value = auth_header[0]

        if not isinstance(header_value, str):
            raise ValueError(f"Incompatible header value received from server: `{header_value}`")

        auth_type, token = header_value.split(" ", 1)
        if auth_type == "Bearer":
            self._set_token(token)

    def sending_headers(self) -> dict[str, str]:
        token = self.token()
        if not token:
            username, password = self._auth
            auth_token = f"{username}:{password}"
            auth_token = "Basic " + base64.b64encode(auth_token.encode("utf-8")).decode("ASCII")
            # There seems to be a bug, `authorization` must be lower key
            return {"authorization": auth_token}
        else:
            return {"authorization": "Bearer " + token}


@dataclass(repr=True, frozen=True)
class NodeLoadDoneResult:
    name: str
    node_count: int

    @classmethod
    def from_json(cls, json: dict[str, Any]) -> NodeLoadDoneResult:
        return cls(
            name=json["name"],
            node_count=json["node_count"],
        )


@dataclass(repr=True, frozen=True)
class RelationshipLoadDoneResult:
    name: str
    relationship_count: int

    @classmethod
    def from_json(cls, json: dict[str, Any]) -> RelationshipLoadDoneResult:
        return cls(
            name=json["name"],
            relationship_count=json["relationship_count"],
        )


@dataclass(repr=True, frozen=True)
class TripletLoadDoneResult:
    name: str
    node_count: int
    relationship_count: int

    @classmethod
    def from_json(cls, json: dict[str, Any]) -> TripletLoadDoneResult:
        return cls(name=json["name"], node_count=json["node_count"], relationship_count=json["relationship_count"])
