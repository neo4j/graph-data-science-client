from __future__ import annotations

import json
import logging
from types import TracebackType
from typing import Any, Iterable, Type

import pandas
import pyarrow
from pyarrow import Array, ChunkedArray, DictionaryArray, RecordBatch, Table, chunked_array, flight
from pyarrow.types import is_dictionary
from pydantic import BaseModel

from graphdatascience.arrow_client.arrow_endpoint_version import ArrowEndpointVersion
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient, ConnectionInfo
from graphdatascience.arrow_client.v1.data_mapper_utils import deserialize_single

from ...procedure_surface.arrow.error_handler import handle_flight_error
from ...semantic_version.semantic_version import SemanticVersion
from ..progress_callback import ProgressCallback


class GdsArrowClient:
    def __init__(
        self,
        flight_client: AuthenticatedArrowClient,
    ):
        """Creates a new GdsArrowClient instance.

        Parameters
        ----------
        flight_client : AuthenticatedArrowClient
            The authenticated flight client to use for communication with the GDS server. Ownership of the client is transferred to this GdsArrowClient.
        """
        self._flight_client = flight_client
        self._logger = logging.getLogger("gds_arrow_client")

    def get_node_properties(
        self,
        graph_name: str,
        database: str,
        node_properties: str | list[str],
        node_labels: list[str] | None = None,
        list_node_labels: bool = False,
        concurrency: int | None = None,
    ) -> pandas.DataFrame:
        """
        Get node properties from the graph.

        Parameters
        ----------
        graph_name
            The name of the graph
        database
            The name of the database to which the graph belongs
        node_properties
            The name of the node properties to retrieve
        node_labels
            A list of node labels to filter the nodes
        list_node_labels
            A flag that indicates whether the node labels should be included in the result
        concurrency
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

        result = self._get_data(graph_name, database, proc, concurrency, config)
        if list_node_labels:
            result.rename(columns={"labels": "nodeLabels"}, inplace=True)

        return result

    def get_node_labels(self, graph_name: str, database: str, concurrency: int | None = None) -> pandas.DataFrame:
        """
        Get all nodes and their labels from the graph.

        Parameters
        ----------
        graph_name
            The name of the graph
        database
            The name of the database to which the graph belongs
        concurrency
            The number of threads used on the server side when serving the data

        Returns
        -------
        DataFrame
            The requested nodes as a DataFrame
        """
        return self._get_data(graph_name, database, "gds.graph.nodeLabels.stream", concurrency, {})

    def get_relationships(
        self,
        graph_name: str,
        database: str,
        relationship_types: list[str],
        concurrency: int | None = None,
    ) -> pandas.DataFrame:
        """
        Get relationships from the graph.

        Parameters
        ----------
        graph_name : str
            The name of the graph
        database : str
            The name of the database to which the graph belongs
        relationship_types : list[str]
            The name of the relationship types to retrieve
        concurrency : int | None
            The number of threads used on the server side when serving the data

        Returns
        -------
        DataFrame
            The requested relationships as a DataFrame
        """
        return self._get_data(
            graph_name,
            database,
            "gds.graph.relationships.stream",
            concurrency,
            {"relationship_types": relationship_types},
        )

    def get_relationship_properties(
        self,
        graph_name: str,
        database: str,
        relationship_properties: str | list[str],
        relationship_types: list[str],
        concurrency: int | None = None,
    ) -> pandas.DataFrame:
        """
        Get relationships and their properties from the graph.

        Parameters
        ----------
        graph_name
            The name of the graph
        database
            The name of the database to which the graph belongs
        relationship_properties
            The name of the relationship properties to retrieve
        relationship_types
            The name of the relationship types to retrieve
        concurrency
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

        return self._get_data(graph_name, database, proc, concurrency, config)

    def create_graph(
        self,
        graph_name: str,
        database: str,
        undirected_relationship_types: list[str] | None = None,
        inverse_indexed_relationship_types: list[str] | None = None,
        concurrency: int | None = None,
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
        undirected_relationship_types : list[str] | None
            A list of relationship types that should be treated as undirected
        inverse_indexed_relationship_types : list[str] | None
            A list of relationship types that should be indexed in reverse direction as well
        concurrency : int | None
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
        undirected_relationship_types: list[str] | None = None,
        inverse_indexed_relationship_types: list[str] | None = None,
        concurrency: int | None = None,
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
        undirected_relationship_types : list[str] | None
            A list of relationship types that should be treated as undirected
        inverse_indexed_relationship_types : list[str] | None
            A list of relationship types that should be indexed in reverse direction as well
        concurrency : int | None
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
        id_type: str | None = None,
        id_property: str | None = None,
        db_format: str | None = None,
        concurrency: int | None = None,
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
        id_type : str | None
            Sets the node id type used in the input data. Can be either `INTEGER` or `STRING` (default is `INTEGER`)
        id_property : str | None
            The node property key which stores the node id of the input data (default is `originalId`)
        db_format
            Database format. Valid values standard, aligned, high_limit or block (default is controlled by the db setting `db.db_format`)
        concurrency : int | None
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
        return NodeLoadDoneResult(**self._send_action("NODE_LOAD_DONE", {"name": graph_name}))

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
        return RelationshipLoadDoneResult(**self._send_action("RELATIONSHIP_LOAD_DONE", {"name": graph_name}))

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
        return TripletLoadDoneResult(**self._send_action("TRIPLET_LOAD_DONE", {"name": graph_name}))

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
        node_data: pyarrow.Table | Iterable[pyarrow.RecordBatch] | pandas.DataFrame,
        batch_size: int = 10_000,
        progress_callback: ProgressCallback = lambda x: None,
    ) -> None:
        """
        Uploads node data to the server.

        Parameters
        ----------
        graph_name : str
            The name of the import process
        node_data : pyarrow.Table | Iterable[pyarrow.RecordBatch] | DataFrame
            The node data to upload
        batch_size : int
            The number of rows per batch
        progress_callback : ProgressCallback
            A callback function that is called with the number of rows uploaded after each batch
        """
        self._upload_data(graph_name, "node", node_data, batch_size, progress_callback)

    def upload_relationships(
        self,
        graph_name: str,
        relationship_data: pyarrow.Table | Iterable[pyarrow.RecordBatch] | pandas.DataFrame,
        batch_size: int = 10_000,
        progress_callback: ProgressCallback = lambda num_rows: None,
    ) -> None:
        """
        Uploads relationship data to the server.

        Parameters
        ----------
        graph_name
            The name of the import process
        relationship_data
            The relationship data to upload
        batch_size
            The number of rows per batch
        progress_callback
            A callback function that is called with the number of rows uploaded after each batch
        """
        self._upload_data(graph_name, "relationship", relationship_data, batch_size, progress_callback)

    def upload_triplets(
        self,
        graph_name: str,
        triplet_data: pyarrow.Table | Iterable[pyarrow.RecordBatch] | pandas.DataFrame,
        batch_size: int = 10_000,
        progress_callback: ProgressCallback = lambda num_triplets: None,
    ) -> None:
        """
        Uploads triplet data to the server.

        Parameters
        ----------
        graph_name
            The name of the import process
        triplet_data : pyarrow.Table | Iterable[pyarrow.RecordBatch] | DataFrame
            The triplet data to upload
        batch_size
            The number of rows per batch
        progress_callback
            A callback function that is called with the number of rows uploaded after each batch
        """
        self._upload_data(graph_name, "triplet", triplet_data, batch_size, progress_callback)

    def advertised_connection_info(self) -> ConnectionInfo:
        """
        Returns the host and port of the GDS Arrow server.

        Returns
        -------
        ConnectionInfo
            the host and port of the GDS Arrow server
        """
        return self._flight_client.advertised_connection_info()

    def request_token(self) -> str | None:
        """
        Requests a token from the server and returns it.

        Returns
        -------
        str | None
            a token from the server and returns it.
        """

        return self._flight_client.request_token()

    def _send_action(self, action_type: str, meta_data: dict[str, Any]) -> dict[str, Any]:
        action_type = f"{ArrowEndpointVersion.V1.prefix()}{action_type}"
        raw_result = self._flight_client.do_action_with_retry(action_type, meta_data)
        return deserialize_single(raw_result)

    def _upload_data(
        self,
        graph_name: str,
        entity_type: str,
        data: pyarrow.Table | list[pyarrow.RecordBatch] | pandas.DataFrame,
        batch_size: int,
        progress_callback: ProgressCallback,
    ) -> None:
        match data:
            case pyarrow.Table():
                batches = data.to_batches(batch_size)
            case pandas.DataFrame():
                batches = pyarrow.Table.from_pandas(data).to_batches(batch_size)
            case _:
                batches = data

        flight_descriptor = {
            "name": "PUT_COMMAND",
            "version": ArrowEndpointVersion.V1.version(),
            "body": {"name": graph_name, "entity_type": entity_type},
        }
        upload_descriptor = flight.FlightDescriptor.for_command(json.dumps(flight_descriptor).encode("utf-8"))

        put_stream, ack_stream = self._flight_client.do_put_with_retry(upload_descriptor, batches[0].schema)

        @self._flight_client._retry_config.decorator(operation_name="Upload batch", logger=self._logger)
        def upload_batch(p: RecordBatch) -> None:
            put_stream.write_batch(p)

        try:
            with put_stream:
                for partition in batches:
                    upload_batch(partition)
                    ack_stream.read()
                    progress_callback(partition.num_rows)
        except Exception as e:
            handle_flight_error(e)

    def _get_data(
        self,
        graph_name: str,
        database: str,
        proc: str,
        concurrency: int | None,
        config: dict[str, Any],
    ) -> pandas.DataFrame:
        ticket = self._build_get_ticket(database, graph_name, proc, concurrency, config)
        get = self._flight_client.get_stream(ticket)
        result = self._fetch_get_result(get)
        return result

    def _build_get_ticket(
        self,
        database: str,
        graph_name: str,
        procedure_name: str,
        concurrency: int | None,
        configuration: dict[str, Any],
    ) -> flight.Ticket:
        body: dict[str, Any] = {
            "database_name": database,
            "graph_name": graph_name,
            "procedure_name": procedure_name,
            "configuration": configuration,
        }

        if concurrency:
            body["concurrency"] = concurrency

        payload = {
            "name": "GET_COMMAND",
            "version": ArrowEndpointVersion.V1.version(),
            "body": body,
        }

        return flight.Ticket(json.dumps(payload).encode("utf-8"))

    def _fetch_get_result(self, get: flight.FlightStreamReader) -> pandas.DataFrame:
        try:
            arrow_table = get.read_all()
        except Exception as e:
            handle_flight_error(e)
        arrow_table = self._sanitize_arrow_table(arrow_table)
        if SemanticVersion.from_string(pandas.__version__) >= SemanticVersion(2, 0, 0):
            return arrow_table.to_pandas(types_mapper=pandas.ArrowDtype)  # type: ignore
        else:
            arrow_table = self._sanitize_arrow_table(arrow_table)
            return arrow_table.to_pandas()  # type: ignore

    def __enter__(self) -> GdsArrowClient:
        return self

    def __exit__(
        self,
        exception_type: Type[BaseException] | None,
        exception_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        self._flight_client.close()

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


class NodeLoadDoneResult(BaseModel):
    name: str
    node_count: int


class RelationshipLoadDoneResult(BaseModel):
    name: str
    relationship_count: int


class TripletLoadDoneResult(BaseModel):
    name: str
    node_count: int
    relationship_count: int
