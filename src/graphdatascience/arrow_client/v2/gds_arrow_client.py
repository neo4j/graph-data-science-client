from __future__ import annotations

import json
import logging
from types import TracebackType
from typing import Any, Type

import pandas
import pyarrow
from pyarrow import RecordBatch, flight

from graphdatascience.arrow_client.arrow_endpoint_version import ArrowEndpointVersion
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient, ConnectionInfo
from graphdatascience.query_runner.termination_flag import TerminationFlag

from ...procedure_surface.api.default_values import ALL_TYPES
from ...procedure_surface.utils.config_converter import ConfigConverter
from ..progress_callback import ProgressCallback
from .api_types import JobStatus
from .job_client import JobClient


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
        node_properties: str | list[str],
        node_labels: list[str] | None = None,
        list_node_labels: bool = False,
        concurrency: int | None = None,
        log_progress: bool = True,
        job_id: str | None = None,
    ) -> str:
        """
        Start a node property export process.

        Parameters
        ----------
        graph_name
            The name of the graph
        node_properties
            The name of the node properties to retrieve
        node_labels
            A list of node labels to filter the nodes
        list_node_labels
            A flag that indicates whether the node labels should be included in the result
        concurrency
            The number of threads used on the server side when serving the data
        log_progress
            Display progress logging.
        job_id
            Identifier for the computation.

        Returns
        -------
        str
            The JobId identifying the export process
        """
        normalized_properties = node_properties if isinstance(node_properties, list) else [node_properties]

        config = ConfigConverter.convert_to_gds_config(
            graph_name=graph_name,
            node_properties=normalized_properties,
            list_node_labels=list_node_labels,
            node_labels=node_labels,
            concurrency=concurrency,
            log_progress=log_progress,
            job_id=job_id,
        )

        return JobClient.run_job(self._flight_client, "v2/graph.nodeProperties.stream", config)

    def get_nodes(
        self,
        graph_name: str,
        *,
        node_filter: str | None = None,
        log_progress: bool = True,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> str:
        """
        Start a new export process to stream the nodes that match the filter from the graph.

        Parameters
        ----------
        graph_name
           The name of the graph
        node_filter
            A Cypher predicate for filtering nodes in the input graph.
        log_progress
            Display progress logging.
        concurrency
            Number of concurrent threads to use.
        job_id
            Identifier for the computation.

        Returns
        -------
        str
            The JobId identifying the export process
        """
        config = ConfigConverter.convert_to_gds_config(
            graph_name=graph_name,
            node_label="__IGNORED__",
            node_filter=node_filter,
            concurrency=concurrency,
            log_progress=log_progress,
            job_id=job_id,
        )

        return JobClient.run_job_and_wait(self._flight_client, "v2/graph.nodeLabel.stream", config, log_progress)

    def get_relationships(
        self,
        graph_name: str,
        relationship_types: list[str] = ALL_TYPES,
        relationship_properties: list[str] | None = None,
        *,
        concurrency: int | None = None,
        log_progress: bool = True,
        job_id: str | None = None,
    ) -> str:
        """
        Start a new export process to stream all relationships of the specified types with the specified properties.

        Parameters
        ----------
        graph_name
           The graph name.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        relationship_properties
            The relationship properties to stream. If not specified, no properties will be streamed.
        concurrency
            Number of concurrent threads to use.
        log_progress
            Display progress logging.
        job_id
            Identifier for the computation.

        Returns
        -------
        str
            The JobId identifying the export process
        """
        config_input = {
            "graph_name": graph_name,
            "relationship_types": relationship_types or ["*"],
            "concurrency": concurrency,
            "log_progress": log_progress,
            "job_id": job_id,
        }

        endpoint = "v2/graph.relationships.stream"
        if relationship_properties and relationship_properties != []:
            config_input["relationship_properties"] = relationship_properties
            endpoint = "v2/graph.relationshipProperties.stream"

        config = ConfigConverter.convert_to_gds_config(**config_input)

        return JobClient.run_job(self._flight_client, endpoint, config)

    def stream_job(self, graph_name: str, job_id: str) -> pandas.DataFrame:
        """
        Streams the results of a previously started job.

        Parameters
        ----------
        graph_name
           The graph name.
        job_id
            Identifier for the computation.

        Returns
        -------
        DataFrame
            A pandas DataFrame containing the results of the job.
        """
        return JobClient().stream_results(self._flight_client, graph_name, job_id)

    def create_graph(
        self,
        graph_name: str,
        *,
        undirected_relationship_types: list[str] | None = None,
        inverse_indexed_relationship_types: list[str] | None = None,
        concurrency: int | None = None,
        log_progress: bool = True,
        job_id: str | None = None,
    ) -> str:
        """
        Starts a new graph import process on the GDS server.

        The import process accepts separate node and relationship stream uploads.

        Parameters
        ----------
        graph_name
            The name used to identify the graph in the catalog
        undirected_relationship_types
            A list of relationship types that should be treated as undirected
        inverse_indexed_relationship_types
            A list of relationship types that should be indexed in reverse direction as well
        concurrency
            Number of concurrent threads to use.
        log_progress
            Display progress logging.
        job_id
            Identifier for the computation.

        Returns
        -------
        str
            The JobId identifying the import process
        """

        config = ConfigConverter.convert_to_gds_config(
            graph_name=graph_name,
            undirected_relationship_types=undirected_relationship_types,
            inverse_indexed_relationship_types=inverse_indexed_relationship_types,
            concurrency=concurrency,
            log_progress=log_progress,
            job_id=job_id,
        )

        return JobClient.run_job(self._flight_client, "v2/graph.project.fromTables", config)

    def create_graph_from_triplets(
        self,
        graph_name: str,
        *,
        undirected_relationship_types: list[str] | None = None,
        inverse_indexed_relationship_types: list[str] | None = None,
        concurrency: int | None = None,
        log_progress: bool = True,
        job_id: str | None = None,
    ) -> str:
        """
         Starts a new graph import process on the GDS server.

         The import process accepts triplets as input data.

        Parameters
         ----------
         graph_name : str
             The name used to identify the graph in the catalog
         undirected_relationship_types
             A list of relationship types that should be treated as undirected
         inverse_indexed_relationship_types
             A list of relationship types that should be indexed in reverse direction as well
         concurrency
             Number of concurrent threads to use.
         log_progress
             Display progress logging.
         job_id
             Identifier for the computation.

         Returns
         -------
         str
             The JobId identifying the import process
        """

        config = ConfigConverter.convert_to_gds_config(
            graph_name=graph_name,
            undirected_relationship_types=undirected_relationship_types,
            inverse_indexed_relationship_types=inverse_indexed_relationship_types,
            concurrency=concurrency,
            log_progress=log_progress,
            job_id=job_id,
        )

        return JobClient.run_job(self._flight_client, "v2/graph.project.fromTriplets", config)

    def node_load_done(self, job_id: str) -> None:
        """
        Notifies the server that all node data has been sent.

        Parameters
        ----------
        job_id
            The job id of the import process
        """
        self._flight_client.do_action_with_retry("v2/graph.project.fromTables.nodesDone", {"jobId": job_id})

    def relationship_load_done(self, job_id: str) -> None:
        """
        Notifies the server that all relationship data has been sent.

        This will trigger the finalization of the import process and make the graph or database available.

        Parameters
        ----------
        job_id
            The job id of the import process
        """
        self._flight_client.do_action_with_retry("v2/graph.project.fromTables.relationshipsDone", {"jobId": job_id})

    def triplet_load_done(self, job_id: str) -> None:
        """
        Notifies the server that all triplet data has been sent.

        This will trigger the finalization of the import process and make the graph available in the graph catalog.

        Parameters
        ----------
        job_id
            The job id of the import process
        """
        self._flight_client.do_action_with_retry("v2/graph.project.fromTriplets.done", {"jobId": job_id})

    def upload_nodes(
        self,
        job_id: str,
        data: pyarrow.Table | list[pyarrow.RecordBatch] | pandas.DataFrame,
        batch_size: int = 10000,
        progress_callback: ProgressCallback = lambda num_rows: None,
        termination_flag: TerminationFlag | None = None,
    ) -> None:
        """
        Uploads node data to the server for a given job.

        Parameters
        ----------
        job_id
            The job id of the import process
        data
            The data to upload
        batch_size
            The number of rows per batch
        progress_callback
            A callback function that is called with the number of rows uploaded after each batch
        termination_flag
            A termination flag to cancel the upload if requested
        """
        self._upload_data(
            "graph.project.fromTables.nodes", job_id, data, batch_size, progress_callback, termination_flag
        )

    def upload_relationships(
        self,
        job_id: str,
        data: pyarrow.Table | list[pyarrow.RecordBatch] | pandas.DataFrame,
        batch_size: int = 10000,
        progress_callback: ProgressCallback = lambda num_rows: None,
        termination_flag: TerminationFlag | None = None,
    ) -> None:
        """
        Uploads relationship data to the server for a given job.

        Parameters
        ----------
        job_id
            The job id of the import process
        data
            The data to upload
        batch_size
            The number of rows per batch
        progress_callback
            A callback function that is called with the number of rows uploaded after each batch
        termination_flag
            A termination flag to cancel the upload if requested
        """
        self._upload_data(
            "graph.project.fromTables.relationships", job_id, data, batch_size, progress_callback, termination_flag
        )

    def upload_triplets(
        self,
        job_id: str,
        data: pyarrow.Table | list[pyarrow.RecordBatch] | pandas.DataFrame,
        batch_size: int = 10000,
        progress_callback: ProgressCallback = lambda num_triplets: None,
        termination_flag: TerminationFlag | None = None,
    ) -> None:
        """
        Uploads triplet data to the server for a given job.

        Parameters
        ----------
        job_id
            The job id of the import process
        data
            The data to upload
        batch_size
            The number of rows per batch
        progress_callback
            A callback function that is called with the number of rows uploaded after each batch
        termination_flag
            A termination flag to cancel the upload if requested
        """
        self._upload_data("graph.project.fromTriplets", job_id, data, batch_size, progress_callback, termination_flag)

    def abort_job(self, job_id: str) -> None:
        """
        Aborts the specified process

        Parameters
        ----------
        job_id : str
            The job id of the process
        """
        self._flight_client.do_action_with_retry("v2/jobs.cancel", {"jobId": job_id})

    def job_status(self, job_id: str) -> JobStatus:
        """
        Retrieves the status of a job.

        Parameters
        ----------
        job_id
            The job id of the process

        Returns
        -------
        JobStatus
            The status of the job
        """
        return JobClient.get_job_status(self._flight_client, job_id)

    def job_summary(self, job_id: str) -> dict[str, Any]:
        """
        Retrieves the summary of a job.

        Parameters
        ----------
        job_id
            The job id of the process

        Returns
        -------
        dict[str, Any]
            The summary of the job
        """
        return JobClient.get_summary(self._flight_client, job_id)

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

    def _upload_data(
        self,
        endpoint: str,
        job_id: str,
        data: pyarrow.Table | list[pyarrow.RecordBatch] | pandas.DataFrame,
        batch_size: int = 10000,
        progress_callback: ProgressCallback = lambda num_rows: None,
        termination_flag: TerminationFlag | None = None,
    ) -> None:
        match data:
            case pyarrow.Table():
                batches = data.to_batches(batch_size)
            case pandas.DataFrame():
                batches = pyarrow.Table.from_pandas(data).to_batches(batch_size)
            case _:
                batches = data

        flight_descriptor = {
            "name": endpoint,
            "version": ArrowEndpointVersion.V2.version(),
            "body": {
                "jobId": job_id,
            },
        }
        upload_descriptor = flight.FlightDescriptor.for_command(json.dumps(flight_descriptor).encode("utf-8"))

        put_stream, ack_stream = self._flight_client.do_put_with_retry(upload_descriptor, batches[0].schema)

        @self._flight_client._retry_config.decorator(operation_name="Upload batch", logger=self._logger)
        def upload_batch(p: RecordBatch) -> None:
            put_stream.write_batch(p)

        with put_stream:
            for partition in batches:
                if termination_flag is not None and termination_flag.is_set():
                    self.abort_job(job_id)
                    # closing the put_stream should raise an error. this is a safeguard to always signal the termination to the user.
                    raise RuntimeError(f"Upload for job '{job_id}' was aborted via termination flag.")

                upload_batch(partition)
                ack_stream.read()
                progress_callback(partition.num_rows)

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
