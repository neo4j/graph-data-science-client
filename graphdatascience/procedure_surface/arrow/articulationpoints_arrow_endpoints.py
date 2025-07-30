from typing import Any, List, Optional

from pandas import DataFrame

from ...arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from ...arrow_client.v2.write_back_client import WriteBackClient
from ...graph.graph_object import Graph
from ..api.articulationpoints_endpoints import (
    ArticulationPointsEndpoints,
    ArticulationPointsMutateResult,
    ArticulationPointsStatsResult,
    ArticulationPointsWriteResult,
)
from ..api.estimation_result import EstimationResult
from .node_property_endpoints import NodePropertyEndpoints


class ArticulationPointsArrowEndpoints(ArticulationPointsEndpoints):
    """Arrow-based implementation of ArticulationPoints algorithm endpoints."""

    def __init__(self, arrow_client: AuthenticatedArrowClient, write_back_client: Optional[WriteBackClient] = None):
        self._node_property_endpoints = NodePropertyEndpoints(arrow_client, write_back_client)

    def mutate(
        self,
        G: Graph,
        mutate_property: str,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
    ) -> ArticulationPointsMutateResult:
        """
        Executes the ArticulationPoints algorithm and writes the results to the in-memory graph as node properties.

        ArticulationPoints is an algorithm that finds nodes that disconnect components if removed.
        These nodes are critical for the connectivity of the graph.

        Parameters
        ----------
        G : Graph
            The graph to run the algorithm on
        mutate_property : str
            The property name to store the articulation point flag for each node
        relationship_types : Optional[List[str]], default=None
            The relationship types used to select relationships for this algorithm run
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run
        sudo : Optional[bool], default=None
            Override memory estimation limits
        log_progress : Optional[bool], default=None
            Whether to log progress
        username : Optional[str], default=None
            The username to attribute the procedure run to
        concurrency : Optional[Any], default=None
            The number of concurrent threads
        job_id : Optional[Any], default=None
            An identifier for the job

        Returns
        -------
        ArticulationPointsMutateResult
            Algorithm metrics and statistics including the count of articulation points found
        """
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
            username=username,
        )

        computation_result, mutate_result = self._node_property_endpoints.run_job_and_mutate(
            "v2/centrality.articulationPoints", G, config, mutate_property
        )

        computation_result["nodePropertiesWritten"] = mutate_result.node_properties_written
        computation_result["mutateMillis"] = mutate_result.mutate_millis

        return ArticulationPointsMutateResult(**computation_result)

    def stats(
        self,
        G: Graph,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
    ) -> ArticulationPointsStatsResult:
        """
        Executes the ArticulationPoints algorithm and returns result statistics without writing the result to Neo4j.

        ArticulationPoints is an algorithm that finds nodes that disconnect components if removed.
        These nodes are critical for the connectivity of the graph.

        Parameters
        ----------
        G : Graph
            The graph to run the algorithm on
        relationship_types : Optional[List[str]], default=None
            The relationship types used to select relationships for this algorithm run
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run
        sudo : Optional[bool], default=None
            Override memory estimation limits
        log_progress : Optional[bool], default=None
            Whether to log progress
        username : Optional[str], default=None
            The username to attribute the procedure run to
        concurrency : Optional[Any], default=None
            The number of concurrent threads
        job_id : Optional[Any], default=None
            An identifier for the job

        Returns
        -------
        ArticulationPointsStatsResult
            Algorithm statistics including the count of articulation points found
        """
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
            username=username,
        )

        computation_result = self._node_property_endpoints.run_job_and_get_summary(
            "v2/centrality.articulationPoints", G, config
        )

        return ArticulationPointsStatsResult(**computation_result)

    def stream(
        self,
        G: Graph,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
    ) -> DataFrame:
        """
        Stream mode is not supported for ArticulationPoints in arrow implementation.

        The result columns cannot be preserved with the current arrow implementation.
        Use cypher endpoints for stream functionality.

        Raises
        ------
        NotImplementedError
            Stream mode is not supported for ArticulationPoints arrow endpoints
        """
        raise NotImplementedError(
            "Stream mode is not supported for ArticulationPoints arrow endpoints. "
            "The result columns cannot be preserved with the current implementation. "
            "Use cypher endpoints for stream functionality."
        )

    def write(
        self,
        G: Graph,
        write_property: str,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        write_concurrency: Optional[Any] = None,
        write_to_result_store: Optional[bool] = None,
    ) -> ArticulationPointsWriteResult:
        """
        Executes the ArticulationPoints algorithm and writes the results back to the Neo4j database.

        ArticulationPoints is an algorithm that finds nodes that disconnect components if removed.
        These nodes are critical for the connectivity of the graph.

        Parameters
        ----------
        G : Graph
            The graph to run the algorithm on
        write_property : str
            The property name to store the articulation point flag for each node
        relationship_types : Optional[List[str]], default=None
            The relationship types used to select relationships for this algorithm run
        node_labels : Optional[List[str]], default=None
            The node labels used to select nodes for this algorithm run
        sudo : Optional[bool], default=None
            Override memory estimation limits
        log_progress : Optional[bool], default=None
            Whether to log progress
        username : Optional[str], default=None
            The username to attribute the procedure run to
        concurrency : Optional[Any], default=None
            The number of concurrent threads
        job_id : Optional[Any], default=None
            An identifier for the job
        write_concurrency : Optional[Any], default=None
            The number of concurrent threads for writing
        write_to_result_store : Optional[bool], default=None
            Whether to write results to the result store

        Returns
        -------
        ArticulationPointsWriteResult
            Algorithm metrics and statistics including the count of articulation points found
        """
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
            username=username,
            write_to_result_store=write_to_result_store,
        )

        computation_result, write_millis = self._node_property_endpoints.run_job_and_write(
            "v2/centrality.articulationPoints", G, config, write_concurrency, concurrency
        )

        computation_result["writeMillis"] = write_millis

        return ArticulationPointsWriteResult(**computation_result)

    def estimate(
        self, G: Optional[Graph] = None, projection_config: Optional[dict[str, Any]] = None
    ) -> EstimationResult:
        """
        Estimates the memory requirements for running the ArticulationPoints algorithm.

        Parameters
        ----------
        G : Optional[Graph], default=None
            The graph to estimate memory requirements for
        projection_config : Optional[dict[str, Any]], default=None
            Configuration for graph projection

        Returns
        -------
        EstimationResult
            Memory estimation results
        """
        return self._node_property_endpoints.estimate("v2/centrality.articulationPoints.estimate", G, projection_config)
