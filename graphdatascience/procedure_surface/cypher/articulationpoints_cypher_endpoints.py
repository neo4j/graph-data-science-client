from typing import Any, List, Optional

from pandas import DataFrame

from graphdatascience.procedure_surface.cypher.estimation_utils import estimate_algorithm

from ...call_parameters import CallParameters
from ...graph.graph_object import Graph
from ...query_runner.query_runner import QueryRunner
from ..api.articulationpoints_endpoints import (
    ArticulationPointsEndpoints,
    ArticulationPointsMutateResult,
    ArticulationPointsStatsResult,
    ArticulationPointsWriteResult,
)
from ..api.estimation_result import EstimationResult
from ..utils.config_converter import ConfigConverter


class ArticulationPointsCypherEndpoints(ArticulationPointsEndpoints):
    """Cypher-based implementation of ArticulationPoints algorithm endpoints."""

    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

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
        config = ConfigConverter.convert_to_gds_config(
            mutate_property=mutate_property,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
            username=username,
        )

        # Run procedure and return results
        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        cypher_result = self._query_runner.call_procedure(
            endpoint="gds.articulationPoints.mutate", params=params
        ).squeeze()

        return ArticulationPointsMutateResult(**cypher_result.to_dict())

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
        config = ConfigConverter.convert_to_gds_config(
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
            username=username,
        )

        # Run procedure and return results
        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        cypher_result = self._query_runner.call_procedure(
            endpoint="gds.articulationPoints.stats", params=params
        ).squeeze()

        return ArticulationPointsStatsResult(**cypher_result.to_dict())

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
        Executes the ArticulationPoints algorithm and returns results as a stream.

        ArticulationPoints is an algorithm that finds nodes that disconnect components if removed.
        These nodes are critical for the connectivity of the graph. The stream mode returns
        individual articulation points with their resulting component information.

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
        DataFrame
            A DataFrame containing articulation points with columns:
            - nodeId: The ID of the articulation point
            - resultingComponents: Information about resulting components
        """
        config = ConfigConverter.convert_to_gds_config(
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
            username=username,
        )

        # Run procedure and return results
        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        return self._query_runner.call_procedure(endpoint="gds.articulationPoints.stream", params=params)

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
        config = ConfigConverter.convert_to_gds_config(
            write_property=write_property,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            sudo=sudo,
            username=username,
            write_concurrency=write_concurrency,
            write_to_result_store=write_to_result_store,
        )

        # Run procedure and return results
        params = CallParameters(graph_name=G.name(), config=config)
        params.ensure_job_id_in_config()

        cypher_result = self._query_runner.call_procedure(
            endpoint="gds.articulationPoints.write", params=params
        ).squeeze()

        return ArticulationPointsWriteResult(**cypher_result.to_dict())

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
        return estimate_algorithm(
            endpoint="gds.articulationPoints.stats.estimate",
            query_runner=self._query_runner,
            G=G,
            projection_config=projection_config,
        )
