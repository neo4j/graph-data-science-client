from typing import Any, List, Optional

from pandas import DataFrame

from ...arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from ...arrow_client.v2.write_back_client import WriteBackClient
from ...graph.graph_object import Graph
from ..api.betweenness_endpoints import (
    BetweennessEndpoints,
    BetweennessMutateResult,
    BetweennessStatsResult,
    BetweennessWriteResult,
)
from ..api.estimation_result import EstimationResult
from .node_property_endpoints import NodePropertyEndpoints


class BetweennessArrowEndpoints(BetweennessEndpoints):
    """Arrow-based implementation of Betweenness Centrality algorithm endpoints."""

    def __init__(self, arrow_client: AuthenticatedArrowClient, write_back_client: Optional[WriteBackClient] = None):
        self._node_property_endpoints = NodePropertyEndpoints(arrow_client, write_back_client)

    def mutate(
        self,
        G: Graph,
        mutate_property: str,
        sampling_size: Optional[int] = None,
        sampling_seed: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
    ) -> BetweennessMutateResult:
        """
        Executes the Betweenness Centrality algorithm and writes the results to the in-memory graph as node properties.

        Betweenness centrality measures the relative information flow that passes through a node.
        It identifies nodes that serve as bridges or intermediaries in the network by quantifying
        how often each node lies on the shortest path between other pairs of nodes.

        Parameters
        ----------
        G : Graph
            The graph to run the algorithm on
        mutate_property : str
            The property name to store the betweenness centrality score for each node
        sampling_size : Optional[int], default=None
            The number of nodes to use for sampling. If not specified, all nodes are used
        sampling_seed : Optional[int], default=None
            The seed value for sampling randomization
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
        relationship_weight_property : Optional[str], default=None
            The property name that contains relationship weights

        Returns
        -------
        BetweennessMutateResult
            Algorithm metrics and statistics including centrality distribution
        """
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            sampling_size=sampling_size,
            sampling_seed=sampling_seed,
            sudo=sudo,
            username=username,
        )

        result = self._node_property_endpoints.run_job_and_mutate(
            "v2/centrality.betweenness", G, config, mutate_property
        )

        return BetweennessMutateResult(**result)

    def stats(
        self,
        G: Graph,
        sampling_size: Optional[int] = None,
        sampling_seed: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
    ) -> BetweennessStatsResult:
        """
        Executes the Betweenness Centrality algorithm and returns result statistics without writing the result to Neo4j.

        Betweenness centrality measures the relative information flow that passes through a node.
        This stats mode allows you to analyze the distribution and statistics of betweenness centrality
        values without modifying the graph.

        Parameters
        ----------
        G : Graph
            The graph to run the algorithm on
        sampling_size : Optional[int], default=None
            The number of nodes to use for sampling. If not specified, all nodes are used
        sampling_seed : Optional[int], default=None
            The seed value for sampling randomization
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
        relationship_weight_property : Optional[str], default=None
            The property name that contains relationship weights

        Returns
        -------
        BetweennessStatsResult
            Algorithm statistics including centrality distribution
        """
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            sampling_size=sampling_size,
            sampling_seed=sampling_seed,
            sudo=sudo,
            username=username,
        )

        computation_result = self._node_property_endpoints.run_job_and_get_summary(
            "v2/centrality.betweenness", G, config
        )

        return BetweennessStatsResult(**computation_result)

    def stream(
        self,
        G: Graph,
        sampling_size: Optional[int] = None,
        sampling_seed: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
    ) -> DataFrame:
        """
        Executes the Betweenness Centrality algorithm and returns the results as a stream.

        Betweenness centrality measures the relative information flow that passes through a node.
        The stream mode returns individual node results that can be processed incrementally.

        Parameters
        ----------
        G : Graph
            The graph to run the algorithm on
        sampling_size : Optional[int], default=None
            The number of nodes to use for sampling. If not specified, all nodes are used
        sampling_seed : Optional[int], default=None
            The seed value for sampling randomization
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
        relationship_weight_property : Optional[str], default=None
            The property name that contains relationship weights

        Returns
        -------
        DataFrame
            DataFrame with nodeId and score columns containing betweenness centrality results
        """
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            sampling_size=sampling_size,
            sampling_seed=sampling_seed,
            sudo=sudo,
            username=username,
        )

        return self._node_property_endpoints.run_job_and_stream("v2/centrality.betweenness", G, config)

    def write(
        self,
        G: Graph,
        write_property: str,
        sampling_size: Optional[int] = None,
        sampling_seed: Optional[int] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        relationship_weight_property: Optional[str] = None,
        write_concurrency: Optional[Any] = None,
        write_to_result_store: Optional[bool] = None,
    ) -> BetweennessWriteResult:
        """
        Executes the Betweenness Centrality algorithm and writes the results to the Neo4j database.

        Betweenness centrality measures the relative information flow that passes through a node.
        The write mode persists the centrality scores as node properties in the Neo4j database.

        Parameters
        ----------
        G : Graph
            The graph to run the algorithm on
        write_property : str
            The property name to store the betweenness centrality score for each node
        sampling_size : Optional[int], default=None
            The number of nodes to use for sampling. If not specified, all nodes are used
        sampling_seed : Optional[int], default=None
            The seed value for sampling randomization
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
        relationship_weight_property : Optional[str], default=None
            The property name that contains relationship weights
        write_concurrency : Optional[Any], default=None
            The number of concurrent threads during the write phase
        write_to_result_store : Optional[bool], default=None
            Whether to write results to the result store

        Returns
        -------
        BetweennessWriteResult
            Algorithm metrics and statistics including centrality distribution
        """
        config = self._node_property_endpoints.create_base_config(
            G,
            concurrency=concurrency,
            job_id=job_id,
            log_progress=log_progress,
            node_labels=node_labels,
            relationship_types=relationship_types,
            relationship_weight_property=relationship_weight_property,
            sampling_size=sampling_size,
            sampling_seed=sampling_seed,
            sudo=sudo,
            username=username,
            write_to_result_store=write_to_result_store,
        )

        computation_result, write_millis = self._node_property_endpoints.run_job_and_write(
            "v2/centrality.betweenness", G, config, write_concurrency, concurrency
        )

        computation_result["writeMillis"] = write_millis

        return BetweennessWriteResult(**computation_result)

    def estimate(
        self, G: Optional[Graph] = None, projection_config: Optional[dict[str, Any]] = None
    ) -> EstimationResult:
        """
        Estimates the memory requirements for running the Betweenness Centrality algorithm.

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
        return self._node_property_endpoints.estimate("v2/centrality.betweenness.estimate", G, projection_config)
