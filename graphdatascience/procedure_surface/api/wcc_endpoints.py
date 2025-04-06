from abc import ABC, abstractmethod
from typing import Any, List, Optional

from pandas import DataFrame, Series

from ...graph.graph_object import Graph


class WccEndpoints(ABC):
    """
    Abstract base class defining the API for the Weakly Connected Components (WCC) algorithm.
    """

    @abstractmethod
    def mutate(
        self,
        G: Graph,
        mutate_property: str,
        threshold: Optional[float] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        seed_property: Optional[str] = None,
        consecutive_ids: Optional[bool] = None,
        relationship_weight_property: Optional[str] = None,
    ) -> Series[Any]:
        """
        Executes the WCC algorithm and writes the results to the in-memory graph as node properties.

        Parameters
        ----------
        G : Graph
            The graph to run the algorithm on
        mutate_property : str
            The property name to store the component ID for each node
        threshold : Optional[float], default=None
            The minimum required weight to consider a relationship during traversal
        relationship_types : Optional[List[str]], default=None
            The relationship types to project
        node_labels : Optional[List[str]], default=None
            The node labels to project
        sudo : Optional[bool], default=None
            Run analysis with admin permission
        log_progress : Optional[bool], default=None
            Whether to log progress
        username : Optional[str], default=None
            The username to attribute the procedure run to
        concurrency : Optional[Any], default=None
            The number of concurrent threads
        job_id : Optional[Any], default=None
            An identifier for the job
        seed_property : Optional[str], default=None
            Defines node properties that are used as initial component identifiers
        consecutive_ids : Optional[bool], default=None
            Flag to decide whether component identifiers are mapped into a consecutive id space
        relationship_weight_property : Optional[str], default=None
            The property name that contains weight

        Returns
        -------
        Series
            Algorithm metrics and statistics
        """
        pass

    @abstractmethod
    def stats(
        self,
        G: Graph,
        threshold: Optional[float] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        seed_property: Optional[str] = None,
        consecutive_ids: Optional[bool] = None,
        relationship_weight_property: Optional[str] = None,
    ) -> Series[Any]:
        """
        Executes the WCC algorithm and returns statistics.

        Parameters
        ----------
        G : Graph
            The graph to run the algorithm on
        threshold : Optional[float], default=None
            The minimum required weight to consider a relationship during traversal
        relationship_types : Optional[List[str]], default=None
            The relationship types to project
        node_labels : Optional[List[str]], default=None
            The node labels to project
        sudo : Optional[bool], default=None
            Run analysis with admin permission
        log_progress : Optional[bool], default=None
            Whether to log progress
        username : Optional[str], default=None
            The username to attribute the procedure run to
        concurrency : Optional[Any], default=None
            The number of concurrent threads
        job_id : Optional[Any], default=None
            An identifier for the job
        seed_property : Optional[str], default=None
            Defines node properties that are used as initial component identifiers
        consecutive_ids : Optional[bool], default=None
            Flag to decide whether component identifiers are mapped into a consecutive id space
        relationship_weight_property : Optional[str], default=None
            The property name that contains weight

        Returns
        -------
        Series
            Algorithm metrics and statistics
        """
        pass

    @abstractmethod
    def stream(
        self,
        G: Graph,
        min_component_size: Optional[int] = None,
        threshold: Optional[float] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        seed_property: Optional[str] = None,
        consecutive_ids: Optional[bool] = None,
        relationship_weight_property: Optional[str] = None,
    ) -> DataFrame:
        """
        Executes the WCC algorithm and returns a stream of results.

        Parameters
        ----------
        G : Graph
            The graph to run the algorithm on
        min_component_size : Optional[int], default=None
            Don't stream components with fewer nodes than this
        threshold : Optional[float], default=None
            The minimum required weight to consider a relationship during traversal
        relationship_types : Optional[List[str]], default=None
            The relationship types to project
        node_labels : Optional[List[str]], default=None
            The node labels to project
        sudo : Optional[bool], default=None
            Run analysis with admin permission
        log_progress : Optional[bool], default=None
            Whether to log progress
        username : Optional[str], default=None
            The username to attribute the procedure run to
        concurrency : Optional[Any], default=None
            The number of concurrent threads
        job_id : Optional[Any], default=None
            An identifier for the job
        seed_property : Optional[str], default=None
            Defines node properties that are used as initial component identifiers
        consecutive_ids : Optional[bool], default=None
            Flag to decide whether component identifiers are mapped into a consecutive id space
        relationship_weight_property : Optional[str], default=None
            The property name that contains weight

        Returns
        -------
        DataFrame
            DataFrame with the algorithm results
        """
        pass

    @abstractmethod
    def write(
        self,
        G: Graph,
        write_property: str,
        min_component_size: Optional[int] = None,
        threshold: Optional[float] = None,
        relationship_types: Optional[List[str]] = None,
        node_labels: Optional[List[str]] = None,
        sudo: Optional[bool] = None,
        log_progress: Optional[bool] = None,
        username: Optional[str] = None,
        concurrency: Optional[Any] = None,
        job_id: Optional[Any] = None,
        seed_property: Optional[str] = None,
        consecutive_ids: Optional[bool] = None,
        relationship_weight_property: Optional[str] = None,
        write_concurrency: Optional[Any] = None,
        write_to_result_store: Optional[bool] = None,
    ) -> Series[Any]:
        """
        Executes the WCC algorithm and writes the results to the Neo4j database.

        Parameters
        ----------
        G : Graph
            The graph to run the algorithm on
        write_property : str
            The property name to write component IDs to
        min_component_size : Optional[int], default=None
            Don't write components with fewer nodes than this
        threshold : Optional[float], default=None
            The minimum required weight to consider a relationship during traversal
        relationship_types : Optional[List[str]], default=None
            The relationship types to project
        node_labels : Optional[List[str]], default=None
            The node labels to project
        sudo : Optional[bool], default=None
            Run analysis with admin permission
        log_progress : Optional[bool], default=None
            Whether to log progress
        username : Optional[str], default=None
            The username to attribute the procedure run to
        concurrency : Optional[Any], default=None
            The number of concurrent threads
        job_id : Optional[Any], default=None
            An identifier for the job
        seed_property : Optional[str], default=None
            Defines node properties that are used as initial component identifiers
        consecutive_ids : Optional[bool], default=None
            Flag to decide whether component identifiers are mapped into a consecutive id space
        relationship_weight_property : Optional[str], default=None
            The property name that contains weight
        write_concurrency : Optional[Any], default=None
            The number of concurrent threads during the write phase
        write_to_result_store : Optional[bool], default=None
            Whether to write the results to the result store

        Returns
        -------
        Series
            Algorithm metrics and statistics
        """
        pass
