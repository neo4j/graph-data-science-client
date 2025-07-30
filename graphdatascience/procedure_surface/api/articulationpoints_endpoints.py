from abc import ABC, abstractmethod
from typing import Any, List, Optional

from pandas import DataFrame
from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel

from ...graph.graph_object import Graph


class ArticulationPointsEndpoints(ABC):
    """
    Abstract base class defining the API for the Articulation Points algorithm.
    """

    @abstractmethod
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
    ) -> "ArticulationPointsMutateResult":
        """
        Executes the ArticulationPoints algorithm and writes the results to the in-memory graph as node properties.

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

    @abstractmethod
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
    ) -> "ArticulationPointsStatsResult":
        """
        Executes the ArticulationPoints algorithm and returns result statistics without writing the result to Neo4j.

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

    @abstractmethod
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
    ) -> "DataFrame":
        """
        Executes the ArticulationPoints algorithm and returns results as a stream.

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

    @abstractmethod
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
    ) -> "ArticulationPointsWriteResult":
        """
        Executes the ArticulationPoints algorithm and writes the results back to the Neo4j database.

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


class ArticulationPointsMutateResult(BaseModel):
    """Result of running ArticulationPoints algorithm with mutate mode."""

    model_config = ConfigDict(alias_generator=to_camel)

    articulation_point_count: int
    node_properties_written: int
    mutate_millis: int
    compute_millis: int
    configuration: dict[str, Any]

    def __getitem__(self, item: str) -> Any:
        return self.__dict__[item]


class ArticulationPointsStatsResult(BaseModel):
    """Result of running ArticulationPoints algorithm with stats mode."""

    model_config = ConfigDict(alias_generator=to_camel)

    articulation_point_count: int
    compute_millis: int
    configuration: dict[str, Any]

    def __getitem__(self, item: str) -> Any:
        return self.__dict__[item]


class ArticulationPointsWriteResult(BaseModel):
    """Result of running ArticulationPoints algorithm with write mode."""

    model_config = ConfigDict(alias_generator=to_camel)

    articulation_point_count: int
    node_properties_written: int
    write_millis: int
    compute_millis: int
    configuration: dict[str, Any]

    def __getitem__(self, item: str) -> Any:
        return self.__dict__[item]
