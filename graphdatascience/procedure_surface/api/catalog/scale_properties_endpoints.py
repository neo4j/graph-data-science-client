from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pandas import DataFrame

from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.catalog.scaler_config import ScalerConfig
from graphdatascience.procedure_surface.api.default_values import ALL_LABELS
from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


class ScalePropertiesEndpoints(ABC):
    @abstractmethod
    def mutate(
        self,
        G: GraphV2,
        mutate_property: str,
        node_properties: list[str],
        scaler: str | dict[str, str | int | float] | ScalerConfig,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> ScalePropertiesMutateResult:
        """
        Runs the Scale Properties algorithm and stores the results in the graph catalog as a new node property.

        Scale Properties scales node properties using a specified scaler (e.g., MinMax, Mean, Max, Log, StdScore, Center).

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        mutate_property : str
            Name of the node property to store the results in.
        node_properties : list[str]
            The node properties to scale. Can be a list of property names or a dictionary mapping property names to configurations.
        scaler : str | dict[str, str | int | float] | ScalerConfig
            The scaler to use. Can be:
            - A string (e.g., 'MinMax', 'Mean', 'Max', 'Log', 'StdScore', 'Center')
            - A dictionary with scaler configuration (e.g., {'type': 'Log', 'offset': 1.0})
            - A ScalerConfig instance
        node_labels : list[str]
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        sudo : bool, default=False
            Disable the memory guard.
        log_progress : bool, default=True
            Display progress logging.
        username : str | None, default=None
            The username to attribute the procedure run to
        concurrency : Any | None, default=None
            Number of threads to use for running the algorithm.
        job_id : Any | None, default=None
            Identifier for the job.

        Returns
        -------
        ScalePropertiesMutateResult
            Algorithm metrics and statistics including the scaler statistics
        """
        pass

    @abstractmethod
    def stats(
        self,
        G: GraphV2,
        node_properties: list[str],
        scaler: str | dict[str, str | int | float] | ScalerConfig,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> ScalePropertiesStatsResult:
        """
        Runs the Scale Properties algorithm and returns result statistics without storing the results.

        Scale Properties scales node properties using a specified scaler (e.g., MinMax, Mean, Max, Log, StdScore, Center).

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        node_properties : list[str]
            The node properties to scale. Can be a list of property names or a dictionary mapping property names to configurations.
        scaler : str | dict[str, str | int | float] | ScalerConfig
            The scaler to use. Can be:
            - A string (e.g., 'MinMax', 'Mean', 'Max', 'Log', 'StdScore', 'Center')
            - A dictionary with scaler configuration (e.g., {'type': 'Log', 'offset': 1.0})
            - A ScalerConfig or LogScalerConfig instance
        node_labels : list[str]
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        sudo : bool
            Disable the memory guard.
        log_progress : bool
            Display progress logging.
        username : str | None, default=None
            The username to attribute the procedure run to
        concurrency : int | None, default=None
            Number of threads to use for running the algorithm.
        job_id : str | None, default=None
            Identifier for the job.

        Returns
        -------
        ScalePropertiesStatsResult
            Algorithm statistics including the scaler statistics
        """
        pass

    @abstractmethod
    def stream(
        self,
        G: GraphV2,
        node_properties: list[str],
        scaler: str | dict[str, str | int | float] | ScalerConfig,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
    ) -> DataFrame:
        """
        Executes the Scale Properties algorithm and returns a stream of results.

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        node_properties : list[str]
            The node properties to scale. Can be a list of property names or a dictionary mapping property names to configurations.
        scaler : str | dict[str, str | int | float] | ScalerConfig
            The scaler to use. Can be:
            - A string (e.g., 'MinMax', 'Mean', 'Max', 'Log', 'StdScore', 'Center')
            - A dictionary with scaler configuration (e.g., {'type': 'Log', 'offset': 1.0})
            - A ScalerConfig instance
        node_labels : list[str]
            The node labels used to select nodes for this algorithm run.
        sudo : bool
            Override memory estimation limits. Use with caution as this can lead to
            memory issues if the estimation is significantly wrong.
        log_progress : bool
            Whether to log progress of the algorithm execution
        username : str | None
            The username to attribute the procedure run to
        concurrency : int | None
            The number of concurrent threads used for the algorithm execution.
        job_id : str | None
            An identifier for the job that can be used for monitoring and cancellation

        Returns
        -------
        DataFrame
            DataFrame with nodeId and scaledProperty columns containing scaled property values.
            Each row represents a node with its corresponding scaled property values.
        """
        pass

    @abstractmethod
    def write(
        self,
        G: GraphV2,
        write_property: str,
        node_properties: list[str],
        scaler: str | dict[str, str | int | float] | ScalerConfig,
        node_labels: list[str] = ALL_LABELS,
        sudo: bool = False,
        log_progress: bool = True,
        username: str | None = None,
        concurrency: int | None = None,
        job_id: str | None = None,
        write_concurrency: int | None = None,
    ) -> ScalePropertiesWriteResult:
        """
        Runs the Scale Properties algorithm and stores the result in the Neo4j database as a new node property.

        Scale Properties scales node properties using a specified scaler (e.g., MinMax, Mean, Max, Log, StdScore, Center).

        Parameters
        ----------
        G : GraphV2
            The graph to run the algorithm on
        write_property : str
            The property name to store the scaled property values for each node in the database
        node_properties : list[str]
            The node properties to scale. Can be a list of property names or a dictionary mapping property names to configurations.
        scaler : str | dict[str, str | int | float] | ScalerConfig
            The scaler to use. Can be:
            - A string (e.g., 'MinMax', 'Mean', 'Max', 'Log', 'StdScore', 'Center')
            - A dictionary with scaler configuration (e.g., {'type': 'Log', 'offset': 1.0})
            - A ScalerConfig instance
        node_labels : list[str]
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        sudo : bool, default=False
            Disable the memory guard.
        log_progress : bool, default=True
            Display progress logging.
        username : str | None, default=None
            The username to attribute the procedure run to
        concurrency : int | None, default=None
            The number of threads to use for running the algorithm.
        job_id : str | None, default=None
            Identifier for the job.
        write_concurrency : int | None, default=None
            The number of concurrent threads used during the write phase.

        Returns
        -------
        ScalePropertiesWriteResult
            Algorithm metrics and statistics including the scaler statistics and write timing
        """
        pass

    @abstractmethod
    def estimate(
        self,
        G: GraphV2 | dict[str, Any],
        node_properties: list[str],
        scaler: str | dict[str, str | int | float] | ScalerConfig,
        node_labels: list[str] = ALL_LABELS,
        concurrency: int | None = None,
    ) -> EstimationResult:
        """
        Estimate the memory consumption of an algorithm run.

        Parameters
        ----------
        G : GraphV2 | dict[str, Any]
            The graph to run the algorithm on or a dictionary representing the graph.
        node_properties : Any
            The node properties to scale. Can be a list of property names or a dictionary mapping property names to configurations.
        scaler : str | dict[str, str | int | float] | ScalerConfig
            The scaler to use. Can be:
            - A string (e.g., 'MinMax', 'Mean', 'Max', 'Log', 'StdScore', 'Center')
            - A dictionary with scaler configuration (e.g., {'type': 'Log', 'offset': 1.0})
            - A ScalerConfig instance
        node_labels : list[str]
            The node labels used to select nodes for this algorithm run
        concurrency : int | None
            The number of concurrent threads

        Returns
        -------
        EstimationResult
            Memory estimation details
        """


class ScalePropertiesMutateResult(BaseResult):
    """Result of running Scale Properties algorithm with mutate mode."""

    compute_millis: int
    configuration: dict[str, Any]
    mutate_millis: int
    node_properties_written: int
    post_processing_millis: int
    pre_processing_millis: int
    scaler_statistics: dict[str, Any]


class ScalePropertiesStatsResult(BaseResult):
    """Result of running Scale Properties algorithm with stats mode."""

    compute_millis: int
    configuration: dict[str, Any]
    post_processing_millis: int
    pre_processing_millis: int
    scaler_statistics: dict[str, Any]


class ScalePropertiesWriteResult(BaseResult):
    """Result of running Scale Properties algorithm with write mode."""

    compute_millis: int
    configuration: dict[str, Any]
    node_properties_written: int
    post_processing_millis: int
    pre_processing_millis: int
    scaler_statistics: dict[str, Any]
    write_millis: int
