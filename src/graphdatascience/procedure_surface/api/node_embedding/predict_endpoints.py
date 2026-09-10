from __future__ import annotations

import random
from abc import ABC, abstractmethod
from typing import Literal

from pandas import DataFrame
from pydantic import BaseModel, Field

from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.base_result import MutateResult, NodeResult, StatsResult, WriteResult
from graphdatascience.procedure_surface.api.descriptions import RANDOM_SEED_DESCRIPTION, TASK_NAME_DESCRIPTION
from graphdatascience.procedure_surface.api.job_handle import JobHandle


class PredictEndpoints(ABC):
    @abstractmethod
    def compute(
        self,
        G: Graph,
        *,
        model_name: str,
        random_seed: int | None = None,
        job_id: str | None = None,
        node_labels: list[str] = ["*"],
        relationship_types: list[str] = ["*"],
        feature_properties: list[str],
    ) -> JobHandle:
        """
        Parameters
        ----------
        G
            Graph object to use
        model_name
            Name of the model.
        random_seed
            Seed for random number generation to ensure reproducible results.
        job_id
            Identifier for the computation.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        feature_properties
            Names of the node properties to use as input features

        Returns
        -------
        JobHandle
        """

    @abstractmethod
    def stream(
        self,
        G: Graph,
        *,
        model_name: str,
        random_seed: int | None = None,
        job_id: str | None = None,
        node_labels: list[str] = ["*"],
        relationship_types: list[str] = ["*"],
        feature_properties: list[str],
    ) -> DataFrame:
        """
        Parameters
        ----------
        G
            Graph object to use
        model_name
            Name of the model.
        random_seed
            Seed for random number generation to ensure reproducible results.
        job_id
            Identifier for the computation.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        feature_properties
            Names of the node properties to use as input features

        Returns
        -------
        DataFrame
        """

    @abstractmethod
    def mutate(
        self,
        G: Graph,
        *,
        model_name: str,
        random_seed: int | None = None,
        mutate_property: str,
        job_id: str | None = None,
        node_labels: list[str] = ["*"],
        relationship_types: list[str] = ["*"],
        feature_properties: list[str],
    ) -> PredictMutateResult:
        """
        Parameters
        ----------
        G
            Graph object to use
        model_name
            Name of the model.
        random_seed
            Seed for random number generation to ensure reproducible results.
        mutate_property
            Name of the node property to store the results in.
        job_id
            Identifier for the computation.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        feature_properties
            Names of the node properties to use as input features

        Returns
        -------
        PredictMutateResult
        """

    @abstractmethod
    def stats(
        self,
        G: Graph,
        *,
        model_name: str,
        random_seed: int | None = None,
        job_id: str | None = None,
        node_labels: list[str] = ["*"],
        relationship_types: list[str] = ["*"],
        feature_properties: list[str],
    ) -> PredictStatsResult:
        """
        Parameters
        ----------
        G
            Graph object to use
        model_name
            Name of the model.
        random_seed
            Seed for random number generation to ensure reproducible results.
        job_id
            Identifier for the computation.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        feature_properties
            Names of the node properties to use as input features

        Returns
        -------
        PredictStatsResult
        """

    @abstractmethod
    def write(
        self,
        G: Graph,
        *,
        model_name: str,
        random_seed: int | None = None,
        write_property: str,
        write_concurrency: int | None = None,
        job_id: str | None = None,
        node_labels: list[str] = ["*"],
        relationship_types: list[str] = ["*"],
        feature_properties: list[str],
    ) -> PredictWriteResult:
        """
        Parameters
        ----------
        G
            Graph object to use
        model_name
            Name of the model.
        random_seed
            Seed for random number generation to ensure reproducible results.
        write_property
            Name of the node property to store the results in.
        write_concurrency
            Number of concurrent threads to use for writing.
        job_id
            Identifier for the computation.
        node_labels
            Filter the graph using the given node labels. Nodes with any of the given labels will be included.
        relationship_types
            Filter the graph using the given relationship types. Relationships with any of the given types will be included.
        feature_properties
            Names of the node properties to use as input features

        Returns
        -------
        PredictWriteResult
        """


class PredictConfig(BaseModel):
    task_name: Literal["GML_PREDICT"] = Field(
        "GML_PREDICT", validation_alias="taskName", description=TASK_NAME_DESCRIPTION
    )
    model_name: str = Field(description="Name of the model")
    random_seed: int = Field(default_factory=lambda: random.randint(0, 2**32 - 1), description=RANDOM_SEED_DESCRIPTION)


class PredictMutateResult(MutateResult, NodeResult):
    pass


class PredictStatsResult(StatsResult, NodeResult):
    pass


class PredictWriteResult(WriteResult, NodeResult):
    pass
