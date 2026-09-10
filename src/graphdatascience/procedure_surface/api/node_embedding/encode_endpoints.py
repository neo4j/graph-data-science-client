from __future__ import annotations

import random
from abc import ABC, abstractmethod
from typing import Annotated, Literal

from pandas import DataFrame
from pydantic import BaseModel, Field

from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.base_result import MutateResult, NodeResult, StatsResult, WriteResult
from graphdatascience.procedure_surface.api.descriptions import RANDOM_SEED_DESCRIPTION, TASK_NAME_DESCRIPTION
from graphdatascience.procedure_surface.api.job_handle import JobHandle
from graphdatascience.procedure_surface.api.node_embedding.config import (
    FastRPConfig,
    IdentityConfig,
    NoTrainGraphEncoderConfig,
)


class EncodeEndpoints(ABC):
    @abstractmethod
    def compute(
        self,
        G: Graph,
        *,
        graph_encoder: str | (FastRPConfig | IdentityConfig),
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
        graph_encoder
            Encoder used to produce node embeddings: either the name of a previously trained encoder model, or an inline configuration for a non-trainable encoder (e.g. FastRP or Identity).
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
        graph_encoder: str | (FastRPConfig | IdentityConfig),
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
        graph_encoder
            Encoder used to produce node embeddings: either the name of a previously trained encoder model, or an inline configuration for a non-trainable encoder (e.g. FastRP or Identity).
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
        graph_encoder: str | (FastRPConfig | IdentityConfig),
        random_seed: int | None = None,
        mutate_property: str,
        job_id: str | None = None,
        node_labels: list[str] = ["*"],
        relationship_types: list[str] = ["*"],
        feature_properties: list[str],
    ) -> EncodeMutateResults:
        """
        Parameters
        ----------
        G
            Graph object to use
        graph_encoder
            Encoder used to produce node embeddings: either the name of a previously trained encoder model, or an inline configuration for a non-trainable encoder (e.g. FastRP or Identity).
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
        EncodeMutateResults
        """

    @abstractmethod
    def stats(
        self,
        G: Graph,
        *,
        graph_encoder: str | (FastRPConfig | IdentityConfig),
        random_seed: int | None = None,
        job_id: str | None = None,
        node_labels: list[str] = ["*"],
        relationship_types: list[str] = ["*"],
        feature_properties: list[str],
    ) -> EncodeStatsResult:
        """
        Parameters
        ----------
        G
            Graph object to use
        graph_encoder
            Encoder used to produce node embeddings: either the name of a previously trained encoder model, or an inline configuration for a non-trainable encoder (e.g. FastRP or Identity).
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
        EncodeStatsResult
        """

    @abstractmethod
    def write(
        self,
        G: Graph,
        *,
        graph_encoder: str | (FastRPConfig | IdentityConfig),
        random_seed: int | None = None,
        write_property: str,
        write_concurrency: int | None = None,
        job_id: str | None = None,
        node_labels: list[str] = ["*"],
        relationship_types: list[str] = ["*"],
        feature_properties: list[str],
    ) -> EncodeWriteResult:
        """
        Parameters
        ----------
        G
            Graph object to use
        graph_encoder
            Encoder used to produce node embeddings: either the name of a previously trained encoder model, or an inline configuration for a non-trainable encoder (e.g. FastRP or Identity).
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
        EncodeWriteResult
        """


class EncodeConfig(BaseModel):
    task_name: Literal["GML_ENCODE"] = Field(
        "GML_ENCODE", validation_alias="taskName", description=TASK_NAME_DESCRIPTION
    )
    graph_encoder: str | Annotated[NoTrainGraphEncoderConfig, Field(discriminator="graph_encoder_type")] = Field(
        description="Encoder used to produce node embeddings: either the name of a previously trained encoder model, or an inline configuration for a non-trainable encoder (e.g. FastRP or Identity)."
    )
    random_seed: int = Field(default_factory=lambda: random.randint(0, 2**32 - 1), description=RANDOM_SEED_DESCRIPTION)


class EncodeMutateResults(MutateResult, NodeResult):
    pass


class EncodeStatsResult(StatsResult, NodeResult):
    pass


class EncodeWriteResult(WriteResult, NodeResult):
    pass
