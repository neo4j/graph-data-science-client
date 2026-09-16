from __future__ import annotations

import random
from abc import ABC, abstractmethod
from typing import Annotated, Literal

from pydantic import BaseModel, Field, PositiveInt

from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.base_result import MutateResult, NodeResult, StatsResult
from graphdatascience.procedure_surface.api.descriptions import RANDOM_SEED_DESCRIPTION, TASK_NAME_DESCRIPTION
from graphdatascience.procedure_surface.api.node_embedding.config import (
    DecoderConfig,
    FastRPConfig,
    GBClassifierConfig,
    GraphEncoderConfig,
    GraphSAGEConfig,
    IdentityConfig,
    MLPClassifierConfig,
    NoTrainGraphEncoderConfig,
)


class EmbeddingEndpoints(ABC):
    @abstractmethod
    def create(
        self,
        G: Graph,
        *,
        graph_encoder: str | (FastRPConfig | IdentityConfig),
        random_seed: int | None = None,
        mutate_property: str,
        job_id: str | None = None,
        node_labels: list[str] = ["*"],
        relationship_types: list[str] = ["*"],
        feature_properties: list[str] = [],
    ) -> EmbeddingCreateResult:
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
        EmbeddingCreateResult
        """

    @abstractmethod
    def train(
        self,
        G: Graph,
        *,
        graph_encoder: FastRPConfig | GraphSAGEConfig | IdentityConfig,
        decoder: GBClassifierConfig | MLPClassifierConfig,
        model_save_name: str,
        target_label: str,
        target_property: str,
        num_epochs: int | None = None,
        batch_size: int | None = None,
        num_trials: int = 1,
        random_seed: int | None = None,
        job_id: str | None = None,
        node_labels: list[str] = ["*"],
        relationship_types: list[str] = ["*"],
        feature_properties: list[str] = [],
    ) -> EmbeddingTrainResult:
        """
        embeddings.train is a preview feature and may change or be removed in future releases.

        Parameters
        ----------
        G
            Graph object to use
        graph_encoder
            Configuration for the graph encoder to train (e.g. FastRP, GraphSAGE, or Identity).
        decoder
            Configuration for the decoder to train on top of the graph encoder's embeddings.
        model_save_name
            Name to save the trained graph encoder + decoder model under.
        target_label
            Node label to train on.
        target_property
            Node property to train on.
        num_epochs
            Maximum number of training epochs.
        batch_size
            Number of examples per training batch.
        num_trials
            Number of hyperparameter tuning trials to run.
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
        """


class EncodeConfig(BaseModel):
    task_name: Literal["GML_ENCODE"] = Field(
        "GML_ENCODE", validation_alias="taskName", description=TASK_NAME_DESCRIPTION
    )
    graph_encoder: str | Annotated[NoTrainGraphEncoderConfig, Field(discriminator="graph_encoder_type")] = Field(
        description="Encoder used to produce node embeddings: either the name of a previously trained encoder model, or an inline configuration for a non-trainable encoder (e.g. FastRP or Identity)."
    )
    random_seed: int = Field(default_factory=lambda: random.randint(0, 2**32 - 1), description=RANDOM_SEED_DESCRIPTION)


class EmbeddingTrainConfig(BaseModel):
    task_name: Literal["GML_TRAIN"] = Field("GML_TRAIN", validation_alias="taskName", description=TASK_NAME_DESCRIPTION)
    graph_encoder: Annotated[GraphEncoderConfig, Field(discriminator="graph_encoder_type")] = Field(
        description="Configuration for the graph encoder to train (e.g. FastRP, GraphSAGE, or Identity)."
    )
    decoder: Annotated[DecoderConfig, Field(discriminator="decoder_type")] = Field(
        description="Configuration for the decoder to train on top of the graph encoder's embeddings."
    )
    model_save_name: str = Field(description="Name to save the trained graph encoder + decoder model under.")
    target_label: str = Field(description="Node label to train on.")
    target_property: str = Field(description="Node property to train on.")
    num_epochs: PositiveInt | None = Field(default=None, description="Maximum number of training epochs.")
    batch_size: PositiveInt | None = Field(default=None, description="Number of examples per training batch.")
    num_trials: PositiveInt = Field(default=1, description="Number of hyperparameter tuning trials to run.")
    random_seed: int = Field(default_factory=lambda: random.randint(0, 2**32 - 1), description=RANDOM_SEED_DESCRIPTION)


class EmbeddingCreateResult(MutateResult, NodeResult):
    pass


class EmbeddingTrainResult(StatsResult):
    pass
