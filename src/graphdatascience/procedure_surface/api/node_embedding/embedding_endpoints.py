from __future__ import annotations

import random
from abc import ABC, abstractmethod
from typing import Literal

from pydantic import BaseModel, Field, PositiveInt

from graphdatascience.embedding import FastRPConfig, GraphSAGEConfig, MLPClassifierConfig
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.base_result import MutateResult, NodeResult, StatsResult
from graphdatascience.procedure_surface.api.descriptions import RANDOM_SEED_DESCRIPTION, TASK_NAME_DESCRIPTION


class EmbeddingEndpoints(ABC):
    @abstractmethod
    def create(
        self,
        G: Graph,
        *,
        graph_encoder: str | FastRPConfig | None = None,
        embedding_dimension: int | None = None,
        random_seed: int | None = None,
        mutate_property: str = "embedding",
        job_id: str | None = None,
        node_labels: list[str] = ["*"],
        relationship_types: list[str] = ["*"],
        input_properties: list[str] = [],
    ) -> EmbeddingCreateResult:
        """
        Parameters
        ----------
        G
            Graph object to use
        graph_encoder
            Encoder used to produce node embeddings: either the name of a previously trained encoder model, or an inline configuration for a non-trainable encoder (e.g. FastRP or Identity).
        embedding_dimension
            Dimension of the node embeddings.
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
        input_properties
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
        graph_encoder: GraphSAGEConfig | None = None,
        decoder: MLPClassifierConfig | None = None,
        embedding_dimension: int | None = None,
        model_name: str,
        target_label: str,
        target_property: str,
        num_epochs: int | None = None,
        batch_size: int | None = None,
        num_trials: int = 1,
        random_seed: int | None = None,
        job_id: str | None = None,
        node_labels: list[str] = ["*"],
        relationship_types: list[str] = ["*"],
        input_properties: list[str] = [],
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
        embedding_dimension
            Dimension of the node embeddings.
        model_name
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
        input_properties
            Names of the node properties to use as input features
        """


class EncodeConfig(BaseModel):
    task_name: Literal["GML_ENCODE"] = Field(
        "GML_ENCODE", validation_alias="taskName", description=TASK_NAME_DESCRIPTION
    )
    graph_encoder: str | FastRPConfig | None = Field(
        description="Encoder used to produce node embeddings: either the name of a previously trained encoder model, or an inline configuration for a non-trainable encoder, i.e. FastRP."
    )
    embedding_dimension: int | None = Field(default=None, description="Dimension of the node embeddings.")
    random_seed: int = Field(default_factory=lambda: random.randint(0, 2**32 - 1), description=RANDOM_SEED_DESCRIPTION)


class EmbeddingTrainConfig(BaseModel):
    task_name: Literal["GML_TRAIN"] = Field("GML_TRAIN", validation_alias="taskName", description=TASK_NAME_DESCRIPTION)
    graph_encoder: GraphSAGEConfig | None = Field(
        default=None, description="Configuration for the graph encoder (GraphSAGE) to train."
    )
    decoder: MLPClassifierConfig | None = Field(
        default=None,
        description="Configuration for the decoder (MLP) to train on top of the graph encoder's embeddings.",
    )
    embedding_dimension: int | None = Field(default=None, description="Dimension of the node embeddings.")
    model_save_name: str | None = Field(
        default=None, description="Name to save the trained graph encoder + decoder model under."
    )
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
