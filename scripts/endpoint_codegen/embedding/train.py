import random
from typing import Annotated, Literal

from pydantic import BaseModel, Field, PositiveInt

from graphdatascience.procedure_surface.api.job_handle import JobHandle

from ..descriptions import (
    RANDOM_SEED_DESCRIPTION,
    TASK_NAME_DESCRIPTION,
)
from .config import (
    DecoderConfig,
    GraphEncoderConfig,
)


class TrainConfig(BaseModel):
    task_name: Literal["gml_train"] = Field("gml_train", validation_alias="taskName", description=TASK_NAME_DESCRIPTION)
    graph_encoder: Annotated[GraphEncoderConfig, Field(discriminator="graph_encoder_type")] = Field(
        description="Configuration for the graph encoder to train (e.g. FastRP, GraphSAGE, or Identity)."
    )
    decoder: Annotated[DecoderConfig, Field(discriminator="decoder_type")] = Field(
        description="Configuration for the decoder to train on top of the graph encoder's embeddings."
    )
    model_save_name: str = Field(
        description="Name to save the trained graph encoder + decoder model under."
    )  # todo: generate if not given
    target_label: str = Field(description="Node label to train on.")
    target_property: str = Field(description="Node property to train on.")
    num_epochs: PositiveInt | None = Field(default=None, description="Maximum number of training epochs.")
    batch_size: PositiveInt | None = Field(default=None, description="Number of examples per training batch.")
    num_trials: PositiveInt = Field(default=1, description="Number of hyperparameter tuning trials to run.")
    random_seed: int = Field(default_factory=lambda: random.randint(0, 2**32 - 1), description=RANDOM_SEED_DESCRIPTION)


SPEC = ("Train", TrainConfig, {"compute": JobHandle, "__call__": None}, "v2/embedding.train")
