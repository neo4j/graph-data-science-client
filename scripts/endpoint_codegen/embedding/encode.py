import random
from typing import Annotated, Literal

from pandas import DataFrame
from pydantic import BaseModel, Field

from graphdatascience.procedure_surface.api.base_result import MutateResult, NodeResult, StatsResult, WriteResult
from graphdatascience.procedure_surface.api.job_handle import JobHandle

from ..descriptions import (
    RANDOM_SEED_DESCRIPTION,
    TASK_NAME_DESCRIPTION,
)
from .config import (
    NoTrainGraphEncoderConfig,
)


class EncodeConfig(BaseModel):
    task_name: Literal["gml_encode"] = Field(
        "gml_encode", validation_alias="taskName", description=TASK_NAME_DESCRIPTION
    )
    graph_encoder: str | Annotated[NoTrainGraphEncoderConfig, Field(discriminator="graph_encoder_type")] = Field(
        description="Encoder used to produce node embeddings: either the name of a previously trained encoder "
        "model, or an inline configuration for a non-trainable encoder (e.g. FastRP or Identity)."
    )
    random_seed: int = Field(default_factory=lambda: random.randint(0, 2**32 - 1), description=RANDOM_SEED_DESCRIPTION)


class EncodeMutateResults(MutateResult, NodeResult):
    pass


class EncodeStatsResult(StatsResult, NodeResult):
    pass


class EncodeWriteResult(WriteResult, NodeResult):
    pass


SPEC = (
    "Encode",
    EncodeConfig,
    {
        "compute": JobHandle,
        "stream": DataFrame,
        "mutate": EncodeMutateResults,
        "stats": EncodeStatsResult,
        "write": EncodeWriteResult,
    },
    "v2/embedding.encode",
)
