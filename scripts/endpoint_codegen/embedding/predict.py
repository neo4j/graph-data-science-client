import random
from typing import Literal

from pandas import DataFrame
from pydantic import BaseModel, Field

from graphdatascience.procedure_surface.api.base_result import MutateResult, NodeResult, StatsResult, WriteResult
from graphdatascience.procedure_surface.api.job_handle import JobHandle

from ..descriptions import (
    RANDOM_SEED_DESCRIPTION,
    TASK_NAME_DESCRIPTION,
)


class PredictConfig(BaseModel):
    task_name: Literal["gml_predict"] = Field(
        "gml_predict", validation_alias="taskName", description=TASK_NAME_DESCRIPTION
    )
    model_name: str = Field(
        description="Name of the previously trained graph encoder + decoder model to use for prediction."
    )
    random_seed: int = Field(default_factory=lambda: random.randint(0, 2**32 - 1), description=RANDOM_SEED_DESCRIPTION)


class PredictMutateResult(MutateResult, NodeResult):
    pass


class PredictStatsResult(StatsResult, NodeResult):
    pass


class PredictWriteResult(WriteResult, NodeResult):
    pass


SPEC = (
    "Predict",
    PredictConfig,
    {
        "compute": JobHandle,
        "stream": DataFrame,
        "mutate": PredictMutateResult,
        "stats": PredictStatsResult,
        "write": PredictWriteResult,
    },
    "v2/embedding.predict",
)
