from typing import Any, Optional

from pandas import DataFrame, Series

from ..error.illegal_attr_checker import IllegalAttrChecker
from ..error.uncallable_namespace import UncallableNamespace
from ..model.pipeline_model import PipelineModel
from .lp_pipeline_create_runner import LPPipelineCreateRunner
from .nc_pipeline_create_runner import NCPipelineCreateRunner
from .training_pipeline import TrainingPipeline


class PipelineBetaProcRunner(UncallableNamespace, IllegalAttrChecker):
    @property
    def linkPrediction(self) -> LPPipelineCreateRunner:
        return LPPipelineCreateRunner(self._query_runner, f"{self._namespace}.linkPrediction", self._server_version)

    @property
    def nodeClassification(self) -> NCPipelineCreateRunner:
        return NCPipelineCreateRunner(self._query_runner, f"{self._namespace}.nodeClassification", self._server_version)

    def list(self, pipeline: Optional[TrainingPipeline[PipelineModel]] = None) -> DataFrame:
        self._namespace += ".list"

        if pipeline:
            query = f"CALL {self._namespace}($pipeline_name)"
            params = {"pipeline_name": pipeline.name()}
        else:
            query = f"CALL {self._namespace}()"
            params = {}

        return self._query_runner.run_query(query, params)

    def exists(self, pipeline_name: str) -> "Series[Any]":
        self._namespace += ".exists"

        query = f"CALL {self._namespace}($pipeline_name)"
        params = {"pipeline_name": pipeline_name}

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore

    def drop(self, pipeline: TrainingPipeline[PipelineModel]) -> "Series[Any]":
        self._namespace += ".drop"

        query = f"CALL {self._namespace}($pipeline_name)"
        params = {"pipeline_name": pipeline.name()}

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore
