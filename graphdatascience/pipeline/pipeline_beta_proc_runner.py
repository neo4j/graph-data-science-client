from typing import Any, Optional

from pandas import DataFrame, Series

from ..error.illegal_attr_checker import IllegalAttrChecker
from ..error.uncallable_namespace import UncallableNamespace
from ..model.pipeline_model import PipelineModel
from .lp_pipeline_create_runner import LPPipelineCreateRunner
from .nc_pipeline_create_runner import NCPipelineCreateRunner
from .pipeline_proc_runner import PipelineProcRunner
from .training_pipeline import TrainingPipeline


class PipelineBetaProcRunner(UncallableNamespace, IllegalAttrChecker):
    @property
    def linkPrediction(self) -> LPPipelineCreateRunner:
        return LPPipelineCreateRunner(self._query_runner, f"{self._namespace}.linkPrediction", self._server_version)

    @property
    def nodeClassification(self) -> NCPipelineCreateRunner:
        return NCPipelineCreateRunner(self._query_runner, f"{self._namespace}.nodeClassification", self._server_version)

    def list(self, pipeline: Optional[TrainingPipeline[PipelineModel]] = None) -> DataFrame:
        return PipelineProcRunner(self._query_runner, self._namespace, self._server_version).list(pipeline)

    def exists(self, pipeline_name: str) -> "Series[Any]":
        return PipelineProcRunner(self._query_runner, self._namespace, self._server_version).exists(pipeline_name)

    def drop(self, pipeline: TrainingPipeline[PipelineModel]) -> "Series[Any]":
        return PipelineProcRunner(self._query_runner, self._namespace, self._server_version).drop(pipeline)
