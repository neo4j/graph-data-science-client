from typing import Any

from pandas import Series

from ..call_parameters import CallParameters
from ..error.illegal_attr_checker import IllegalAttrChecker
from ..error.uncallable_namespace import UncallableNamespace
from .lp_training_pipeline import LPTrainingPipeline


class LPPipelineCreateRunner(UncallableNamespace, IllegalAttrChecker):
    def create(self, name: str) -> tuple[LPTrainingPipeline, "Series[Any]"]:
        self._namespace += ".create"

        params = CallParameters(name=name)
        result = self._query_runner.call_procedure(endpoint=self._namespace, params=params).squeeze()

        return LPTrainingPipeline(name, self._query_runner, self._server_version), result
