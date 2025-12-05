from __future__ import annotations

from typing import Any

from pandas import Series

from ..call_parameters import CallParameters
from ..error.illegal_attr_checker import IllegalAttrChecker
from ..error.uncallable_namespace import UncallableNamespace
from .nc_training_pipeline import NCTrainingPipeline


class NCPipelineCreateRunner(UncallableNamespace, IllegalAttrChecker):
    def create(self, name: str) -> tuple[NCTrainingPipeline, Series[Any]]:
        self._namespace += ".create"

        params = CallParameters(name=name)
        result = self._query_runner.call_procedure(endpoint=self._namespace, params=params).squeeze()

        return NCTrainingPipeline(name, self._query_runner, self._server_version), result
