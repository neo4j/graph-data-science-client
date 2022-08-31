from typing import Any, Tuple

from pandas import Series

from ..caller_base import CallerBase
from ..error.illegal_attr_checker import IllegalAttrChecker
from ..error.uncallable_namespace import UncallableNamespace
from .nr_training_pipeline import NRTrainingPipeline


class NRPipelineCreateRunner(CallerBase, UncallableNamespace, IllegalAttrChecker):
    def create(self, name: str) -> Tuple[NRTrainingPipeline, "Series[Any]"]:
        self._namespace += ".create"

        query = f"CALL {self._namespace}($name)"
        params = {"name": name}
        result = self._query_runner.run_query(query, params).squeeze()

        return NRTrainingPipeline(name, self._query_runner, self._server_version), result
