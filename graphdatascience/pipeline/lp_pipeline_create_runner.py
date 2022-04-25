from typing import Tuple

from pandas.core.series import Series

from ..caller_base import CallerBase
from ..error.illegal_attr_checker import IllegalAttrChecker
from ..error.uncallable_namespace import UncallableNamespace
from .lp_training_pipeline import LPTrainingPipeline


class LPPipelineCreateRunner(CallerBase, UncallableNamespace, IllegalAttrChecker):
    def create(self, name: str) -> Tuple[LPTrainingPipeline, Series]:
        self._namespace += ".create"

        query = f"CALL {self._namespace}($name)"
        params = {"name": name}
        result = self._query_runner.run_query(query, params).squeeze()

        return LPTrainingPipeline(name, self._query_runner, self._server_version), result
