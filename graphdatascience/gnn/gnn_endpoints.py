from ..caller_base import CallerBase
from ..error.illegal_attr_checker import IllegalAttrChecker
from ..error.uncallable_namespace import UncallableNamespace
from .gnn_nc_runner import GNNNodeClassificationRunner


class GNNRunner(UncallableNamespace, IllegalAttrChecker):
    @property
    def nodeClassification(self) -> GNNNodeClassificationRunner:
        return GNNNodeClassificationRunner(
            self._query_runner, f"{self._namespace}.nodeClassification", self._server_version
        )


class GnnEndpoints(CallerBase):
    @property
    def gnn(self) -> GNNRunner:
        return GNNRunner(self._query_runner, f"{self._namespace}.gnn", self._server_version)
