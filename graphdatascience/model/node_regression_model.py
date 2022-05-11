from typing import Any

from pandas.core.series import Series

from ..graph.graph_object import Graph
from .model import Model


class NRModel(Model):
    def _query_prefix(self) -> str:
        return "CALL gds.alpha.pipeline.nodeRegression.predict."
