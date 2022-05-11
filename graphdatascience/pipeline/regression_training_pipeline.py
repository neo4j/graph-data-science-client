from abc import ABC, abstractmethod
from typing import Any, Dict, Tuple

import pandas
from pandas.core.frame import DataFrame
from pandas.core.series import Series

from ..graph.graph_object import Graph
from ..model.model import Model
from ..query_runner.query_runner import QueryRunner
from ..server_version.server_version import ServerVersion
from .training_pipeline import TrainingPipeline


class RegressionTrainingPipeline(TrainingPipeline):
    def addLinearRegression(self, **config: Any) -> Series:
        query = f"{self._query_prefix()}addLinearRegression($pipeline_name, $config)"
        params = {"pipeline_name": self.name(), "config": self._expand_ranges(config)}

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore

    def addRandomForest(self, **config: Any) -> Series:
        query = f"{self._query_prefix()}addRandomForest($pipeline_name, $config)"
        params = {"pipeline_name": self.name(), "config": self._expand_ranges(config)}

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore
