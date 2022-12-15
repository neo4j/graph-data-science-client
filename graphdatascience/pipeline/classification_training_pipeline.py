from abc import ABC
from typing import Any

from pandas import Series

from .training_pipeline import MODEL_TYPE, TrainingPipeline


class ClassificationTrainingPipeline(TrainingPipeline[MODEL_TYPE], ABC):
    def addLogisticRegression(self, **config: Any) -> "Series[Any]":
        query = f"{self._query_prefix()}addLogisticRegression($pipeline_name, $config)"
        params = {"pipeline_name": self.name(), "config": self._expand_ranges(config)}

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore

    def addRandomForest(self, **config: Any) -> "Series[Any]":
        query_prefix = self._query_prefix().replace("beta", "alpha")
        query = f"{query_prefix}addRandomForest($pipeline_name, $config)"
        params = {"pipeline_name": self.name(), "config": self._expand_ranges(config)}

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore

    def addMLP(self, **config: Any) -> "Series[Any]":
        query_prefix = self._query_prefix().replace("beta", "alpha")
        query = f"{query_prefix}addMLP($pipeline_name, $config)"
        params = {"pipeline_name": self.name(), "config": self._expand_ranges(config)}

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore
