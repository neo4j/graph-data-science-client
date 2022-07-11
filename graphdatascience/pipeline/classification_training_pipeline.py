from typing import Any

from pandas.core.series import Series

from .training_pipeline import TrainingPipeline


class ClassificationTrainingPipeline(TrainingPipeline):
    def addLogisticRegression(self, **config: Any) -> Series:
        query = f"{self._query_prefix()}addLogisticRegression($pipeline_name, $config)"
        params = {"pipeline_name": self.name(), "config": self._expand_ranges(config)}

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore

    def addRandomForest(self, **config: Any) -> Series:
        query_prefix = self._query_prefix().replace("beta", "alpha")
        query = f"{query_prefix}addRandomForest($pipeline_name, $config)"
        params = {"pipeline_name": self.name(), "config": self._expand_ranges(config)}

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore

    def addMLP(self, **config: Any) -> Series:
        query_prefix = self._query_prefix().replace("beta", "alpha")
        query = f"{query_prefix}addMLP($pipeline_name, $config)"
        params = {"pipeline_name": self.name(), "config": self._expand_ranges(config)}

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore
