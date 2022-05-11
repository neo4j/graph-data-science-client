from typing import Any

from pandas.core.series import Series

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
