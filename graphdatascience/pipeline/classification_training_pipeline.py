from abc import ABC
from typing import Any

from pandas import Series

from ..server_version.server_version import ServerVersion
from .training_pipeline import MODEL_TYPE, TrainingPipeline


class ClassificationTrainingPipeline(TrainingPipeline[MODEL_TYPE], ABC):
    def addLogisticRegression(self, **config: Any) -> "Series[Any]":
        """
        Add a logistic regression model candidate to the pipeline.

        Args:
            **config: The configuration for the logistic regression model.

        Returns:
            The result of the query.
        """
        query = f"{self._query_prefix()}addLogisticRegression($pipeline_name, $config)"
        params = {"pipeline_name": self.name(), "config": self._expand_ranges(config)}

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore

    def addRandomForest(self, **config: Any) -> "Series[Any]":
        """
        Add a random forest model candidate to the pipeline.

        Args:
            **config: The configuration for the random forest model.

        Returns:
            The result of the query.

        """
        query_prefix = self._query_prefix()
        if self._server_version < ServerVersion(2, 4, 0):
            query_prefix = self._query_prefix().replace("beta", "alpha")
        query = f"{query_prefix}addRandomForest($pipeline_name, $config)"
        params = {"pipeline_name": self.name(), "config": self._expand_ranges(config)}

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore

    def addMLP(self, **config: Any) -> "Series[Any]":
        """
        Add a multi-layer perceptron model candidate to the pipeline.

        Args:
            **config: The configuration for the multi-layer perceptron model.

        Returns:
            The result of the query.

        """
        query_prefix = self._query_prefix().replace("beta", "alpha")
        query = f"{query_prefix}addMLP($pipeline_name, $config)"
        params = {"pipeline_name": self.name(), "config": self._expand_ranges(config)}

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore
