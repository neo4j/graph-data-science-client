from typing import List, Union

import pandas
from pandas.core.series import Series

from ..model.node_regression_model import NRModel
from ..query_runner.query_runner import QueryRunner
from graphdatascience.pipeline.regression_training_pipeline import (
    RegressionTrainingPipeline,
)


class NRTrainingPipeline(RegressionTrainingPipeline):
    def selectFeatures(self, node_properties: Union[str, List[str]]) -> Series:
        query = f"{self._query_prefix()}selectFeatures($pipeline_name, $node_properties)"
        params = {"pipeline_name": self.name(), "node_properties": node_properties}

        return self._query_runner.run_query(query, params).squeeze()  # type: ignore

    def feature_properties(self) -> Series:
        pipeline_info = self._list_info()["pipelineInfo"][0]
        return pandas.Series(pipeline_info["featurePipeline"]["featureProperties"], dtype=object)

    def _query_prefix(self) -> str:
        return "CALL gds.alpha.pipeline.nodeRegression."

    def _create_trained_model(self, name: str, query_runner: QueryRunner) -> NRModel:
        return NRModel(name, query_runner, self._server_version)
