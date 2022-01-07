from typing import Any, Dict, List, Union

from ..query_runner.query_runner import QueryRunner
from .nc_trained_pipeline import NCTrainedPipeline
from .pipeline import Pipeline


class NCPipeline(Pipeline):
    def __init__(self, name: str, query_runner: QueryRunner):
        super().__init__(name, query_runner)

    def selectFeatures(self, node_properties: Union[str, List[str]]) -> None:
        query = (
            f"{self._query_prefix()}selectFeatures($pipeline_name, $node_properties)"
        )
        params = {"pipeline_name": self.name(), "node_properties": node_properties}
        self._query_runner.run_query(query, params)

    def feature_properties(self) -> List[Dict[str, Any]]:
        return self._list_info()["modelInfo"]["featurePipeline"]["featureProperties"]  # type: ignore

    def _query_prefix(self) -> str:
        return "CALL gds.alpha.ml.pipeline.nodeClassification."

    def _create_trained_model(
        self, name: str, query_runner: QueryRunner
    ) -> NCTrainedPipeline:
        return NCTrainedPipeline(name, query_runner)
