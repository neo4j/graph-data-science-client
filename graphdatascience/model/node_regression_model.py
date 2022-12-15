from typing import Any, Dict, List

from .pipeline_model import PipelineModel


class NRModel(PipelineModel):
    def _query_prefix(self) -> str:
        return "CALL gds.alpha.pipeline.nodeRegression.predict."

    def feature_properties(self) -> List[str]:
        features: List[Dict[str, Any]] = self._list_info()["modelInfo"][0]["pipeline"]["featureProperties"]
        return [f["feature"] for f in features]
