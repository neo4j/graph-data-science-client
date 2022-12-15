from dataclasses import dataclass
from typing import Any, Dict, List

from .pipeline_model import PipelineModel


@dataclass(frozen=True, repr=True)
class LinkFeature:

    name: str
    config: Dict[str, Any]

    def __str__(self) -> str:
        return f"({self.name}, {self.config})"


class LPModel(PipelineModel):
    def _query_prefix(self) -> str:
        return "CALL gds.beta.pipeline.linkPrediction.predict."

    def link_features(self) -> List[LinkFeature]:
        steps: List[Dict[str, Any]] = self._list_info()["modelInfo"][0]["pipeline"]["featureSteps"]
        return [LinkFeature(s["name"], s["config"]) for s in steps]
