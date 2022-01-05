from typing import Any, Dict, List

from ..query_runner.query_runner import QueryRunner


class LPPipeline:
    QUERY_PREFIX = "CALL gds.alpha.ml.pipeline.linkPrediction."

    def __init__(self, name: str, query_runner: QueryRunner):
        self._name = name
        self._query_runner = query_runner

    def name(self) -> str:
        return self._name

    def addNodeProperty(self, procedure_name: str, **config: Any) -> None:
        query = f"{self.QUERY_PREFIX}addNodeProperty($pipeline_name, $procedure_name, $config)"
        params = {
            "pipeline_name": self.name(),
            "procedure_name": procedure_name,
            "config": config,
        }
        self._query_runner.run_query(query, params)

    def node_property_steps(self) -> List[Dict[str, Any]]:
        query = "CALL gds.beta.model.list($name)"
        params = {"name": self.name()}
        model_info = self._query_runner.run_query(query, params)[0]["modelInfo"]

        return model_info["featurePipeline"]["nodePropertySteps"]  # type: ignore
