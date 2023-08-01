import json
from typing import Any, List

from ..error.illegal_attr_checker import IllegalAttrChecker
from ..error.uncallable_namespace import UncallableNamespace


class GNNNodeClassificationRunner(UncallableNamespace, IllegalAttrChecker):
    def make_graph_sage_config(self, graph_sage_config):
        GRAPH_SAGE_DEFAULT_CONFIG = {"layer_config": {}, "num_neighbors": [25, 10], "dropout": 0.5,
                                     "hidden_channels": 256, "learning_rate": 0.003}
        final_sage_config = GRAPH_SAGE_DEFAULT_CONFIG
        if graph_sage_config:
            bad_keys = []
            for key in graph_sage_config:
                if key not in GRAPH_SAGE_DEFAULT_CONFIG:
                    bad_keys.append(key)
            if len(bad_keys) > 0:
                raise Exception(f"Argument graph_sage_config contains invalid keys {', '.join(bad_keys)}.")

            final_sage_config.update(graph_sage_config)
        return final_sage_config

    def train(
        self,
        graph_name: str,
        model_name: str,
        feature_properties: List[str],
        target_property: str,
        relationship_types: List[str],
        target_node_label: str = None,
        node_labels: List[str] = None,
        graph_sage_config = None
    ) -> "Series[Any]":  # noqa: F821
        mlConfigMap = {
            "featureProperties": feature_properties,
            "targetProperty": target_property,
            "job_type": "train",
            "nodeProperties": feature_properties + [target_property],
            "relationshipTypes": relationship_types,
            "graph_sage_config": self.make_graph_sage_config(graph_sage_config)
        }

        if target_node_label:
            mlConfigMap["targetNodeLabel"] = target_node_label
        if node_labels:
            mlConfigMap["nodeLabels"] = node_labels

        mlTrainingConfig = json.dumps(mlConfigMap)

        # token and uri will be injected by arrow_query_runner
        self._query_runner.run_query(
            "CALL gds.upload.graph($config)",
            params={
                "config": {"mlTrainingConfig": mlTrainingConfig, "graphName": graph_name, "modelName": model_name},
            },
        )

    def predict(
        self,
        graph_name: str,
        model_name: str,
        mutateProperty: str,
        predictedProbabilityProperty: str = None,
    ) -> "Series[Any]":  # noqa: F821
        mlConfigMap = {
            "job_type": "predict",
            "mutateProperty": mutateProperty
        }
        if predictedProbabilityProperty:
            mlConfigMap["predictedProbabilityProperty"] = predictedProbabilityProperty

        mlTrainingConfig = json.dumps(mlConfigMap)
        self._query_runner.run_query(
            "CALL gds.upload.graph($config)",
            params={
                "config": {"mlTrainingConfig": mlTrainingConfig, "graphName": graph_name, "modelName": model_name},
            },
        )  # type: ignore
