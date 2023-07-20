import json
from typing import Any, List

from ..error.illegal_attr_checker import IllegalAttrChecker
from ..error.uncallable_namespace import UncallableNamespace


class GNNNodeClassificationRunner(UncallableNamespace, IllegalAttrChecker):
    def train(
        self,
        graph_name: str,
        model_name: str,
        feature_properties: List[str],
        target_property: str,
        relationship_types: List[str],
        target_node_label: str = None,
        node_labels: List[str] = None,
    ) -> "Series[Any]":  # noqa: F821
        mlConfigMap = {
            "featureProperties": feature_properties,
            "targetProperty": target_property,
            "job_type": "train",
            "nodeProperties": feature_properties + [target_property],
            "relationshipTypes": relationship_types
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
        feature_properties: List[str],
        relationship_types: List[str],
        mutateProperty: str,
        target_node_label: str = None,
        node_labels: List[str] = None,
    ) -> "Series[Any]":  # noqa: F821
        mlConfigMap = {
            "featureProperties": feature_properties,
            "job_type": "predict",
            "nodeProperties": feature_properties,
            "relationshipTypes": relationship_types,
            "mutateProperty": mutateProperty
        }
        if target_node_label:
            mlConfigMap["targetNodeLabel"] = target_node_label
        if node_labels:
            mlConfigMap["nodeLabels"] = node_labels

        mlTrainingConfig = json.dumps(mlConfigMap)
        self._query_runner.run_query(
            "CALL gds.upload.graph($config)",
            params={
                "config": {"mlTrainingConfig": mlTrainingConfig, "graphName": graph_name, "modelName": model_name},
            },
        )  # type: ignore
