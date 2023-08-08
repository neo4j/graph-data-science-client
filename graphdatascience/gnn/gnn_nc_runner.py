import json
import time
from typing import Any, List

from ..error.illegal_attr_checker import IllegalAttrChecker
from ..error.uncallable_namespace import UncallableNamespace


class GNNNodeClassificationRunner(UncallableNamespace, IllegalAttrChecker):
    def make_graph_sage_config(self, graph_sage_config):
        GRAPH_SAGE_DEFAULT_CONFIG = {
            "layer_config": {},
            "num_neighbors": [25, 10],
            "dropout": 0.5,
            "hidden_channels": 256,
            "learning_rate": 0.003,
        }
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

    def watch_logs(self, job_id: str, logging_interval: int = 5):
        print(f"Watching logs of job {job_id}.")
        print("This needs to be interrupted manually in order to continue (for example when training is done).")

        def get_logs(offset) -> "Series[Any]":  # noqa: F821
            return self._query_runner.run_query(
                "RETURN gds.remoteml.getLogs($job_id, $offset)", params={"job_id": job_id, "offset": offset}
            ).squeeze()

        received_logs = 0
        training_done = False
        while not training_done:
            time.sleep(logging_interval)
            for log in get_logs(offset=received_logs):
                print(log)
                received_logs += 1
        return job_id

    def train(
        self,
        graph_name: str,
        model_name: str,
        feature_properties: List[str],
        target_property: str,
        relationship_types: List[str],
        target_node_label: str = None,
        node_labels: List[str] = None,
        graph_sage_config=None,
    ) -> str:
        mlConfigMap = {
            "featureProperties": feature_properties,
            "targetProperty": target_property,
            "job_type": "train",
            "nodeProperties": feature_properties + [target_property],
            "relationshipTypes": relationship_types,
            "graph_sage_config": self.make_graph_sage_config(graph_sage_config),
        }

        if target_node_label:
            mlConfigMap["targetNodeLabel"] = target_node_label
        if node_labels:
            mlConfigMap["nodeLabels"] = node_labels

        mlTrainingConfig = json.dumps(mlConfigMap)

        # token and uri will be injected by arrow_query_runner
        job_id = self._query_runner.run_query(
            "CALL gds.upload.graph($config) YIELD jobId",
            params={
                "config": {"mlTrainingConfig": mlTrainingConfig, "graphName": graph_name, "modelName": model_name},
            },
        ).jobId[0]

        print(f"Started job with jobId={job_id}. Use `gds.gnn.nodeClassification.watch_logs` to track progress.")
        return job_id

    def predict(
        self,
        graph_name: str,
        model_name: str,
        mutateProperty: str,
        predictedProbabilityProperty: str = None,
    ) -> str:
        mlConfigMap = {"job_type": "predict", "mutateProperty": mutateProperty}
        if predictedProbabilityProperty:
            mlConfigMap["predictedProbabilityProperty"] = predictedProbabilityProperty

        mlTrainingConfig = json.dumps(mlConfigMap)
        job_id = self._query_runner.run_query(
            "CALL gds.upload.graph($config) YIELD jobId",
            params={
                "config": {"mlTrainingConfig": mlTrainingConfig, "graphName": graph_name, "modelName": model_name},
            },
        ).jobId[0]

        print(f"Started job with jobId={job_id}. Use `gds.gnn.nodeClassification.watch_logs` to track progress.")
        return job_id
