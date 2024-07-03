import logging
import os
import time
from typing import Any, Dict, Optional

import requests
from pandas import Series

from ..error.client_only_endpoint import client_only_endpoint
from ..error.illegal_attr_checker import IllegalAttrChecker
from ..error.uncallable_namespace import UncallableNamespace
from ..graph.graph_object import Graph
from ..query_runner.query_runner import QueryRunner
from ..server_version.compatible_with import compatible_with
from ..server_version.server_version import ServerVersion

logging.basicConfig(level=logging.INFO)


class KgeRunner(UncallableNamespace, IllegalAttrChecker):
    def __init__(
        self,
        query_runner: QueryRunner,
        namespace: str,
        server_version: ServerVersion,
        compute_cluster_ip: str,
        encrypted_db_password: str,
        arrow_uri: str,
    ):
        print("!init", flush=True)
        self._query_runner = query_runner
        self._namespace = namespace
        self._server_version = server_version
        self._compute_cluster_web_uri = f"http://{compute_cluster_ip}:5005"
        self._compute_cluster_arrow_uri = f"grpc://{compute_cluster_ip}:8815"
        self._compute_cluster_mlflow_uri = f"http://{compute_cluster_ip}:8080"
        self._encrypted_db_password = encrypted_db_password
        self._arrow_uri = arrow_uri

    @client_only_endpoint("gds.kge")
    def model(self):
        print("!model")
        return self

    # @client_only_endpoint("gds.kge.model") and name is train
    # @compatible_with("stream", min_inclusive=ServerVersion(2, 5, 0))
    @client_only_endpoint("gds.kge.model")
    def train(
        self,
        G: Graph,
        scoring_function,
        num_epochs,
        embedding_dimension,
        mlflow_experiment_name: Optional[str] = None,
    ) -> Series:
        print("!!!train")
        graph_config = {"name": G.name()}

        algo_config = {
            "scoring_function": scoring_function,
            "num_epochs": num_epochs,
            "embedding_dimension": embedding_dimension,
        }

        config = {
            "user_name": "DUMMY_USER",
            "task": "KGE_TRAINING_PYG",
            "task_config": {
                "graph_config": graph_config,
                "task_config": algo_config,
            },
            "encrypted_db_password": self._encrypted_db_password,
            "graph_arrow_uri": self._arrow_uri,
        }

        if mlflow_experiment_name is not None:
            config["task_config"]["mlflow"] = {
                "config": {"tracking_uri": self._compute_cluster_mlflow_uri, "experiment_name": mlflow_experiment_name}
            }

        job_id = self._start_job(config)

        self._wait_for_job(job_id)

        return Series({"status": "finished"})

    def _start_job(self, config: Dict[str, Any]) -> str:
        res = requests.post(f"{self._compute_cluster_web_uri}/api/machine-learning/start", json=config)
        res.raise_for_status()
        job_id = res.json()["job_id"]
        logging.info(f"Job with ID '{job_id}' started")

        return job_id

    def _wait_for_job(self, job_id: str) -> None:
        while True:
            time.sleep(1)

            res = requests.get(f"{self._compute_cluster_web_uri}/api/machine-learning/status/{job_id}")

            res_json = res.json()
            if res_json["job_status"] == "exited":
                logging.info("KGE job completed!")
                return
            elif res_json["job_status"] == "failed":
                error = f"KGE job failed with errors:{os.linesep}{os.linesep.join(res_json['errors'])}"
                if res.status_code == 400:
                    raise ValueError(error)
                else:
                    raise RuntimeError(error)
