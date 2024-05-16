import logging
import os
import time
from typing import Any, Dict, Optional

import pyarrow as pa
import pyarrow.flight
import requests
from pandas import DataFrame

from ..error.client_only_endpoint import client_only_endpoint
from ..error.illegal_attr_checker import IllegalAttrChecker
from ..error.uncallable_namespace import UncallableNamespace
from ..graph.graph_object import Graph
from ..query_runner.query_runner import QueryRunner
from ..server_version.compatible_with import compatible_with
from ..server_version.server_version import ServerVersion

logging.basicConfig(level=logging.INFO)


class FastPathRunner(UncallableNamespace, IllegalAttrChecker):
    def __init__(
        self,
        query_runner: QueryRunner,
        namespace: str,
        server_version: ServerVersion,
        compute_cluster_ip: str,
        encrypted_db_password: str,
        arrow_uri: str,
    ):
        self._query_runner = query_runner
        self._namespace = namespace
        self._server_version = server_version
        self._compute_cluster_web_uri = f"http://{compute_cluster_ip}:5000"
        self._compute_cluster_arrow_uri = f"grpc://{compute_cluster_ip}:8815"
        self._compute_cluster_mlflow_uri = f"http://{compute_cluster_ip}:8080"
        self._encrypted_db_password = encrypted_db_password
        self._arrow_uri = arrow_uri

    @compatible_with("stream", min_inclusive=ServerVersion(2, 5, 0))
    @client_only_endpoint("gds.fastpath")
    def stream(
        self,
        G: Graph,
        graph_filter: Optional[Dict[str, Any]] = None,
        mlflow_experiment_name: Optional[str] = None,
        **algo_config: Any,
    ) -> DataFrame:
        if graph_filter is None:
            # Take full graph if no filter provided
            node_filter = G.node_properties().to_dict()
            rel_filter = G.relationship_properties().to_dict()
            graph_filter = {"node_labels": node_filter, "rel_types": rel_filter}

        graph_config = {"name": G.name()}
        graph_config.update(graph_filter)

        config = {
            "user_name": "DUMMY_USER",
            "task": "FASTPATH",
            "task_config": {
                "graph_config": graph_config,
                "task_config": algo_config,
                "stream_node_results": True,
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

        return self._stream_results(job_id)

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
                logging.info("FastPath job completed!")
                return
            elif res_json["job_status"] == "failed":
                error = f"FastPath job failed with errors:{os.linesep}{os.linesep.join(res_json['errors'])}"
                if res.status_code == 400:
                    raise ValueError(error)
                else:
                    raise RuntimeError(error)

    def _stream_results(self, job_id: str) -> DataFrame:
        client = pa.flight.connect(self._compute_cluster_arrow_uri)

        upload_descriptor = pa.flight.FlightDescriptor.for_path(f"{job_id}.nodes")
        flight = client.get_flight_info(upload_descriptor)
        reader = client.do_get(flight.endpoints[0].ticket)
        read_table = reader.read_all()

        return read_table.to_pandas()
