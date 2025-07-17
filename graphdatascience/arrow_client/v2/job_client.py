import json
from typing import Any

from pandas import ArrowDtype, DataFrame
from pyarrow._flight import Ticket

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.api_types import JobIdConfig, JobStatus
from graphdatascience.arrow_client.v2.data_mapper_utils import deserialize_single

JOB_STATUS_ENDPOINT = "v2/jobs.status"
RESULTS_SUMMARY_ENDPOINT = "v2/results.summary"


class JobClient:
    @staticmethod
    def run_job_and_wait(client: AuthenticatedArrowClient, endpoint: str, config: dict[str, Any]) -> str:
        job_id = JobClient.run_job(client, endpoint, config)
        JobClient.wait_for_job(client, job_id)
        return job_id

    @staticmethod
    def run_job(client: AuthenticatedArrowClient, endpoint: str, config: dict[str, Any]) -> str:
        encoded_config = json.dumps(config).encode("utf-8")
        res = client.do_action_with_retry(endpoint, encoded_config)

        single = deserialize_single(res)
        return JobIdConfig(**single).job_id

    @staticmethod
    def wait_for_job(client: AuthenticatedArrowClient, job_id: str) -> None:
        while True:
            encoded_config = JobIdConfig(jobId=job_id).dump_json().encode("utf-8")

            arrow_res = client.do_action_with_retry(JOB_STATUS_ENDPOINT, encoded_config)
            job_status = JobStatus(**deserialize_single(arrow_res))
            if job_status.status == "Done":
                break

    @staticmethod
    def get_summary(client: AuthenticatedArrowClient, job_id: str) -> dict[str, Any]:
        encoded_config = JobIdConfig(jobId=job_id).dump_json().encode("utf-8")

        res = client.do_action_with_retry(RESULTS_SUMMARY_ENDPOINT, encoded_config)
        return deserialize_single(res)

    @staticmethod
    def stream_results(client: AuthenticatedArrowClient, job_id: str) -> DataFrame:
        encoded_config = JobIdConfig(jobId=job_id).dump_json().encode("utf-8")

        res = client.do_action_with_retry("v2/results.stream", encoded_config)
        export_job_id = JobIdConfig(**deserialize_single(res)).job_id

        payload = {
            "name": export_job_id,
            "version": 1,
        }

        ticket = Ticket(json.dumps(payload).encode("utf-8"))
        with client.get_stream(ticket) as get:
            arrow_table = get.read_all()

        return arrow_table.to_pandas(types_mapper=ArrowDtype)  # type: ignore
