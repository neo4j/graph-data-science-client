import json
from typing import Any

from pandas import ArrowDtype, DataFrame
from pyarrow._flight import Ticket
from tenacity import Retrying, retry_if_result, wait_exponential

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.api_types import JobIdConfig, JobStatus
from graphdatascience.arrow_client.v2.data_mapper_utils import deserialize_single
from graphdatascience.query_runner.progress.progress_bar import TqdmProgressBar
from graphdatascience.query_runner.termination_flag import TerminationFlag

JOB_STATUS_ENDPOINT = "v2/jobs.status"
RESULTS_SUMMARY_ENDPOINT = "v2/results.summary"


class JobClient:
    def __init__(self, progress_bar_options: dict[str, Any] | None = None):
        self._progress_bar_options = progress_bar_options or {}

    @staticmethod
    def run_job_and_wait(
        client: AuthenticatedArrowClient, endpoint: str, config: dict[str, Any], show_progress: bool
    ) -> str:
        job_id = JobClient.run_job(client, endpoint, config)
        JobClient().wait_for_job(client, job_id, show_progress=show_progress)
        return job_id

    @staticmethod
    def run_job(client: AuthenticatedArrowClient, endpoint: str, config: dict[str, Any]) -> str:
        res = client.do_action_with_retry(endpoint, config)

        single = deserialize_single(res)
        return JobIdConfig(**single).job_id

    def wait_for_job(
        self,
        client: AuthenticatedArrowClient,
        job_id: str,
        show_progress: bool,
        termination_flag: TerminationFlag | None = None,
    ) -> None:
        progress_bar: TqdmProgressBar | None = None

        if termination_flag is None:
            termination_flag = TerminationFlag.create()

        for attempt in Retrying(retry=retry_if_result(lambda _: True), wait=wait_exponential(min=0.1, max=5)):
            with attempt:
                termination_flag.assert_running()

                arrow_res = client.do_action_with_retry(JOB_STATUS_ENDPOINT, JobIdConfig(jobId=job_id).dump_camel())
                job_status = JobStatus(**deserialize_single(arrow_res))

                if job_status.succeeded() or job_status.aborted():
                    if progress_bar:
                        progress_bar.finish(success=job_status.succeeded())
                    return

                if show_progress:
                    if progress_bar is None:
                        base_task = job_status.base_task()
                        if base_task:
                            progress_bar = TqdmProgressBar(
                                task_name=base_task,
                                relative_progress=job_status.progress_percent(),
                                bar_options=self._progress_bar_options,
                            )
                    if progress_bar:
                        progress_bar.update(job_status.status, job_status.progress_percent(), job_status.sub_tasks())

    @staticmethod
    def get_summary(client: AuthenticatedArrowClient, job_id: str) -> dict[str, Any]:
        res = client.do_action_with_retry(RESULTS_SUMMARY_ENDPOINT, JobIdConfig(jobId=job_id).dump_camel())
        return deserialize_single(res)

    @staticmethod
    def stream_results(client: AuthenticatedArrowClient, graph_name: str, job_id: str) -> DataFrame:
        payload = {
            "graphName": graph_name,
            "jobId": job_id,
        }

        res = client.do_action_with_retry("v2/results.stream", payload)
        export_job_id = JobIdConfig(**deserialize_single(res)).job_id

        stream_payload = {"version": "v2", "name": export_job_id, "body": {}}

        ticket = Ticket(json.dumps(stream_payload).encode("utf-8"))

        get = client.get_stream(ticket)
        arrow_table = get.read_all()
        return arrow_table.to_pandas(types_mapper=ArrowDtype)  # type: ignore
