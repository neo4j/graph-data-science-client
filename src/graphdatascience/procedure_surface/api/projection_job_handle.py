from __future__ import annotations

from typing import Any, Tuple

from pyarrow import ArrowKeyError

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.api_types import JobStatus
from graphdatascience.arrow_client.v2.job_client import JobClient
from graphdatascience.graph.v2 import GraphV2
from graphdatascience.procedure_surface.api.job_not_finished_error import JobNotFinishedError
from graphdatascience.procedure_surface.arrow.catalog.graph_backend_arrow import ArrowGraphBackend
from graphdatascience.query_runner.termination_flag import TerminationFlag
from graphdatascience.retry_utils.retry_config import ExponentialWaitConfig, RetryConfigV2, StopConfig


class ProjectionJobHandle:
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        graph_name: str,
        job_id: str,
        termination_flag: TerminationFlag,
    ):
        self._arrow_client = arrow_client
        self._job_client = JobClient()
        self._graph_name = graph_name
        self._job_id = job_id
        self._termination_flag = termination_flag

    def job_id(self) -> str:
        return self._job_id

    def status(self) -> JobStatus:
        # We need to retry this because the job might not yet be registered with the session
        retry_config = RetryConfigV2(
            retryable_exceptions=[ArrowKeyError],
            stop_config=StopConfig(after_attempt=10),
            wait_config=ExponentialWaitConfig(multiplier=1, min=1, max=10),
        )

        @retry_config.decorator()
        def _poll() -> JobStatus:
            self._termination_flag.assert_running()
            return self._job_client.get_job_status(self._arrow_client, self._job_id)

        return _poll()

    def done(self) -> bool:
        return self.status().succeeded() or self.status().aborted()

    def wait(self, log_progress: bool = True) -> None:
        if self.done():
            return

        JobClient().wait_for_job(
            self._arrow_client,
            self._job_id,
            show_progress=log_progress,
            termination_flag=self._termination_flag,
        )

    def result(self, *, wait: bool = True) -> Tuple[GraphV2, dict[str, Any]]:
        if not self.done():
            if not wait:
                raise JobNotFinishedError(f"Projection job '{self._job_id}' is not finished yet.")
            self.wait()

        summary = self._job_client.get_summary(self._arrow_client, self._job_id)

        return GraphV2(self._graph_name, ArrowGraphBackend(self._graph_name, self._arrow_client)), summary
