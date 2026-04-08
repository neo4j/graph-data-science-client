from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar

from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.api_types import MutateResult, JobStatus
from graphdatascience.arrow_client.v2.job_client import JobClient
from graphdatascience.arrow_client.v2.mutation_client import MutationClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient, WriteBackResult
from graphdatascience.procedure_surface.api.base_result import BaseResult
from graphdatascience.procedure_surface.api.catalog import GraphV2
from graphdatascience.procedure_surface.arrow.endpoints_helper_base import EndpointsHelperBase

T = TypeVar("T", bound=BaseResult)


class JobHandle(ABC, Generic[T]):
    def __init__(self, client: AuthenticatedArrowClient, job_id: str, **kwargs: Any):
        self._client = client
        self._job_id = job_id

    def progress(self) -> float:
        return JobClient.get_job_status(self._client, self._job_id).progress

    def status(self) -> JobStatus:
        return JobClient.get_job_status(self._client, self._job_id)

    def result(self) -> T:
        summary = JobClient.get_summary(self._client, self._job_id)
        EndpointsHelperBase.drop_write_internals(summary)
        return self._parse_result(summary)

    def wait(self, show_progress: bool = False) -> None:
        JobClient().wait_for_job(self._client, self._job_id, show_progress=show_progress)

    @abstractmethod
    def _parse_result(self, summary: dict[str, Any]) -> T:
        """Parse the raw summary dictionary into the specific result type."""
        pass


class NodePropertyMutateHandle(ABC):
    def __init__(self, client: AuthenticatedArrowClient, job_id: str, **kwargs: Any):
        self._job_id = job_id
        self._client = client

    def mutate(self, mutate_property: str) -> MutateResult:
        return MutationClient().mutate_node_property(self._client, self._job_id, mutate_property)


class NodePropertyWriteHandle(ABC):
    def __init__(
        self,
        client: AuthenticatedArrowClient,
        job_id: str,
        remote_write_back_client: RemoteWriteBackClient | None = None,
        **kwargs: Any,
    ):
        self._job_id = job_id
        self._client = client
        self._remote_write_back_client = remote_write_back_client

    def write(
        self, G: GraphV2, write_property: str, write_concurrency: int | None = None, log_progress: bool = True
    ) -> WriteBackResult:

        if self._remote_write_back_client is None:
            raise Exception("Write back client is not initialized")

        return self._remote_write_back_client.write(
            G.name(), self._job_id, write_concurrency, write_property, None, log_progress
        )


class NodePropertyStreamHandle(ABC):
    def __init__(self, client: AuthenticatedArrowClient, job_id: str, **kwargs: Any):
        self._job_id = job_id
        self._client = client

    def stream(self, G: GraphV2) -> DataFrame:
        return JobClient().stream_results(self._client, G.name(), self._job_id)
