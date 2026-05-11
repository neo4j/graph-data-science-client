from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.data_mapper_utils import deserialize
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.pipeline.pipeline_endpoints import (
    PipelineCatalogEntry,
    PipelineExistsResult,
)


class PipelineCatalogArrowEndpoints:
    def __init__(
        self,
        arrow_client: AuthenticatedArrowClient,
        write_back_client: RemoteWriteBackClient | None,
        show_progress: bool = True,
    ) -> None:
        self._arrow_client = arrow_client
        self._write_back_client = write_back_client
        self._show_progress = show_progress

    def list(self, pipeline_name: str | None = None) -> list[PipelineCatalogEntry]:
        config = {} if pipeline_name is None else {"pipelineName": pipeline_name}
        result = deserialize(self._arrow_client.do_action_with_retry("v2/pipeline.list", config))
        return [PipelineCatalogEntry(**item) for item in result]

    def exists(self, pipeline_name: str) -> PipelineExistsResult | None:
        result = deserialize(
            self._arrow_client.do_action_with_retry("v2/pipeline.list", {"pipelineName": pipeline_name})
        )
        if not result:
            return None

        item = result[0]
        return PipelineExistsResult(
            pipelineName=str(item["pipelineName"]),
            pipelineType=str(item["pipelineType"]),
            exists=True,
        )

    def drop(self, pipeline_name: str, *, fail_if_missing: bool = False) -> PipelineCatalogEntry | None:
        result = deserialize(
            self._arrow_client.do_action_with_retry(
                "v2/pipeline.drop",
                {"pipelineName": pipeline_name, "failIfMissing": fail_if_missing},
            )
        )
        if not result and fail_if_missing:
            raise ValueError(f"Pipeline with name `{pipeline_name}` does not exist")
        if not result:
            return None

        return PipelineCatalogEntry(**result[0])
