from __future__ import annotations

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.data_mapper_utils import deserialize
from graphdatascience.procedure_surface.api.system_endpoints import ProgressResult, SystemEndpoints
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter


class SystemArrowEndpoints(SystemEndpoints):
    def __init__(self, arrow_client: AuthenticatedArrowClient):
        self._arrow_client = arrow_client

    def list_progress(
        self,
        job_id: str | None = None,
        show_completed: bool = False,
    ) -> list[ProgressResult]:
        config = ConfigConverter.convert_to_gds_config(
            job_id=job_id,
            show_completed=show_completed,
        )

        rows = deserialize(self._arrow_client.do_action_with_retry("v2/listProgress", config))
        return [ProgressResult(**row) for row in rows]
