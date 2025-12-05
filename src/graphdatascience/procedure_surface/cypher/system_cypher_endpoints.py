from __future__ import annotations

from graphdatascience.call_parameters import CallParameters
from graphdatascience.procedure_surface.api.system_endpoints import ProgressResult, SystemEndpoints
from graphdatascience.query_runner.query_runner import QueryRunner


class SystemCypherEndpoints(SystemEndpoints):
    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def list_progress(
        self,
        job_id: str | None = None,
        show_completed: bool = False,
    ) -> list[ProgressResult]:
        params = CallParameters(
            job_id=job_id if job_id else "",
            show_completed=True,
        )

        result = self._query_runner.call_procedure(endpoint="gds.listProgress", params=params)
        return [ProgressResult(**row.to_dict()) for _, row in result.iterrows()]
