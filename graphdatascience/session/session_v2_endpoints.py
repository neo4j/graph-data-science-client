from typing import Optional

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.wcc_endpoints import WccEndpoints
from graphdatascience.procedure_surface.arrow.catalog_arrow_endpoints import CatalogArrowEndpoints
from graphdatascience.procedure_surface.arrow.wcc_arrow_endpoints import WccArrowEndpoints
from graphdatascience.query_runner.query_runner import QueryRunner


class SessionV2Endpoints:
    def __init__(self, arrow_client: AuthenticatedArrowClient, db_client: Optional[QueryRunner] = None):
        self._arrow_client = arrow_client
        self._db_client = db_client

        self._write_back_client = RemoteWriteBackClient(arrow_client, db_client) if db_client is not None else None

    @property
    def wcc(self) -> WccEndpoints:
        return WccArrowEndpoints(self._arrow_client, self._write_back_client)

    @property
    def graph(self) -> CatalogArrowEndpoints:
        return CatalogArrowEndpoints(self._arrow_client, self._db_client)
