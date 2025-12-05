from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.arrow.endpoints_helper_base import EndpointsHelperBase


class TableEndpointsHelper(EndpointsHelperBase):
    """
    Helper class for Arrow algorithm endpoints that work with table results
    Provides common functionality for job execution, and streaming.
    Notably lacks mutation and write functionality.
    """

    def __init__(self, arrow_client: AuthenticatedArrowClient, show_progress: bool = True) -> None:
        super().__init__(arrow_client, None, show_progress)
