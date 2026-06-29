from typing import Callable, Generator

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.graph_construction.arrow_v2_graph_constructor import ArrowV2GraphConstructor
from graphdatascience.graph_construction.graph_constructor import GraphConstructor
from graphdatascience.procedure_surface.arrow.catalog.catalog_arrow_endpoints import CatalogArrowEndpoints

from .graph_constructor_test_base import GraphConstructorTestBase


@pytest.fixture
def constructor_factory(arrow_client: AuthenticatedArrowClient) -> Callable[..., GraphConstructor]:
    def factory(graph_name: str, **kwargs: object) -> GraphConstructor:
        return ArrowV2GraphConstructor(
            authenticated_arrow_client=arrow_client,
            graph_name=graph_name,
            show_progress=False,
            **kwargs,  # type: ignore[arg-type]
        )

    return factory


@pytest.fixture
def catalog(arrow_client: AuthenticatedArrowClient) -> Generator[CatalogArrowEndpoints, None, None]:
    yield CatalogArrowEndpoints(arrow_client)


class TestArrowV2GraphConstructor(GraphConstructorTestBase):
    graph_name_prefix = "arrow_v2"
