from typing import Generator

import pytest

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.node_embedding.config import FastRPConfig, GBClassifierConfig
from graphdatascience.procedure_surface.arrow.model.model_catalog_arrow_endpoints import ModelCatalogArrowEndpoints
from graphdatascience.procedure_surface.arrow.node_embedding.train_arrow_endpoints import TrainArrowEndpoints
from tests.integration.procedure_surface.arrow.graph_creation_helper import (
    create_graph,
)
from tests.integration.procedure_surface.arrow.node_embedding.conftest import ignore_preview_warning

graph = """
        CREATE
            (a1: A {x: 3.0, y: 0}),
            (a2: A {x: 3.0, y: 0}),
            (b1: B {x: 3.0}),
            (b2: B {x: 3.0}),
            (b3: B {x: 3.0}),
            (a1)-[:R]->(b1),
            (a1)-[:R]->(b2),
            (b2)-[:R]->(b3),
            (b3)-[:R]->(a1),
            (b3)-[:R]->(a2)
        """


@pytest.fixture
def sample_graph(arrow_client_runtime: AuthenticatedArrowClient) -> Generator[Graph, None, None]:
    with create_graph(arrow_client_runtime, "g", graph) as G:
        yield G


@ignore_preview_warning
def test_embedding_train(arrow_client_runtime: AuthenticatedArrowClient, sample_graph: Graph) -> None:
    model_name = "my_model456"
    try:
        result = TrainArrowEndpoints(arrow_client_runtime)(
            G=sample_graph,
            feature_properties=["x"],
            graph_encoder=FastRPConfig(),
            decoder=GBClassifierConfig(),
            target_label="A",
            target_property="y",
            model_save_name=model_name,
        )
        assert result.compute_millis > 0
    finally:
        ModelCatalogArrowEndpoints(arrow_client_runtime).delete(model_name=model_name, fail_if_missing=True)
