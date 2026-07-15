from unittest.mock import Mock

import pytest

from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.arrow.catalog.graph_export_arrow_endpoints import (
    GraphExportArrowEndpoints,
)


def test_all_endpoints_raise_not_implemented() -> None:
    endpoints = GraphExportArrowEndpoints()
    G = Mock(spec=Graph)

    with pytest.raises(NotImplementedError, match="not available in AGA sessions"):
        endpoints(G, "targetdb")
    with pytest.raises(NotImplementedError, match="not available in AGA sessions"):
        endpoints.csv(G, "myexport")
