import pytest

from graphdatascience.procedure_surface.api.topological_link_prediction_endpoints import Direction
from graphdatascience.procedure_surface.arrow.topological_link_prediction_arrow_endpoints import (
    TopologicalLinkPredictionArrowEndpoints,
)


def test_all_functions_raise_not_implemented() -> None:
    endpoints = TopologicalLinkPredictionArrowEndpoints()

    with pytest.raises(NotImplementedError, match="not available in AGA sessions"):
        endpoints.adamic_adar(1, 2)
    with pytest.raises(NotImplementedError, match="not available in AGA sessions"):
        endpoints.common_neighbors(1, 2)
    with pytest.raises(NotImplementedError, match="not available in AGA sessions"):
        endpoints.preferential_attachment(1, 2, relationship_query="REL", direction=Direction.INCOMING)
    with pytest.raises(NotImplementedError, match="not available in AGA sessions"):
        endpoints.resource_allocation(1, 2)
    with pytest.raises(NotImplementedError, match="not available in AGA sessions"):
        endpoints.same_community(1, 2)
    with pytest.raises(NotImplementedError, match="not available in AGA sessions"):
        endpoints.total_neighbors(1, 2)
