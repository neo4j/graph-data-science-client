from unittest.mock import MagicMock

from graphdatascience.procedure_surface.arrow.node_embedding.graphsage_train_arrow_endpoints import (
    GraphSageTrainArrowEndpoints,
)


def test_train_endpoints_forward_show_progress_to_predict() -> None:
    ep = GraphSageTrainArrowEndpoints(
        arrow_client=MagicMock(),
        write_protocol=None,
        show_progress=False,
    )
    assert ep._show_progress is False
    assert ep._node_property_endpoints._show_progress is False


def test_train_endpoints_default_show_progress() -> None:
    ep = GraphSageTrainArrowEndpoints(
        arrow_client=MagicMock(),
        write_protocol=None,
    )
    assert ep._show_progress is True
