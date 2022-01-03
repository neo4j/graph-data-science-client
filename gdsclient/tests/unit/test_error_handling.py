import pytest

from gdsclient.graph_data_science import GraphDataScience


def test_call_nonexisting_endpoint(gds: GraphDataScience) -> None:
    with pytest.raises(SyntaxError):
        gds.bogus.thing()
