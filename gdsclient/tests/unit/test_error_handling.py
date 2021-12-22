import pytest

from gdsclient import GraphDataScience

from . import CollectingQueryRunner

runner = CollectingQueryRunner()
gds = GraphDataScience(runner)


def test_call_nonexisting_endpoint():
    with pytest.raises(SyntaxError):
        gds.bogus.thing()
