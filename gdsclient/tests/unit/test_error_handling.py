import pytest

from gdsclient import GraphDataScience

from . import CollectingQueryRunner


def setup_module():
    global runner
    global gds

    runner = CollectingQueryRunner()
    gds = GraphDataScience(runner)


def test_call_nonexisting_endpoint():
    with pytest.raises(SyntaxError):
        gds.bogus.thing()
