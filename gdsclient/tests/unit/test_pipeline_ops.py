from gdsclient.graph_data_science import GraphDataScience

from .conftest import CollectingQueryRunner


def test_create_lp_pipeline(
    runner: CollectingQueryRunner, gds: GraphDataScience
) -> None:
    pipe = gds.alpha.ml.pipeline.linkPrediction.create("pipe")
    assert pipe.name() == "pipe"

    assert (
        runner.last_query() == "CALL gds.alpha.ml.pipeline.linkPrediction.create($name)"
    )
    assert runner.last_params() == {
        "name": "pipe",
    }
