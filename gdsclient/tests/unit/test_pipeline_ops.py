from typing import Generator

import pytest

from gdsclient.graph_data_science import GraphDataScience
from gdsclient.pipeline.lp_pipeline import LPPipeline

from .conftest import CollectingQueryRunner

PIPE_NAME = "pipe"


@pytest.fixture
def pipe(gds: GraphDataScience) -> LPPipeline:
    pipe = gds.alpha.ml.pipeline.linkPrediction.create(PIPE_NAME)
    return pipe


def test_create_lp_pipeline(
    runner: CollectingQueryRunner, gds: GraphDataScience
) -> None:
    pipe = gds.alpha.ml.pipeline.linkPrediction.create("hello")
    assert pipe.name() == "hello"

    assert (
        runner.last_query() == "CALL gds.alpha.ml.pipeline.linkPrediction.create($name)"
    )
    assert runner.last_params() == {
        "name": "hello",
    }


def test_add_node_property_lp_pipeline(
    runner: CollectingQueryRunner, pipe: LPPipeline
) -> None:
    pipe.addNodeProperty(
        "pageRank", mutateProperty="rank", dampingFactor=0.2, tolerance=0.3
    )

    assert (
        runner.last_query()
        == "CALL gds.alpha.ml.pipeline.linkPrediction.addNodeProperty($pipeline_name, $procedure_name, $config)"
    )
    assert runner.last_params() == {
        "pipeline_name": pipe.name(),
        "procedure_name": "pageRank",
        "config": {"mutateProperty": "rank", "dampingFactor": 0.2, "tolerance": 0.3},
    }
