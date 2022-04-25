import pytest

from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.model.graphsage_model import GraphSageModel
from graphdatascience.model.model import Model
from graphdatascience.server_version.server_version import ServerVersion

from .conftest import CollectingQueryRunner

MODEL_NAME = "dummy"


@pytest.fixture
def model(runner: CollectingQueryRunner, server_version: ServerVersion) -> Model:
    return GraphSageModel(MODEL_NAME, runner, server_version)


def test_store_model(runner: CollectingQueryRunner, gds: GraphDataScience, model: Model) -> None:
    gds.alpha.model.store(model, False)

    assert runner.last_query() == "CALL gds.alpha.model.store($model_name, $fail_flag)"
    assert runner.last_params() == {"model_name": MODEL_NAME, "fail_flag": False}


def test_list_models(runner: CollectingQueryRunner, gds: GraphDataScience, model: Model) -> None:
    gds.beta.model.list(model)

    assert runner.last_query() == "CALL gds.beta.model.list($model_name)"
    assert runner.last_params() == {"model_name": MODEL_NAME}

    gds.beta.model.list()

    assert runner.last_query() == "CALL gds.beta.model.list()"
    assert runner.last_params() == {}


def test_exists_model(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    gds.alpha.model.exists("my_model")

    assert runner.last_query() == "CALL gds.alpha.model.exists($model_name)"
    assert runner.last_params() == {"model_name": "my_model"}


def test_drop_model(runner: CollectingQueryRunner, gds: GraphDataScience, model: Model) -> None:
    gds.alpha.model.drop(model)

    assert runner.last_query() == "CALL gds.alpha.model.drop($model_name)"
    assert runner.last_params() == {"model_name": MODEL_NAME}


def test_delete_model(runner: CollectingQueryRunner, gds: GraphDataScience, model: Model) -> None:
    gds.alpha.model.delete(model)

    assert runner.last_query() == "CALL gds.alpha.model.delete($model_name)"
    assert runner.last_params() == {"model_name": MODEL_NAME}
