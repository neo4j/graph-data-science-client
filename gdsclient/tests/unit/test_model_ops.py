from gdsclient.graph_data_science import GraphDataScience

from .conftest import CollectingQueryRunner


def test_store_model(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    gds.alpha.model.store("my_model", False)

    assert runner.last_query() == "CALL gds.alpha.model.store($model_name, $fail_flag)"
    assert runner.last_params() == {"model_name": "my_model", "fail_flag": False}


def test_list_models(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    gds.beta.model.list("my_model")

    assert runner.last_query() == "CALL gds.beta.model.list($model_name)"
    assert runner.last_params() == {"model_name": "my_model"}

    gds.beta.model.list()

    assert runner.last_query() == "CALL gds.beta.model.list()"
    assert runner.last_params() == {}


def test_exists_model(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    gds.alpha.model.exists("my_model")

    assert runner.last_query() == "CALL gds.alpha.model.exists($model_name)"
    assert runner.last_params() == {"model_name": "my_model"}


def test_drop_model(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    gds.alpha.model.drop("my_model")

    assert runner.last_query() == "CALL gds.alpha.model.drop($model_name)"
    assert runner.last_params() == {"model_name": "my_model"}


def test_delete_model(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    gds.alpha.model.delete("my_model")

    assert runner.last_query() == "CALL gds.alpha.model.delete($model_name)"
    assert runner.last_params() == {"model_name": "my_model"}
