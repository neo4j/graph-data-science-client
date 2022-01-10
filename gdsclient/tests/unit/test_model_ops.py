from gdsclient.graph_data_science import GraphDataScience

from .conftest import CollectingQueryRunner


def test_store_model(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    gds.alpha.model.store("my_model", False)

    assert runner.last_query() == "CALL gds.alpha.model.store($model_name, $fail_flag)"
    assert runner.last_params() == {"model_name": "my_model", "fail_flag": False}
