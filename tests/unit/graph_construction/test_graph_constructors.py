from unittest import mock

from pandas import DataFrame

from graphdatascience.graph_construction.arrow_v1_graph_constructor import ArrowV1GraphConstructor
from graphdatascience.graph_construction.cypher_graph_constructor import CypherGraphConstructor
from graphdatascience.versions import ServerVersion
from tests.unit.conftest import CollectingQueryRunner


def test_arrow_v1_constructor_forwards_inverse_indexed_relationship_types() -> None:
    client = mock.Mock()
    constructor = ArrowV1GraphConstructor(
        database="db",
        graph_name="g",
        flight_client=client,
        inverse_indexed_relationship_types=["KNOWS"],
    )

    constructor.run([DataFrame({"sourceNodeId": [0]})], [DataFrame({"sourceNodeId": [0], "targetNodeId": [1]})])

    client.create_graph.assert_called_once()
    _, kwargs = client.create_graph.call_args
    assert kwargs["inverse_indexed_relationship_types"] == ["KNOWS"]


def test_cypher_constructor_forwards_inverse_indexed_relationship_types() -> None:
    runner = CollectingQueryRunner(
        ServerVersion(3, 0, 0),
        result_mock={"sysInfo": DataFrame({"value": ["Community"]})},
    )

    constructor = CypherGraphConstructor(
        query_runner=runner,
        graph_name="g",
        inverse_indexed_relationship_types=["KNOWS"],
    )

    with mock.patch.object(CypherGraphConstructor.CypherProjectionRunner, "run", autospec=True) as mock_run:
        constructor.run(
            [DataFrame({"sourceNodeId": [0], "Person": [1]})],
            [DataFrame({"sourceNodeId": [0], "targetNodeId": [1], "KNOWS": [1]})],
        )

    mock_run.assert_called_once()
    runner_instance = mock_run.call_args[0][0]
    assert runner_instance._inverse_indexed_relationship_types == ["KNOWS"]
