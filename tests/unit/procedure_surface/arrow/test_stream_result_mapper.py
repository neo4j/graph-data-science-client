from typing import Any, cast

import pytest
from pandas import DataFrame

from graphdatascience.procedure_surface.arrow.stream_result_mapper import aggregate_traversal_rels, apply_stream_mapper


@pytest.mark.parametrize(
    ("endpoint", "input", "expected_columns"),
    [
        (
            "v2/community.sllpa",
            DataFrame(data={"nodeId": [0], "community": [{"1": 0.5}]}),
            ["nodeId", "values"],
        ),
        (
            "v2/community.cliquecounting",
            DataFrame(data={"nodeId": [0], "cliqueCount": [[1, 2]]}),
            ["nodeId", "counts"],
        ),
        (
            "v2/graph.nodeProperties.stream",
            DataFrame(data={"nodeId": [0], "labels": [["A"]], "prop": [42]}),
            ["nodeId", "nodeLabels", "prop"],
        ),
        (
            "v2/graph.nodeProperties.scale",
            DataFrame(data={"nodeId": [0], "scaledProperties": [[0.5]]}),
            ["nodeId", "scaledProperty"],
        ),
        (
            "v2/pipeline.linkPrediction.predict",
            DataFrame(
                data={"sourceNodeId": [0], "targetNodeId": [1], "relationshipType": ["REL"], "probability": [0.9]}
            ),
            ["node1", "node2", "probability"],
        ),
    ],
)
def test_apply_stream_mapper_aligns_columns_with_cypher(
    endpoint: str, input: DataFrame, expected_columns: list[str]
) -> None:
    assert list(apply_stream_mapper(endpoint, input).columns) == expected_columns


def test_aggregate_traversal_rels() -> None:
    input = DataFrame(
        data={"sourceNodeId": [0, 2, 1], "relationshipType": "TYPE", "targetNodeId": [1, 2, 3], "index": [1, 0, 2]}
    )
    actual = aggregate_traversal_rels(input, 0)

    assert actual.shape[0] == 1
    assert actual["sourceNode"].iat[0] == 0
    assert cast("Any", actual["nodeIds"].iat[0]).tolist() == [2, 1, 3]
