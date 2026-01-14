from pandas import DataFrame

from graphdatascience.procedure_surface.arrow.stream_result_mapper import aggregate_traversal_rels


def test_aggregate_traversal_rels() -> None:
    input = DataFrame(
        data={"sourceNodeId": [0, 2, 1], "relationshipType": "TYPE", "targetNodeId": [1, 2, 3], "index": [1, 0, 2]}
    )
    actual = aggregate_traversal_rels(input, 0)

    assert actual.shape[0] == 1
    assert actual["sourceNode"].iat[0] == 0
    assert actual["nodeIds"].iat[0].tolist(), [2, 1, 3]
