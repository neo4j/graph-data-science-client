from pandas import DataFrame


def rename_similarity_stream_result(result: DataFrame) -> None:
    result.rename(columns={"sourceNodeId": "node1", "targetNodeId": "node2"}, inplace=True)
    if "relationshipType" in result.columns:
        result.drop(columns=["relationshipType"], inplace=True)


def map_shortest_path_stream_result(result: DataFrame) -> None:
    result.rename(
        columns={
            "sourceNodeId": "sourceNode",
            "targetNodeId": "targetNode",
        },
        inplace=True,
    )
    result.drop(columns=["relationshipType"], inplace=True)

    # add an index column to the result DataFrame with just a range from 0 to len(result)-1
    result["index"] = range(len(result))
