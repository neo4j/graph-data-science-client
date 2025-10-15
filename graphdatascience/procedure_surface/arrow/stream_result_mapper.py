from pandas import DataFrame


def rename_similarity_stream_result(result: DataFrame) -> None:
    result.rename(columns={"sourceNodeId": "node1", "targetNodeId": "node2"}, inplace=True)
    if "relationshipType" in result.columns:
        result.drop(columns=["relationshipType"], inplace=True)
