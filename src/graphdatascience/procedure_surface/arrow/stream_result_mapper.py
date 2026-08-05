from typing import Callable

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


def map_max_flow_stream_result(result: DataFrame) -> None:
    result.rename(
        columns={
            "sourceNodeId": "source",
            "targetNodeId": "target",
        },
        inplace=True,
    )
    result.drop(columns=["relationshipType"], inplace=True)


def map_all_shortest_path_stream_result(result: DataFrame) -> None:
    result.drop(columns=["relationshipType"], inplace=True)


def map_topological_sort_stream_result(result: DataFrame) -> None:
    # The Arrow result carries an `index` column encoding the topological order; sort by it and drop it.
    result.sort_values("index", inplace=True)
    result.drop(columns=["index"], inplace=True)
    result.reset_index(drop=True, inplace=True)


def aggregate_traversal_rels(result: DataFrame, source_node: int) -> DataFrame:
    result.drop(columns=["sourceNodeId", "relationshipType"], inplace=True)

    # Aggregate targetNodes + index column into a list
    node_ids = result.sort_values("index")["targetNodeId"].values

    return DataFrame(
        data={
            "sourceNode": [source_node],
            "nodeIds": [node_ids],  # Wrap the list in a list to keep it as a single value
        }
    )


def aggregate_traversal_rels_from_result(result: DataFrame) -> DataFrame:
    """Aggregate traversal relationships, reading the source node from the result rows."""
    if len(result) == 0:
        return DataFrame(data={"sourceNode": [], "nodeIds": []})
    source_node = int(result["sourceNodeId"].iloc[0])
    return aggregate_traversal_rels(result, source_node)


def map_steiner_tree_stream_result(result: DataFrame) -> None:
    result.rename(
        columns={
            "sourceNodeId": "nodeId",
            "targetNodeId": "parentId",
        },
        inplace=True,
    )
    if "relationshipType" in result.columns:
        result.drop(columns=["relationshipType"], inplace=True)


def map_conductance_stream_result(result: DataFrame) -> None:
    result.rename(
        columns={"communityId": "community"},
        inplace=True,
    )


def map_sllpa_stream_result(result: DataFrame) -> None:
    result.rename(columns={"community": "values"}, inplace=True)


def map_clique_counting_stream_result(result: DataFrame) -> None:
    result.rename(columns={"cliqueCount": "counts"}, inplace=True)


def map_node_properties_stream_result(result: DataFrame) -> None:
    result.rename(columns={"labels": "nodeLabels"}, inplace=True)


def map_scale_properties_stream_result(result: DataFrame) -> None:
    result.rename(columns={"scaledProperties": "scaledProperty"}, inplace=True)


_STREAM_MAPPERS: dict[str, Callable[[DataFrame], DataFrame | None]] = {
    "v2/similarity.knn": rename_similarity_stream_result,
    "v2/similarity.knn.filtered": rename_similarity_stream_result,
    "v2/similarity.nodeSimilarity": rename_similarity_stream_result,
    "v2/similarity.nodeSimilarity.filtered": rename_similarity_stream_result,
    "v2/pathfinding.sourceTarget.dijkstra": map_shortest_path_stream_result,
    "v2/pathfinding.sourceTarget.aStar": map_shortest_path_stream_result,
    "v2/pathfinding.sourceTarget.yens": map_shortest_path_stream_result,
    "v2/pathfinding.singleSource.dijkstra": map_shortest_path_stream_result,
    "v2/pathfinding.singleSource.deltaStepping": map_shortest_path_stream_result,
    "v2/pathfinding.singleSource.bellmanFord": map_shortest_path_stream_result,
    "v2/pathfinding.longestPath": map_shortest_path_stream_result,
    "v2/pathfinding.topologicalSort": map_topological_sort_stream_result,
    "v2/pathfinding.maxFlow": map_max_flow_stream_result,
    "v2/pathfinding.maxFlow.minCost": map_max_flow_stream_result,
    "v2/pathfinding.allShortestPaths": map_all_shortest_path_stream_result,
    "v2/pathfinding.spanningTree": map_steiner_tree_stream_result,
    "v2/pathfinding.steinerTree": map_steiner_tree_stream_result,
    "v2/pathfinding.prizeSteinerTree": map_steiner_tree_stream_result,
    "v2/pathfinding.bfs": aggregate_traversal_rels_from_result,
    "v2/pathfinding.dfs": aggregate_traversal_rels_from_result,
    "v2/community.conductance": map_conductance_stream_result,
    "v2/community.sllpa": map_sllpa_stream_result,
    "v2/community.cliquecounting": map_clique_counting_stream_result,
    "v2/graph.nodeProperties.stream": map_node_properties_stream_result,
    "v2/graph.nodeProperties.scale": map_scale_properties_stream_result,
    "v2/pipeline.linkPrediction.predict": rename_similarity_stream_result,
}


def apply_stream_mapper(endpoint: str, result: DataFrame) -> DataFrame:
    """Apply the endpoint-specific stream result mapper.

    Mappers may mutate the DataFrame in place (returning ``None``) or return a new one.
    """
    mapper = _STREAM_MAPPERS.get(endpoint)
    if mapper is None:
        return result
    mapped = mapper(result)
    return mapped if mapped is not None else result
