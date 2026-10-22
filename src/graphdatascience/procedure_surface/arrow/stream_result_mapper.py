from typing import Callable

from pandas import DataFrame, isna


def rename_similarity_stream_result(result: DataFrame) -> DataFrame:
    result = result.rename(columns={"sourceNodeId": "node1", "targetNodeId": "node2"})
    if "relationshipType" in result.columns:
        result = result.drop(columns=["relationshipType"])
    return result


def map_shortest_path_stream_result(result: DataFrame) -> DataFrame:
    result = result.rename(
        columns={
            "sourceNodeId": "sourceNode",
            "targetNodeId": "targetNode",
        }
    )
    result = result.drop(columns=["relationshipType"])

    # add an index column to the result DataFrame with just a range from 0 to len(result)-1
    result["index"] = range(len(result))

    return result


def map_max_flow_stream_result(result: DataFrame) -> DataFrame:
    result = result.rename(
        columns={
            "sourceNodeId": "source",
            "targetNodeId": "target",
        }
    )
    result = result.drop(columns=["relationshipType"])

    return result


def map_all_shortest_path_stream_result(result: DataFrame) -> DataFrame:
    return result.drop(columns=["relationshipType"])


def map_topological_sort_stream_result(result: DataFrame) -> DataFrame:
    # The Arrow result carries an `index` column encoding the topological order; sort by it and drop it.
    return result.sort_values("index").drop(columns=["index"]).reset_index(drop=True)


def aggregate_traversal_rels(result: DataFrame, source_node: int) -> DataFrame:
    result = result.drop(columns=["sourceNodeId", "relationshipType"])

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


def map_steiner_tree_stream_result(result: DataFrame) -> DataFrame:
    result = result.rename(
        columns={
            "sourceNodeId": "nodeId",
            "targetNodeId": "parentId",
        }
    )
    if "relationshipType" in result.columns:
        result = result.drop(columns=["relationshipType"])
    return result


def map_conductance_stream_result(result: DataFrame) -> DataFrame:
    return result.rename(columns={"communityId": "community"})


def map_sllpa_stream_result(result: DataFrame) -> DataFrame:
    return result.rename(columns={"community": "values"})


def map_clique_counting_stream_result(result: DataFrame) -> DataFrame:
    return result.rename(columns={"cliqueCount": "counts"})


def map_node_properties_stream_result(result: DataFrame) -> DataFrame:
    return result.rename(columns={"labels": "nodeLabels"})


def map_scale_properties_stream_result(result: DataFrame) -> DataFrame:
    return result.rename(columns={"scaledProperties": "scaledProperty"})


def map_graphsage_supervised_predict_stream_result(result: DataFrame) -> DataFrame:
    return result.rename(
        columns={
            "predicted_class": "predictedClass",
            "predicted_probabilities": "predictedProbabilities",
        }
    )


def map_articulation_points_stream_result(result: DataFrame) -> DataFrame:
    min_sizes = result["minComponentSize"]
    max_sizes = result["maxComponentSize"]
    counts = result["componentCount"]

    resulting_components: list[dict[str, int] | None] = []
    for min_size, max_size, count in zip(min_sizes, max_sizes, counts):
        if not isna(min_size) and not isna(max_size) and not isna(count):
            resulting_components.append({"min": int(min_size), "max": int(max_size), "count": int(count)})
        else:
            resulting_components.append(None)

    result["resultingComponents"] = resulting_components  # type: ignore[assignment]
    return result.drop(columns=["score", "minComponentSize", "maxComponentSize", "componentCount"])


_STREAM_MAPPERS: dict[str, Callable[[DataFrame], DataFrame]] = {
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
    "v2/centrality.articulationPoints": map_articulation_points_stream_result,
    "v2/embeddings.graphSage.supervised.predict": map_graphsage_supervised_predict_stream_result,
    "v2/pipeline.linkPrediction.predict": rename_similarity_stream_result,
}


def apply_stream_mapper(endpoint: str, result: DataFrame) -> DataFrame:
    """Apply the endpoint-specific stream result mapper, returning the mapped DataFrame."""
    mapper = _STREAM_MAPPERS.get(endpoint)
    if mapper is None:
        return result
    return mapper(result)
