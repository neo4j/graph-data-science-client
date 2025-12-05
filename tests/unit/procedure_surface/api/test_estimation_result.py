from graphdatascience.procedure_surface.api.estimation_result import EstimationResult


def test_estimation_result_initialization() -> None:
    estimation_result = EstimationResult(
        nodeCount=5,
        relationshipCount=10,
        requiredMemory="512MB",
        treeView="TreeData",
        mapView={"key": "value"},
        bytesMin=500,
        bytesMax=1000,
        heapPercentageMin=0.1,
        heapPercentageMax=0.5,
    )  # type: ignore

    assert estimation_result.node_count == 5
    assert estimation_result.relationship_count == 10
    assert estimation_result.required_memory == "512MB"
    assert estimation_result.tree_view == "TreeData"
    assert estimation_result.map_view == {"key": "value"}
    assert estimation_result.bytes_min == 500
    assert estimation_result.bytes_max == 1000
    assert estimation_result.heap_percentage_min == 0.1
    assert estimation_result.heap_percentage_max == 0.5


def test_estimation_result_getitem() -> None:
    estimation_result = EstimationResult(
        nodeCount=5,
        relationshipCount=10,
        requiredMemory="512MB",
        treeView="TreeData",
        mapView={"key": "value"},
        bytesMin=500,
        bytesMax=1000,
        heapPercentageMin=0.1,
        heapPercentageMax=0.5,
    )  # type: ignore

    assert estimation_result["node_count"] == 5
    assert estimation_result["relationship_count"] == 10
    assert estimation_result["required_memory"] == "512MB"
    assert estimation_result["tree_view"] == "TreeData"
    assert estimation_result["map_view"] == {"key": "value"}
    assert estimation_result["bytes_min"] == 500
    assert estimation_result["bytes_max"] == 1000
    assert estimation_result["heap_percentage_min"] == 0.1
    assert estimation_result["heap_percentage_max"] == 0.5


def test_estimation_result_from_cypher() -> None:
    cypher_result = {
        "nodeCount": 5,
        "relationshipCount": 10,
        "requiredMemory": "512MB",
        "treeView": "TreeData",
        "mapView": {"key": "value"},
        "bytesMin": 500,
        "bytesMax": 1000,
        "heapPercentageMin": 0.1,
        "heapPercentageMax": 0.5,
    }
    estimation_result = EstimationResult.from_cypher(cypher_result)
    assert isinstance(estimation_result, EstimationResult)
    assert estimation_result.node_count == 5
    assert estimation_result.relationship_count == 10
    assert estimation_result.required_memory == "512MB"
    assert estimation_result.tree_view == "TreeData"
    assert estimation_result.map_view == {"key": "value"}
    assert estimation_result.bytes_min == 500
    assert estimation_result.bytes_max == 1000
    assert estimation_result.heap_percentage_min == 0.1
    assert estimation_result.heap_percentage_max == 0.5
