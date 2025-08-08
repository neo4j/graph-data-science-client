from typing import Any


def estimate_mock_result() -> dict[str, Any]:
    return {
        "nodeCount": 100,
        "relationshipCount": 200,
        "requiredMemory": "1024 Bytes",
        "bytesMin": 1024,
        "bytesMax": 2048,
        "heapPercentageMin": 1.0,
        "heapPercentageMax": 2.0,
        "treeView": "1024 KiB",
        "mapView": {},
    }
