from typing import OrderedDict

PROCEDURE_NAME_TO_PYTHON_ENDPOINT_MAPPINGS = OrderedDict(
    [
        ("linkprediction", "topological_link_prediction"),
        ("influenceMaximization.celf", "influence_maximization_celf"),
        ("k1coloring", "k1_coloring"),
        ("maxkcut", "max_k_cut"),
        ("beta.graphSage", "graph_sage"),
        ("ml.kge.predict", "kge.predict"),
        ("hashgnn", "hash_gnn"),
        ("astar", "a_star"),
        ("version", "server_version"),
        ("beta.pipeline.nodeClassification", "pipeline.node_classification"),
        ("alpha.pipeline.nodeClassification", "pipeline.node_classification"),
        ("beta.pipeline.linkPrediction", "pipeline.link_prediction"),
        ("alpha.pipeline.linkPrediction", "pipeline.link_prediction"),
        ("alpha.pipeline.nodeRegression", "pipeline.node_regression"),
        ("nodeLabel", "node_labels"),
    ]
)


def procedure_name_from_python_endpoint(requested_endpoint: str) -> str:
    """Given a python client endpoint name returns the corresponding lowercase procedure name."""
    for procedure, endpoint in PROCEDURE_NAME_TO_PYTHON_ENDPOINT_MAPPINGS.items():
        if endpoint in requested_endpoint:
            requested_endpoint = requested_endpoint.replace(endpoint, procedure)

    return "".join(part for part in requested_endpoint.split("_"))
