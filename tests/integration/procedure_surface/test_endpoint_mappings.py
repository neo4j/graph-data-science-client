from graphdatascience.session.endpoint_mappings import (
    PROCEDURE_NAME_TO_PYTHON_ENDPOINT_MAPPINGS,
    procedure_name_from_python_endpoint,
)
from tests.integration.procedure_surface.gds_api_spec import EndpointWithModesSpec


def test_procedure_name_from_python_endpoint() -> None:
    assert procedure_name_from_python_endpoint("harmonic_centrality") == "closeness.harmonic"
    assert procedure_name_from_python_endpoint("closeness_centrality") == "closeness"
    assert procedure_name_from_python_endpoint("betweenness_centrality") == "betweenness"
    assert procedure_name_from_python_endpoint("degree_centrality") == "degree"
    assert procedure_name_from_python_endpoint("eigenvector_centrality") == "eigenvector"
    assert procedure_name_from_python_endpoint("topological_link_prediction") == "linkprediction"
    assert procedure_name_from_python_endpoint("influence_maximization_celf") == "influenceMaximization.celf"
    assert procedure_name_from_python_endpoint("clique_counting") == "cliquecounting"
    assert procedure_name_from_python_endpoint("k1_coloring") == "k1coloring"
    assert procedure_name_from_python_endpoint("k_core_decomposition") == "kcore"
    assert procedure_name_from_python_endpoint("max_k_cut") == "maxkcut"
    assert procedure_name_from_python_endpoint("fast_rp") == "fastrp"
    assert procedure_name_from_python_endpoint("graph_sage") == "beta.graphSage"
    assert procedure_name_from_python_endpoint("kge.predict") == "ml.kge.predict"
    assert procedure_name_from_python_endpoint("hash_gnn") == "hashgnn"
    assert procedure_name_from_python_endpoint("a_star") == "astar"
    assert procedure_name_from_python_endpoint("k_spanning_tree") == "kspanningtree"
    assert procedure_name_from_python_endpoint("prize_steiner_tree") == "prizesteinertree"
    assert procedure_name_from_python_endpoint("spanning_tree") == "spanningtree"
    assert procedure_name_from_python_endpoint("steiner_tree") == "steinertree"


def test_all_base_endpoint_mappings_are_used(gds_api_spec: list[EndpointWithModesSpec]) -> None:
    used_mappings: set[str] = set()

    for endpoint_with_modes_spec in gds_api_spec:
        for endpoint_spec in endpoint_with_modes_spec.callable_modes():
            endpoint = endpoint_spec.name.removeprefix("gds.")

            for old, new in PROCEDURE_NAME_TO_PYTHON_ENDPOINT_MAPPINGS.items():
                if old in endpoint:
                    used_mappings.add(old)
                    endpoint = endpoint.replace(old, new)

    unused_mappings = PROCEDURE_NAME_TO_PYTHON_ENDPOINT_MAPPINGS.keys() - used_mappings
    assert not unused_mappings, (
        f"Unused entries in PROCEDURE_NAME_TO_PYTHON_ENDPOINT_MAPPINGS: {sorted(unused_mappings)}. "
        f"Please remove them or fix the mapping."
    )
