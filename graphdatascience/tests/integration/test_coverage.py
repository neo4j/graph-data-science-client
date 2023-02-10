from graphdatascience.error.uncallable_namespace import UncallableNamespace
from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.ignored_server_endpoints import IGNORED_SERVER_ENDPOINTS


def test_coverage(gds: GraphDataScience) -> None:
    result = gds.list()
    for server_endpoint in result["name"].tolist():
        if server_endpoint in IGNORED_SERVER_ENDPOINTS:
            continue

        # Divide the endpoint string into its parts and cut out the "gds" prefix
        endpoint_components = server_endpoint.split(".")[1:]

        # Check that each step of the string building is a valid object
        base = gds
        for idx, attr in enumerate(endpoint_components):
            try:
                base = getattr(base, attr)
                assert base

                # When we have constructed the full chain, we should make sure
                # that the object is callable, and callable without errors.
                if idx < len(endpoint_components) - 1:
                    continue

                assert callable(base)

                if isinstance(base, UncallableNamespace):
                    raise AssertionError()
            except Exception:
                raise AssertionError(f"Could not find a client endpoint for the {server_endpoint} server endpoint")
