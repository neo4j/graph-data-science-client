from graphdatascience.session.endpoint_mappings import PROCEDURE_NAME_TO_PYTHON_ENDPOINT_MAPPINGS
from tests.integration.procedure_surface.gds_api_spec import EndpointWithModesSpec


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
