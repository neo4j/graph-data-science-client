import textdistance

from ..ignored_server_endpoints import IGNORED_SERVER_ENDPOINTS


def generate_suggestive_error_message(requested_endpoint: str, all_endpoints: list[str]) -> str:
    MIN_SIMILARITY_FOR_SUGGESTION = 0.9

    closest_endpoint = None
    curr_max_similarity = 0.0
    for ep in all_endpoints:
        similarity = textdistance.jaro_winkler(requested_endpoint, ep)
        if similarity >= MIN_SIMILARITY_FOR_SUGGESTION:
            if similarity > curr_max_similarity or (
                similarity == curr_max_similarity and ep not in IGNORED_SERVER_ENDPOINTS
            ):
                closest_endpoint = ep
                curr_max_similarity = similarity

    if closest_endpoint:
        if closest_endpoint in IGNORED_SERVER_ENDPOINTS:
            return (
                f"There is no '{requested_endpoint}' to call. It is similar to '{closest_endpoint}' which is a valid "
                f"GDS server endpoint. '{closest_endpoint}' does however not have a corresponding Python method"
            )
        else:
            return f"There is no '{requested_endpoint}' to call. Did you mean '{closest_endpoint}'?"
    else:
        return f"There is no '{requested_endpoint}' to call"
