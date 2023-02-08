from typing import List

import textdistance

from ..ignored_server_endpoints import IGNORED_SERVER_ENDPOINTS


def generate_suggestive_error_message(requested_endpoint: str, all_endpoints: List[str]) -> str:
    MAX_DIST_FOR_SUGGESTION = 3

    filtered_endpoints = [ep for ep in all_endpoints if ep not in IGNORED_SERVER_ENDPOINTS]

    closest_endpoint = None
    curr_min_dist = 1_000
    for ep in filtered_endpoints:
        dist = textdistance.levenshtein(requested_endpoint, ep)
        if dist <= MAX_DIST_FOR_SUGGESTION and dist < curr_min_dist:
            closest_endpoint = ep
            curr_min_dist = dist

    if closest_endpoint:
        return f"There is no '{requested_endpoint}' to call. Did you mean '{closest_endpoint}'?"
    else:
        return f"There is no '{requested_endpoint}' to call"
