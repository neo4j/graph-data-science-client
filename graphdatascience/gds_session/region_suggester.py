from typing import Iterable

import textdistance


def closest_match(query: str, options: Iterable[str]) -> str:
    if not options:
        raise ValueError("No options to choose from.")

    curr_max_similarity = 0.0
    for ep in options:
        similarity = textdistance.jaro_winkler(query, ep)
        if similarity > curr_max_similarity:
            closest_option = ep
            curr_max_similarity = similarity

    return closest_option
