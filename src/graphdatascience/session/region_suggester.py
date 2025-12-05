from typing import Iterable

import textdistance


# AuraDB and AuraDS regions are not the same, so we need to find the closest match.
def closest_match(db_region: str, ds_regions: Iterable[str]) -> str | None:
    curr_max_similarity = 0.0
    closest_option = None

    for region in ds_regions:
        similarity = textdistance.jaro_winkler(db_region, region)
        if similarity > curr_max_similarity:
            closest_option = region
            curr_max_similarity = similarity

    return closest_option
