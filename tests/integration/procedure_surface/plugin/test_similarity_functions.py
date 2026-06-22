"""
Integration test: compare the Python SimilarityFunctions implementation against
the GDS plugin running in a real Neo4j container.

10 000 random float vector pairs are generated once, all six GDS similarity
functions are evaluated in a single batched Cypher UNWIND query, and the
results are compared element-by-element against the pure-Python implementation.
"""

from typing import Generator

import numpy as np
import pandas as pd
import pytest

from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.procedure_surface.api.similarity.similarity_functions import SimilarityFunctions
from graphdatascience.session.dbms_connection_info import DbmsConnectionInfo

VECTOR_DIM = 10
NUM_PAIRS = 100_000


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def gds(neo4j_connection: DbmsConnectionInfo) -> Generator[GraphDataScience, None, None]:
    g = GraphDataScience(
        endpoint=neo4j_connection.get_uri(),
        auth=(neo4j_connection.username, neo4j_connection.password),  # type: ignore[arg-type]
    )
    yield g
    g.close()


@pytest.fixture(scope="module")
def vector_pairs() -> list[tuple[list[float], list[float]]]:
    rng = np.random.default_rng(42)
    # Return as list[tuple[list[float], list[float]]] by converting
    # the generated numpy integer arrays to float lists.
    return [
        (
            rng.integers(-100, 100, size=VECTOR_DIM).astype(float).tolist(),
            rng.integers(-100, 100, size=VECTOR_DIM).astype(float).tolist(),
        )
        for _ in range(NUM_PAIRS)
    ]


@pytest.fixture(scope="module")
def plugin_results(
    gds: GraphDataScience,
    vector_pairs: list[tuple[list[float], list[float]]],
) -> pd.DataFrame:
    """Evaluate all six GDS similarity functions in one batched Cypher query."""
    return gds.run_cypher(
        """
        UNWIND $pairs AS pair
        RETURN
            gds.similarity.jaccard(pair[0], pair[1])           AS jaccard,
            gds.similarity.overlap(pair[0], pair[1])           AS overlap,
            gds.similarity.cosine(pair[0], pair[1])            AS cosine,
            gds.similarity.pearson(pair[0], pair[1])           AS pearson,
            gds.similarity.euclidean(pair[0], pair[1])         AS euclidean,
            gds.similarity.euclideanDistance(pair[0], pair[1]) AS euclideanDistance
        """,
        params={"pairs": [[v1, v2] for v1, v2 in vector_pairs]},
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _assert_approx(
    actual: float,
    expected: float,
    fn_name: str,
    v1: list[float],
    v2: list[float],
    pair_index: int,
) -> None:
    if actual != pytest.approx(expected, abs=1e-9):
        pytest.fail(
            f"{fn_name} mismatch at pair {pair_index}:\n"
            f"  python : {actual}\n"
            f"  plugin : {expected}\n"
            f"  vector1: {v1}\n"
            f"  vector2: {v2}"
        )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_jaccard(
    vector_pairs: list[tuple[list[float], list[float]]],
    plugin_results: pd.DataFrame,
) -> None:
    sim = SimilarityFunctions()
    for i, (v1, v2) in enumerate(vector_pairs):
        _assert_approx(sim.jaccard(v1, v2), plugin_results.iloc[i]["jaccard"], "jaccard", v1, v2, i)


def test_overlap(
    vector_pairs: list[tuple[list[float], list[float]]],
    plugin_results: pd.DataFrame,
) -> None:
    sim = SimilarityFunctions()
    for i, (v1, v2) in enumerate(vector_pairs):
        _assert_approx(sim.overlap(v1, v2), plugin_results.iloc[i]["overlap"], "overlap", v1, v2, i)


def test_cosine(
    vector_pairs: list[tuple[list[float], list[float]]],
    plugin_results: pd.DataFrame,
) -> None:
    sim = SimilarityFunctions()
    for i, (v1, v2) in enumerate(vector_pairs):
        _assert_approx(sim.cosine(v1, v2), plugin_results.iloc[i]["cosine"], "cosine", v1, v2, i)


def test_pearson(
    vector_pairs: list[tuple[list[float], list[float]]],
    plugin_results: pd.DataFrame,
) -> None:
    sim = SimilarityFunctions()
    for i, (v1, v2) in enumerate(vector_pairs):
        _assert_approx(sim.pearson(v1, v2), plugin_results.iloc[i]["pearson"], "pearson", v1, v2, i)


def test_euclidean(
    vector_pairs: list[tuple[list[float], list[float]]],
    plugin_results: pd.DataFrame,
) -> None:
    sim = SimilarityFunctions()
    for i, (v1, v2) in enumerate(vector_pairs):
        _assert_approx(sim.euclidean(v1, v2), plugin_results.iloc[i]["euclidean"], "euclidean", v1, v2, i)


def test_euclidean_distance(
    vector_pairs: list[tuple[list[float], list[float]]],
    plugin_results: pd.DataFrame,
) -> None:
    sim = SimilarityFunctions()
    for i, (v1, v2) in enumerate(vector_pairs):
        _assert_approx(
            sim.euclidean_distance(v1, v2), plugin_results.iloc[i]["euclideanDistance"], "euclidean_distance", v1, v2, i
        )
