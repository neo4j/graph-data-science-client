import math

import pytest

from graphdatascience.procedure_surface.api.similarity.similarity_functions import SimilarityFunctions


@pytest.fixture
def sim() -> SimilarityFunctions:
    return SimilarityFunctions()


# --- jaccard ---


def test_jaccard_identical(sim: SimilarityFunctions) -> None:
    assert sim.jaccard([1.0, 2.0, 3.0], [1.0, 2.0, 3.0]) == pytest.approx(1.0)


def test_jaccard_disjoint(sim: SimilarityFunctions) -> None:
    assert sim.jaccard([1.0, 0.0], [2.0, 3.0]) == pytest.approx(0)


def test_jaccard_partial_overlap(sim: SimilarityFunctions) -> None:
    assert sim.jaccard([1.0, 1.0, 2.0], [0.0, 1.0, 1.0]) == pytest.approx(2 / 4)


def test_jaccard_all_zeros(sim: SimilarityFunctions) -> None:
    assert sim.jaccard([0.0, 0.0], [0.0, 0.0]) == pytest.approx(1.0)


def test_jaccard_ints(sim: SimilarityFunctions) -> None:
    assert sim.jaccard([1, 1, 2, 3], [1, 2, 2, 4]) == pytest.approx(1 / 3)


# --- overlap ---


def test_overlap_identical(sim: SimilarityFunctions) -> None:
    assert sim.overlap([1.0, 2.0], [1.0, 2.0]) == pytest.approx(1.0)


def test_overlap_subset(sim: SimilarityFunctions) -> None:
    assert sim.overlap([1.0, 0.0], [1.0, 1.0]) == pytest.approx(1 / 2)


def test_overlap_no_overlap(sim: SimilarityFunctions) -> None:
    assert sim.overlap([1.0, 0.0], [2.0, 3.0]) == pytest.approx(0.0)


def test_overlap_all_zeros(sim: SimilarityFunctions) -> None:
    assert sim.overlap([0.0], [0.0]) == pytest.approx(1.0)


def test_overlap_mismatched_lengths(sim: SimilarityFunctions) -> None:
    with pytest.raises(ValueError):
        sim.overlap([1.0], [1.0, 2.0])


# --- cosine ---


def test_cosine_identical(sim: SimilarityFunctions) -> None:
    assert sim.cosine([1.0, 2.0, 3.0], [1.0, 2.0, 3.0]) == pytest.approx(1.0)


def test_cosine_orthogonal(sim: SimilarityFunctions) -> None:
    assert sim.cosine([1.0, 0.0], [0.0, 1.0]) == pytest.approx(0.0)


def test_cosine_opposite(sim: SimilarityFunctions) -> None:
    assert sim.cosine([1.0, 0.0], [-1.0, 0.0]) == pytest.approx(-1.0)


def test_cosine_integer_inputs(sim: SimilarityFunctions) -> None:
    assert sim.cosine([1, 2], [1, 2]) == pytest.approx(1.0)


def test_cosine_zero_vector(sim: SimilarityFunctions) -> None:
    assert sim.cosine([0.0, 0.0], [1.0, 2.0]) == pytest.approx(0.0)


def test_cosine_mismatched_lengths(sim: SimilarityFunctions) -> None:
    with pytest.raises(ValueError):
        sim.cosine([1.0], [1.0, 2.0])


# --- pearson ---


def test_pearson_perfect_positive(sim: SimilarityFunctions) -> None:
    assert sim.pearson([1.0, 2.0, 3.0], [2.0, 4.0, 6.0]) == pytest.approx(1.0)


def test_pearson_perfect_negative(sim: SimilarityFunctions) -> None:
    assert sim.pearson([1.0, 2.0, 3.0], [3.0, 2.0, 1.0]) == pytest.approx(-1.0)


def test_pearson_no_correlation(sim: SimilarityFunctions) -> None:
    # Constant second vector → zero std dev → returns 0
    assert sim.pearson([1.0, 2.0, 3.0], [5.0, 5.0, 5.0]) == pytest.approx(0.0)


def test_pearson_integer_inputs(sim: SimilarityFunctions) -> None:
    assert sim.pearson([1, 2, 3], [2, 4, 6]) == pytest.approx(1.0)


def test_pearson_mismatched_lengths(sim: SimilarityFunctions) -> None:
    with pytest.raises(ValueError):
        sim.pearson([1.0], [1.0, 2.0])


# --- euclidean_distance ---


def test_euclidean_distance_same_vector(sim: SimilarityFunctions) -> None:
    assert sim.euclidean_distance([1.0, 2.0, 3.0], [1.0, 2.0, 3.0]) == pytest.approx(0.0)


def test_euclidean_distance_known(sim: SimilarityFunctions) -> None:
    assert sim.euclidean_distance([0.0, 0.0], [3.0, 4.0]) == pytest.approx(5.0)


def test_euclidean_distance_integer_inputs(sim: SimilarityFunctions) -> None:
    assert sim.euclidean_distance([0, 0], [3, 4]) == pytest.approx(5.0)


def test_euclidean_distance_mismatched_lengths(sim: SimilarityFunctions) -> None:
    with pytest.raises(ValueError):
        sim.euclidean_distance([1.0], [1.0, 2.0])


# --- euclidean (similarity) ---


def test_euclidean_same_vector(sim: SimilarityFunctions) -> None:
    assert sim.euclidean([1.0, 2.0], [1.0, 2.0]) == pytest.approx(1.0)


def test_euclidean_known_distance(sim: SimilarityFunctions) -> None:
    assert sim.euclidean([0.0, 0.0], [3.0, 4.0]) == pytest.approx(1 / 6)


def test_euclidean_always_positive(sim: SimilarityFunctions) -> None:
    result = sim.euclidean([0.0, 0.0], [100.0, 100.0])
    assert result > 0.0


def test_euclidean_mismatched_lengths(sim: SimilarityFunctions) -> None:
    with pytest.raises(ValueError):
        sim.euclidean([1.0], [1.0, 2.0])


# --- client-side: no database calls needed ---


def test_all_functions_are_client_side() -> None:
    """SimilarityFunctions requires no query runner — pure Python."""
    sim = SimilarityFunctions()
    assert math.isfinite(sim.jaccard([1.0], [1.0]))
    assert math.isfinite(sim.overlap([1.0], [1.0]))
    assert math.isfinite(sim.cosine([1.0], [1.0]))
    assert math.isfinite(sim.pearson([1.0], [1.0]))
    assert math.isfinite(sim.euclidean_distance([1.0], [1.0]))
    assert math.isfinite(sim.euclidean([1.0], [1.0]))
