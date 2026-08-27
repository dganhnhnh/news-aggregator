from datetime import datetime, timedelta, timezone

import pytest

from models.storage_models import ClusterCandidate
from services.clustering import (
    choose_canonical,
    choose_cluster,
    cosine_similarity,
    recompute_centroid,
    running_centroid,
)


NOW = datetime(2026, 8, 26, 12, 0, tzinfo=timezone.utc)


def _candidate(
    cluster_id: int, similarity: float, *, seconds_away: int = 0
) -> ClusterCandidate:
    at = NOW + timedelta(seconds=seconds_away)
    return ClusterCandidate(cluster_id, [1.0, 0.0], 1, at, at, similarity)


def test_running_centroid_keeps_exact_unnormalized_arithmetic_mean() -> None:
    result = running_centroid([1.0, 0.0], 1, [0.0, 1.0])

    assert result == [0.5, 0.5]
    assert sum(value * value for value in result) == pytest.approx(0.5)


def test_running_centroid_is_independent_of_arrival_order() -> None:
    vectors = [[1.0, 0.0], [0.0, 1.0], [-0.5, 0.5]]

    forward = running_centroid(
        running_centroid(vectors[0], 1, vectors[1]), 2, vectors[2]
    )
    reverse = running_centroid(
        running_centroid(vectors[2], 1, vectors[1]), 2, vectors[0]
    )

    assert forward == pytest.approx(reverse)
    assert forward == pytest.approx([1 / 6, 0.5])


def test_cosine_similarity_validates_dimensions_and_zero_vectors() -> None:
    assert cosine_similarity([1.0, 0.0], [2.0, 0.0]) == pytest.approx(1.0)
    with pytest.raises(ValueError, match="same dimension"):
        cosine_similarity([1.0], [1.0, 2.0])
    with pytest.raises(ValueError, match="zero vector"):
        cosine_similarity([0.0, 0.0], [1.0, 0.0])


def test_choose_cluster_applies_threshold_and_deterministic_tie_breaks() -> None:
    candidates = [
        _candidate(3, 0.80, seconds_away=30),
        _candidate(2, 0.8000004, seconds_away=10),
        _candidate(1, 0.8000003, seconds_away=10),
    ]

    assert choose_cluster(candidates, article_at=NOW, threshold=0.75).cluster_id == 1
    assert choose_cluster(candidates, article_at=NOW, threshold=0.81) is None


def test_recompute_centroid_is_the_exact_arithmetic_mean() -> None:
    result = recompute_centroid([[1.0, 0.0], [0.0, 1.0], [-0.5, 0.5]])

    assert result == pytest.approx([1 / 6, 0.5])


def test_recompute_centroid_matches_running_centroid_for_the_same_members() -> None:
    vectors = [[1.0, 0.0], [0.0, 1.0], [-0.5, 0.5]]

    running = running_centroid(running_centroid(vectors[0], 1, vectors[1]), 2, vectors[2])
    exact = recompute_centroid(vectors)

    assert exact == pytest.approx(running)


def test_recompute_centroid_validates_input() -> None:
    with pytest.raises(ValueError, match="must not be empty"):
        recompute_centroid([])
    with pytest.raises(ValueError, match="common, non-zero dimension"):
        recompute_centroid([[1.0, 0.0], [1.0]])
    with pytest.raises(ValueError, match="common, non-zero dimension"):
        recompute_centroid([[], []])


def test_choose_canonical_prefers_older_first_seen_then_lower_id() -> None:
    older = NOW - timedelta(hours=1)

    assert choose_canonical(1, NOW, 2, older) == (2, 1)
    assert choose_canonical(2, older, 1, NOW) == (2, 1)
    # Tie on first_seen_at breaks toward the lower id.
    assert choose_canonical(5, NOW, 3, NOW) == (3, 5)

