from datetime import datetime, timedelta, timezone

import pytest

from models.storage_models import ClusterCandidate
from services.clustering import choose_cluster, cosine_similarity, running_centroid


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

