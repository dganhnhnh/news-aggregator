"""Pure online-clustering policy; database orchestration lives elsewhere."""

from __future__ import annotations

import math
from collections.abc import Sequence
from datetime import datetime

from models.storage_models import ClusterCandidate


SIMILARITY_EPSILON = 1e-6


def cosine_similarity(left: Sequence[float], right: Sequence[float]) -> float:
    """Return cosine similarity, rejecting vectors that cannot be compared."""
    if len(left) != len(right):
        raise ValueError("vectors must have the same dimension")
    if not left:
        raise ValueError("vectors must not be empty")
    dot = math.fsum(a * b for a, b in zip(left, right))
    left_norm = math.sqrt(math.fsum(value * value for value in left))
    right_norm = math.sqrt(math.fsum(value * value for value in right))
    if left_norm == 0 or right_norm == 0:
        raise ValueError("cosine similarity is undefined for a zero vector")
    return dot / (left_norm * right_norm)


def running_centroid(
    old_centroid: Sequence[float],
    old_count: int,
    article_embedding: Sequence[float],
) -> list[float]:
    """Compute the exact arithmetic mean after adding one member.

    The stored centroid deliberately remains unnormalized. Normalizing every
    intermediate mean destroys the magnitude needed to reconstruct the prior
    vector sum and makes later updates dependent on article arrival order.
    """
    if old_count <= 0:
        raise ValueError("old_count must be positive")
    if len(old_centroid) != len(article_embedding):
        raise ValueError("vectors must have the same dimension")
    if not old_centroid:
        raise ValueError("vectors must not be empty")
    new_count = old_count + 1
    return [
        (old_value * old_count + article_value) / new_count
        for old_value, article_value in zip(old_centroid, article_embedding)
    ]


def choose_cluster(
    candidates: Sequence[ClusterCandidate],
    *,
    article_at: datetime,
    threshold: float,
) -> ClusterCandidate | None:
    """Choose deterministically among candidates that clear the threshold."""
    eligible = [item for item in candidates if item.similarity >= threshold]
    if not eligible:
        return None
    best_similarity = max(item.similarity for item in eligible)
    tied = [
        item
        for item in eligible
        if best_similarity - item.similarity <= SIMILARITY_EPSILON
    ]
    return min(
        tied,
        key=lambda item: (
            abs((item.last_seen_at - article_at).total_seconds()),
            item.cluster_id,
        ),
    )
