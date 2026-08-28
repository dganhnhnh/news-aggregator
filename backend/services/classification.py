"""Pure topic-classification and cluster-aggregation policy.

Everything here is I/O-free so the same code answers the question in three
places that must never disagree: the classify worker scoring a freshly
embedded article, the cluster worker refreshing a story's label after
attach/merge, and the golden semantic suite replaying fixtures in memory.
"""

from __future__ import annotations

import hashlib
import math
import re
import unicodedata
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

from services.clustering import cosine_similarity


# Week 2 scores with two signals only; `source_prior` is reserved with weight
# zero (spec §2.4). These are a starting point, not a conclusion -- they are
# recorded in `worker_runs.params` so a later sweep can be compared against.
PROTOTYPE_WEIGHT = 0.80
KEYWORD_WEIGHT = 0.20
# Matched keyword weight needed to saturate the keyword component. With a
# 0.20 coefficient, keywords alone top out at 0.20 and can never carry a topic
# over the 0.40 threshold by themselves -- they only ever break a near-tie.
KEYWORD_SATURATION = 2.0

_TOKEN_RE = re.compile(r"\w+", re.UNICODE)


class ClassifierMismatch(RuntimeError):
    """A prototype cannot be compared against an article embedding.

    Raised instead of silently computing a meaningless cosine when the model
    or dimension differs; the worker turns it into an actionable job failure.
    """


@dataclass(frozen=True)
class TopicKeyword:
    value: str
    weight: float = 1.0


@dataclass(frozen=True)
class TopicClassifier:
    """One active topic's complete scoring input."""

    topic_id: int
    slug: str
    threshold: float
    classifier_revision: int
    model: str
    dim: int
    prototype: tuple[float, ...]
    definition_hash: bytes
    keywords: tuple[TopicKeyword, ...] = ()


@dataclass(frozen=True)
class TopicScore:
    topic_id: int
    slug: str
    score: float
    prototype_score: float
    keyword_score: float
    assigned: bool
    classifier_revision: int


@dataclass(frozen=True)
class ArticleClassification:
    scores: tuple[TopicScore, ...]
    max_score: float
    assigned_count: int

    @property
    def assigned(self) -> tuple[TopicScore, ...]:
        return tuple(score for score in self.scores if score.assigned)


@dataclass(frozen=True)
class ClassifierSnapshot:
    """The active classifier set, loaded once per run.

    `topics_without_prototype` is reported rather than silently dropped: an
    active topic with no compiled prototype is an operational error the
    operator has to hear about (`news-worker taxonomy-rebuild`), not a topic
    that quietly stops matching anything.
    """

    classifiers: tuple[TopicClassifier, ...]
    set_hash: bytes
    topics_without_prototype: tuple[str, ...] = ()

    @property
    def thresholds(self) -> dict[int, float]:
        return {item.topic_id: item.threshold for item in self.classifiers}


@dataclass(frozen=True)
class MemberTopicScores:
    """One cluster member's contribution to the cluster-level vote."""

    article_id: int
    source_id: int | None
    scores: Mapping[int, float] = field(default_factory=dict)


@dataclass(frozen=True)
class ClusterTopicScore:
    topic_id: int
    score: float
    assigned: bool


def normalize_tokens(text: str) -> list[str]:
    """NFKC-normalize, case-fold and split into word tokens."""
    normalized = unicodedata.normalize("NFKC", text).casefold()
    return _TOKEN_RE.findall(normalized)


def keyword_score(text: str, keywords: Sequence[TopicKeyword]) -> float:
    """Fraction of keyword weight present in `text`, saturating at 1.0.

    Matching is on token boundaries over a normalized token stream, never raw
    substrings: `AI` must not match inside `said`, and a multi-word keyword
    like `machine learning` must match those two tokens adjacently rather than
    appearing anywhere in the document. A keyword matched many times still
    contributes its weight once -- repetition is not evidence of topicality.
    """
    if not keywords:
        return 0.0
    haystack = f" {' '.join(normalize_tokens(text))} "
    if haystack.isspace():
        return 0.0
    matched = 0.0
    for keyword in keywords:
        needle = " ".join(normalize_tokens(keyword.value))
        if needle and f" {needle} " in haystack:
            matched += keyword.weight
    if matched <= 0:
        return 0.0
    return min(1.0, matched / KEYWORD_SATURATION)


def prototype_score(
    article_embedding: Sequence[float],
    classifier: TopicClassifier,
    *,
    article_model: str,
    article_dim: int,
) -> float:
    """Cosine similarity to the topic prototype, floored at 0.

    A negative cosine means "points away from this topic", which is not more
    informative than "unrelated"; clamping keeps the component in [0, 1] so
    the weighted sum stays comparable against a threshold in the same range.
    """
    if classifier.model != article_model:
        raise ClassifierMismatch(
            f"topic {classifier.slug} prototype uses model {classifier.model!r} "
            f"but the article embedding uses {article_model!r}; "
            "run `news-worker taxonomy-rebuild` after changing the model"
        )
    if classifier.dim != article_dim or len(classifier.prototype) != len(
        article_embedding
    ):
        raise ClassifierMismatch(
            f"topic {classifier.slug} prototype has dimension {classifier.dim} "
            f"but the article embedding has {article_dim}"
        )
    return max(0.0, cosine_similarity(article_embedding, classifier.prototype))


def classify_article(
    *,
    embedding: Sequence[float],
    model: str,
    dim: int,
    text: str,
    classifiers: Sequence[TopicClassifier],
) -> ArticleClassification:
    """Score one article against every active topic.

    Returns a score for every classifier, not only the assigned ones: week 2
    keeps the near-misses so aggregation and evaluation can see them.
    """
    scores: list[TopicScore] = []
    for classifier in classifiers:
        similarity = prototype_score(
            embedding, classifier, article_model=model, article_dim=dim
        )
        keywords = keyword_score(text, classifier.keywords)
        final = PROTOTYPE_WEIGHT * similarity + KEYWORD_WEIGHT * keywords
        scores.append(
            TopicScore(
                topic_id=classifier.topic_id,
                slug=classifier.slug,
                score=final,
                prototype_score=similarity,
                keyword_score=keywords,
                assigned=final >= classifier.threshold,
                classifier_revision=classifier.classifier_revision,
            )
        )
    ordered = tuple(sorted(scores, key=lambda item: (-item.score, item.slug)))
    return ArticleClassification(
        scores=ordered,
        max_score=max((item.score for item in ordered), default=0.0),
        assigned_count=sum(1 for item in ordered if item.assigned),
    )


def aggregate_cluster_topics(
    members: Sequence[MemberTopicScores],
    thresholds: Mapping[int, float],
) -> list[ClusterTopicScore]:
    """Collapse member article scores into one label set per cluster.

    Each publisher gets exactly one vote (spec §2.7), so an outlet that posts
    six articles about the same event cannot outvote five other outlets:

        source_score(topic) = max(article_score(topic) within that source)
        cluster_score(topic) = sum(source_score) / n_distinct_sources

    A member with no row for a topic votes 0 for it, which is what makes the
    denominator the count of *all* voters rather than only interested ones.
    Members with no `source_id` cannot be attributed to a publisher, so each
    one counts as its own voter instead of collapsing into a single anonymous
    bloc that a single outlet could stuff.
    """
    if not members:
        return []
    by_voter: dict[tuple[str, int], dict[int, float]] = defaultdict(dict)
    for member in members:
        voter: tuple[str, int] = (
            ("source", member.source_id)
            if member.source_id is not None
            else ("article", member.article_id)
        )
        bucket = by_voter[voter]
        for topic_id, score in member.scores.items():
            bucket[topic_id] = max(bucket.get(topic_id, 0.0), score)

    voter_count = len(by_voter)
    topic_ids = {topic_id for bucket in by_voter.values() for topic_id in bucket}
    results: list[ClusterTopicScore] = []
    for topic_id in sorted(topic_ids):
        total = math.fsum(bucket.get(topic_id, 0.0) for bucket in by_voter.values())
        score = total / voter_count
        threshold = thresholds.get(topic_id)
        results.append(
            ClusterTopicScore(
                topic_id=topic_id,
                score=score,
                assigned=threshold is not None and score >= threshold,
            )
        )
    return results


def classifier_set_hash(classifiers: Sequence[TopicClassifier]) -> bytes:
    """Fingerprint the active classifier set.

    Covers every input that changes what a score *means* -- which topics are
    active, their thresholds and revisions, and the exemplar set each
    prototype was compiled from -- but not the prototype floats themselves,
    which `definition_hash` already stands in for deterministically. Adding,
    removing, retuning or recompiling a topic changes the hash and so marks
    every previously stored classification stale.
    """
    digest = hashlib.sha256()
    for classifier in sorted(classifiers, key=lambda item: item.slug):
        digest.update(classifier.slug.encode("utf-8"))
        digest.update(b"\x00")
        digest.update(f"{classifier.threshold:.6f}".encode("ascii"))
        digest.update(b"\x00")
        digest.update(str(classifier.classifier_revision).encode("ascii"))
        digest.update(b"\x00")
        digest.update(classifier.model.encode("utf-8"))
        digest.update(b"\x00")
        digest.update(classifier.definition_hash)
        digest.update(b"\n")
    return digest.digest()
