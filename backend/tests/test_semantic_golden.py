"""Real-model semantic smoke suite for the Week 2 MVP.

This module is deliberately excluded from the default pytest selection.  It
uses the production MiniLM adapter and the production text builder, scoring,
online-clustering, and merge policies; deterministic fakes remain the right
tool for the ordinary unit suite.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from importlib.metadata import version
from pathlib import Path
from typing import Any

import pytest
import yaml

from adapters.embedding.fastembed_embedder import FastEmbedEmbedder
from models.storage_models import ClusterCandidate
from services.classification import (
    TopicClassifier,
    TopicKeyword,
    classify_article,
)
from services.clustering import (
    choose_canonical,
    choose_cluster,
    cosine_similarity,
    recompute_centroid,
    running_centroid,
)
from services.taxonomy import (
    compile_prototype,
    definition_hash,
    parse_seed,
    validate_seed,
)
from services.text import article_embed_text


pytestmark = pytest.mark.semantic

ROOT = Path(__file__).resolve().parents[2]
FIXTURE_PATH = Path(__file__).parent / "fixtures" / "week2_golden_cases.json"
SEED_PATH = ROOT / "data" / "taxonomy.seed.yaml"
CLUSTER_THRESHOLD = 0.75
MERGE_THRESHOLD = 0.85
CLUSTER_WINDOW = timedelta(hours=72)


def _load_cases() -> list[dict[str, Any]]:
    with FIXTURE_PATH.open(encoding="utf-8") as stream:
        cases = json.load(stream)
    assert isinstance(cases, list)
    return cases


GOLDEN_CASES = _load_cases()


@dataclass(frozen=True)
class _SemanticContext:
    model_name: str
    fastembed_version: str
    dim: int
    classifiers: tuple[TopicClassifier, ...]
    article_vectors: dict[str, list[float]]

    @property
    def report_context(self) -> str:
        return (
            f"model={self.model_name}, fastembed={self.fastembed_version}, "
            f"dim={self.dim}, "
            "input_builder=services.text.article_embed_text, "
            f"cluster_threshold={CLUSTER_THRESHOLD}, "
            f"merge_threshold={MERGE_THRESHOLD}, "
            f"window_hours={CLUSTER_WINDOW.total_seconds() / 3600:g}"
        )


@dataclass
class _MemoryCluster:
    cluster_id: int
    first_seen_at: datetime
    last_seen_at: datetime
    centroid: list[float]
    members: list[str]


@pytest.fixture(scope="session")
def semantic_context() -> _SemanticContext:
    """Compile the checked-in taxonomy and embed all golden inputs once."""
    with SEED_PATH.open(encoding="utf-8") as stream:
        seed = parse_seed(yaml.safe_load(stream))
    validate_seed(seed)

    embedder = FastEmbedEmbedder()
    active_topics = sorted(
        (topic for topic in seed.topics if topic.active), key=lambda topic: topic.slug
    )
    exemplar_texts = [
        exemplar for topic in active_topics for exemplar in topic.exemplars
    ]
    exemplar_vectors = embedder.embed(exemplar_texts)

    classifiers: list[TopicClassifier] = []
    offset = 0
    for topic_id, topic in enumerate(active_topics, start=1):
        topic_vectors = exemplar_vectors[offset : offset + len(topic.exemplars)]
        offset += len(topic.exemplars)
        prototype = compile_prototype(topic_vectors)
        classifiers.append(
            TopicClassifier(
                topic_id=topic_id,
                slug=topic.slug,
                threshold=topic.threshold,
                classifier_revision=1,
                model=embedder.model_name,
                dim=embedder.dim,
                prototype=tuple(prototype),
                definition_hash=definition_hash(topic.exemplars),
                keywords=tuple(
                    TopicKeyword(value=keyword.value, weight=keyword.weight)
                    for keyword in topic.keywords
                ),
            )
        )

    unique_articles: dict[str, str] = {}
    for case in GOLDEN_CASES:
        for article in case["articles"]:
            text, _ = article_embed_text(
                title=article["title"],
                content=article["text"],
                feed_summary="",
                content_source="feed_content",
            )
            previous = unique_articles.setdefault(article["key"], text)
            assert previous == text, f"article key {article['key']!r} is not stable"

    keys = list(unique_articles)
    vectors = embedder.embed([unique_articles[key] for key in keys])
    return _SemanticContext(
        model_name=embedder.model_name,
        fastembed_version=version("fastembed"),
        dim=embedder.dim,
        classifiers=tuple(classifiers),
        article_vectors=dict(zip(keys, vectors, strict=True)),
    )


def _event_at(article: dict[str, Any]) -> datetime:
    return datetime.fromisoformat(article["published_at"].replace("Z", "+00:00"))


def _cluster_case(
    articles: list[dict[str, Any]],
    context: _SemanticContext,
) -> list[_MemoryCluster]:
    """Replay production online matching followed by the production merge pass."""
    clusters: list[_MemoryCluster] = []
    next_cluster_id = 1

    for article in articles:
        article_at = _event_at(article)
        vector = context.article_vectors[article["key"]]
        candidates = [
            ClusterCandidate(
                cluster_id=cluster.cluster_id,
                centroid=cluster.centroid,
                centroid_count=len(cluster.members),
                first_seen_at=cluster.first_seen_at,
                last_seen_at=cluster.last_seen_at,
                similarity=cosine_similarity(vector, cluster.centroid),
            )
            for cluster in clusters
            if cluster.last_seen_at >= article_at - CLUSTER_WINDOW
            and cluster.first_seen_at <= article_at + CLUSTER_WINDOW
        ]
        best = choose_cluster(
            candidates,
            article_at=article_at,
            threshold=CLUSTER_THRESHOLD,
        )
        if best is None:
            clusters.append(
                _MemoryCluster(
                    cluster_id=next_cluster_id,
                    first_seen_at=article_at,
                    last_seen_at=article_at,
                    centroid=list(vector),
                    members=[article["key"]],
                )
            )
            next_cluster_id += 1
            continue

        cluster = next(
            item for item in clusters if item.cluster_id == best.cluster_id
        )
        cluster.centroid = running_centroid(
            cluster.centroid, len(cluster.members), vector
        )
        cluster.first_seen_at = min(cluster.first_seen_at, article_at)
        cluster.last_seen_at = max(cluster.last_seen_at, article_at)
        cluster.members.append(article["key"])

    while True:
        mergeable: list[tuple[float, _MemoryCluster, _MemoryCluster]] = []
        for index, left in enumerate(clusters):
            for right in clusters[index + 1 :]:
                overlap = (
                    left.first_seen_at <= right.last_seen_at
                    and right.first_seen_at <= left.last_seen_at
                )
                if not overlap:
                    continue
                similarity = cosine_similarity(left.centroid, right.centroid)
                if similarity >= MERGE_THRESHOLD:
                    mergeable.append((similarity, left, right))
        if not mergeable:
            break

        _, left, right = max(
            mergeable,
            key=lambda item: (item[0], -item[1].cluster_id, -item[2].cluster_id),
        )
        canonical_id, loser_id = choose_canonical(
            left.cluster_id,
            left.first_seen_at,
            right.cluster_id,
            right.first_seen_at,
        )
        canonical = left if left.cluster_id == canonical_id else right
        loser = right if right.cluster_id == loser_id else left
        canonical.members.extend(loser.members)
        canonical.first_seen_at = min(
            canonical.first_seen_at, loser.first_seen_at
        )
        canonical.last_seen_at = max(canonical.last_seen_at, loser.last_seen_at)
        canonical.centroid = recompute_centroid(
            [context.article_vectors[key] for key in canonical.members]
        )
        clusters.remove(loser)

    return clusters


def _member_sets(clusters: list[_MemoryCluster]) -> set[frozenset[str]]:
    return {frozenset(cluster.members) for cluster in clusters}


def _assert_fixture_contract(
    case: dict[str, Any], context: _SemanticContext
) -> None:
    assert "expected_member_kinds" not in case, (
        "Week 2 retains every valid article; member_kind/near_duplicate is backlog"
    )
    classifier_slugs = {classifier.slug for classifier in context.classifiers}
    article_keys = [article["key"] for article in case["articles"]]
    assert len(article_keys) == len(set(article_keys)), "article keys must be unique"
    expected_members = [
        key for members in case["expected_clusters"] for key in members
    ]
    assert sorted(expected_members) == sorted(article_keys), (
        "expected_clusters must contain every article exactly once"
    )
    for article in case["articles"]:
        expected = set(article["expected_topics"])
        forbidden = set(article["forbidden_topics"])
        assert expected.isdisjoint(forbidden)
        unknown = (expected | forbidden) - classifier_slugs
        assert not unknown, (
            f"fixture references topics absent from the runtime seed: {sorted(unknown)}"
        )


@pytest.mark.parametrize(
    "case",
    GOLDEN_CASES,
    ids=[case["case_id"] for case in GOLDEN_CASES],
)
def test_semantic_golden_case(
    case: dict[str, Any], semantic_context: _SemanticContext
) -> None:
    context = semantic_context
    assert len(GOLDEN_CASES) == 10, "Week 2 semantic suite must stay at 10 scenarios"
    assert sorted(case["scenario_number"] for case in GOLDEN_CASES) == list(
        range(1, 11)
    )
    _assert_fixture_contract(case, context)

    for article in case["articles"]:
        input_text, _ = article_embed_text(
            title=article["title"],
            content=article["text"],
            feed_summary="",
            content_source="feed_content",
        )
        result = classify_article(
            embedding=context.article_vectors[article["key"]],
            model=context.model_name,
            dim=context.dim,
            text=input_text,
            classifiers=context.classifiers,
        )
        assigned = {score.slug for score in result.assigned}
        expected = set(article["expected_topics"])
        forbidden = set(article["forbidden_topics"])
        assert expected <= assigned, (
            f"missing required topics {sorted(expected - assigned)}; "
            f"assigned={sorted(assigned)}; {context.report_context}"
        )
        assert forbidden.isdisjoint(assigned), (
            f"assigned forbidden topics {sorted(forbidden & assigned)}; "
            f"assigned={sorted(assigned)}; {context.report_context}"
        )
        if article.get("expected_uncategorized"):
            assert not assigned, (
                f"expected uncategorized but assigned={sorted(assigned)}; "
                f"{context.report_context}"
            )

    clusters = _cluster_case(case["articles"], context)
    expected_clusters = {
        frozenset(members) for members in case["expected_clusters"]
    }
    assert _member_sets(clusters) == expected_clusters, (
        f"cluster partition differs; actual="
        f"{sorted(sorted(members) for members in _member_sets(clusters))}; "
        f"expected={sorted(sorted(members) for members in expected_clusters)}; "
        f"{context.report_context}"
    )

    distinct_sources = case.get("expected_distinct_sources")
    if distinct_sources is not None:
        sources_by_key = {
            article["key"]: article["source"] for article in case["articles"]
        }
        assert all(
            len({sources_by_key[key] for key in cluster.members})
            == distinct_sources
            for cluster in clusters
        )

    if case["case_id"] == "arrival_order_invariance":
        reversed_clusters = _cluster_case(list(reversed(case["articles"])), context)
        assert _member_sets(reversed_clusters) == _member_sets(clusters), (
            f"arrival order changed canonical member sets; {context.report_context}"
        )
