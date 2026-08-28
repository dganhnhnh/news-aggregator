"""Pure taxonomy-seed parsing/validation and prototype-compile policy.

Nothing here opens a database connection or calls the embedder; orchestration
(reading the YAML file, diffing against current rows, calling the embedder)
lives in `workers/cli.py` and `adapters/storage/postgres.py`.
"""

from __future__ import annotations

import hashlib
import math
import unicodedata
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from models.storage_models import FlatTaxonomyNode, TaxonomyNodeSnapshot, TaxonomySnapshot, TopicSnapshot


# A topic without at least this many active exemplars cannot be activated or
# have a prototype compiled: there is nothing meaningful to average.
MIN_EXEMPLARS_TO_ACTIVATE = 3


class TaxonomyValidationError(ValueError):
    def __init__(self, errors: Sequence[str]) -> None:
        super().__init__("; ".join(errors))
        self.errors = list(errors)


@dataclass(frozen=True)
class SeedKeyword:
    value: str
    weight: float = 1.0


@dataclass(frozen=True)
class SeedTopic:
    slug: str
    name: str
    description: str | None
    threshold: float
    exemplars: tuple[str, ...]
    keywords: tuple[SeedKeyword, ...]
    # Not part of the spec's example YAML; lets a seed stage a topic with
    # fewer than three exemplars without failing validation, by declaring it
    # not yet ready to activate.
    active: bool = True


@dataclass(frozen=True)
class SeedTaxonomyNode:
    slug: str
    name: str
    topics: tuple[str, ...]
    children: tuple["SeedTaxonomyNode", ...]


@dataclass(frozen=True)
class SeedTaxonomy:
    slug: str
    name: str
    nodes: tuple[SeedTaxonomyNode, ...]


@dataclass(frozen=True)
class TaxonomySeed:
    version: int
    topics: tuple[SeedTopic, ...]
    taxonomies: tuple[SeedTaxonomy, ...]


def parse_seed(raw: Mapping[str, object]) -> TaxonomySeed:
    """Parse a YAML-loaded mapping into typed dataclasses.

    Only catches structural/type problems (missing keys, wrong shapes);
    business rules are `validate_seed`'s job so every violation can be
    reported together instead of failing on the first one.
    """
    try:
        version = int(raw.get("version", 1))
        topics = tuple(_parse_topic(item) for item in raw.get("topics") or [])
        taxonomies = tuple(_parse_taxonomy(item) for item in raw.get("taxonomies") or [])
    except (TypeError, KeyError, ValueError) as exc:
        raise TaxonomyValidationError([f"malformed taxonomy seed: {exc}"]) from exc
    return TaxonomySeed(version=version, topics=topics, taxonomies=taxonomies)


def _parse_topic(raw: Mapping[str, object]) -> SeedTopic:
    exemplars = tuple(
        str(value).strip() for value in raw.get("exemplars") or [] if str(value).strip()
    )
    keywords = tuple(
        SeedKeyword(value=str(k["value"]).strip(), weight=float(k.get("weight", 1.0)))
        for k in raw.get("keywords") or []
    )
    return SeedTopic(
        slug=str(raw["slug"]),
        name=str(raw["name"]),
        description=(str(raw["description"]) if raw.get("description") else None),
        threshold=float(raw.get("threshold", 0.55)),
        exemplars=exemplars,
        keywords=keywords,
        active=bool(raw.get("active", True)),
    )


def _parse_taxonomy(raw: Mapping[str, object]) -> SeedTaxonomy:
    nodes = tuple(_parse_node(item) for item in raw.get("nodes") or [])
    return SeedTaxonomy(slug=str(raw["slug"]), name=str(raw["name"]), nodes=nodes)


def _parse_node(raw: Mapping[str, object]) -> SeedTaxonomyNode:
    topics = tuple(str(slug) for slug in raw.get("topics") or [])
    children = tuple(_parse_node(item) for item in raw.get("children") or [])
    return SeedTaxonomyNode(
        slug=str(raw["slug"]), name=str(raw["name"]), topics=topics, children=children
    )


def _normalize_exemplar(text: str) -> str:
    return unicodedata.normalize("NFKC", text).strip().lower()


def validate_seed(seed: TaxonomySeed) -> None:
    """Validate every rule up front; raises with every violation at once so
    an operator does not have to fix-and-rerun one error at a time."""
    errors: list[str] = []
    topic_slugs: set[str] = set()
    for topic in seed.topics:
        if topic.slug in topic_slugs:
            errors.append(f"duplicate topic slug: {topic.slug}")
        topic_slugs.add(topic.slug)
        if not 0.0 <= topic.threshold <= 1.0:
            errors.append(f"topic {topic.slug}: threshold must be within [0, 1]")
        exemplar_keys = [_normalize_exemplar(e) for e in topic.exemplars]
        if len(exemplar_keys) != len(set(exemplar_keys)):
            errors.append(f"topic {topic.slug}: duplicate exemplar")
        keyword_values = [k.value.strip().lower() for k in topic.keywords]
        if len(keyword_values) != len(set(keyword_values)):
            errors.append(f"topic {topic.slug}: duplicate keyword")
        for keyword in topic.keywords:
            if keyword.weight <= 0:
                errors.append(
                    f"topic {topic.slug}: keyword {keyword.value!r} weight must be positive"
                )
        if topic.active and len(topic.exemplars) < MIN_EXEMPLARS_TO_ACTIVATE:
            errors.append(
                f"topic {topic.slug}: active topics need at least "
                f"{MIN_EXEMPLARS_TO_ACTIVATE} exemplars, has {len(topic.exemplars)}"
            )

    taxonomy_slugs: set[str] = set()
    for taxonomy in seed.taxonomies:
        if taxonomy.slug in taxonomy_slugs:
            errors.append(f"duplicate taxonomy slug: {taxonomy.slug}")
        taxonomy_slugs.add(taxonomy.slug)
        _validate_nodes(taxonomy.nodes, topic_slugs, errors, path=taxonomy.slug)

    if errors:
        raise TaxonomyValidationError(errors)


def _validate_nodes(
    nodes: Sequence[SeedTaxonomyNode],
    topic_slugs: set[str],
    errors: list[str],
    *,
    path: str,
) -> None:
    seen_slugs: set[str] = set()
    for node in nodes:
        node_path = f"{path}/{node.slug}"
        if node.slug in seen_slugs:
            errors.append(f"duplicate node slug under {path}: {node.slug}")
        seen_slugs.add(node.slug)
        for topic_slug in node.topics:
            if topic_slug not in topic_slugs:
                errors.append(f"node {node_path} references unknown topic: {topic_slug}")
        # Children are a nested list, not parent pointers, so a cycle cannot
        # be expressed by the seed format itself; recursing is enough.
        _validate_nodes(node.children, topic_slugs, errors, path=node_path)


def definition_hash(exemplars: Sequence[str]) -> bytes:
    """Order- and case/whitespace-insensitive hash of an exemplar set.

    A prototype is valid exactly when this matches the topic's current
    active exemplars; deterministic normalization means rebuild does not
    treat a re-saved-but-unchanged seed as a change.
    """
    normalized = sorted(_normalize_exemplar(e) for e in exemplars)
    digest = hashlib.sha256()
    for text in normalized:
        digest.update(text.encode("utf-8"))
        digest.update(b"\n")
    return digest.digest()


def l2_normalize(vector: Sequence[float]) -> list[float]:
    norm = math.sqrt(math.fsum(value * value for value in vector))
    if norm == 0:
        raise ValueError("cannot normalize a zero vector")
    return [value / norm for value in vector]


def compile_prototype(exemplar_embeddings: Sequence[Sequence[float]]) -> list[float]:
    """prototype = L2_normalize(mean(L2_normalize(embed(exemplar_i))))"""
    if len(exemplar_embeddings) < MIN_EXEMPLARS_TO_ACTIVATE:
        raise ValueError(
            f"at least {MIN_EXEMPLARS_TO_ACTIVATE} exemplar embeddings are required"
        )
    normalized = [l2_normalize(vector) for vector in exemplar_embeddings]
    dim = len(normalized[0])
    if dim == 0 or any(len(vector) != dim for vector in normalized):
        raise ValueError("vectors must share a common, non-zero dimension")
    mean = [math.fsum(vector[i] for vector in normalized) / len(normalized) for i in range(dim)]
    return l2_normalize(mean)


def _signal_set(
    signals: Sequence, kind: str
) -> frozenset[tuple[str, float]]:
    return frozenset(
        (s.value, s.weight) for s in signals if s.kind == kind and s.active
    )


def scoring_changed(existing: TopicSnapshot | None, seed_topic: SeedTopic) -> bool:
    """Whether this import would change classifier scoring for the topic.

    Only threshold, exemplars and keywords count; display_name/description
    are presentation and must never bump the revision (spec §6.2).
    """
    if existing is None:
        return True
    if existing.threshold != seed_topic.threshold:
        return True
    existing_exemplars = frozenset(
        _normalize_exemplar(s.value)
        for s in existing.signals
        if s.kind == "exemplar" and s.active
    )
    new_exemplars = frozenset(_normalize_exemplar(e) for e in seed_topic.exemplars)
    if existing_exemplars != new_exemplars:
        return True
    existing_keywords = _signal_set(existing.signals, "keyword")
    new_keywords = frozenset((k.value, k.weight) for k in seed_topic.keywords)
    return existing_keywords != new_keywords


def next_classifier_revision(existing: TopicSnapshot | None, seed_topic: SeedTopic) -> int:
    if existing is None:
        return 1
    if scoring_changed(existing, seed_topic):
        return existing.classifier_revision + 1
    return existing.classifier_revision


def build_node_tree(flat_nodes: Sequence[FlatTaxonomyNode]) -> list[TaxonomyNodeSnapshot]:
    """Rebuild the node tree from flat `taxonomy_nodes` rows of one taxonomy."""
    children_by_parent: dict[int | None, list[FlatTaxonomyNode]] = defaultdict(list)
    for node in flat_nodes:
        children_by_parent[node.parent_id].append(node)
    for siblings in children_by_parent.values():
        siblings.sort(key=lambda n: (n.sort_order, n.slug))

    def _build(node: FlatTaxonomyNode) -> TaxonomyNodeSnapshot:
        return TaxonomyNodeSnapshot(
            slug=node.slug,
            display_name=node.display_name,
            sort_order=node.sort_order,
            topic_slugs=list(node.topic_slugs),
            children=[_build(child) for child in children_by_parent.get(node.id, [])],
        )

    return [_build(node) for node in children_by_parent.get(None, [])]


def to_seed_dict(
    topics: Sequence[TopicSnapshot], taxonomies: Sequence[TaxonomySnapshot]
) -> dict:
    """Deterministic dict ready for `yaml.safe_dump`, in seed-file shape."""
    return {
        "version": 1,
        "topics": [_topic_to_dict(topic) for topic in sorted(topics, key=lambda t: t.slug)],
        "taxonomies": [
            {
                "slug": taxonomy.slug,
                "name": taxonomy.display_name,
                "nodes": [_node_to_dict(node) for node in taxonomy.nodes],
            }
            for taxonomy in sorted(taxonomies, key=lambda t: t.slug)
        ],
    }


def _topic_to_dict(topic: TopicSnapshot) -> dict:
    exemplars = sorted(s.value for s in topic.signals if s.kind == "exemplar" and s.active)
    keywords = sorted(
        (s for s in topic.signals if s.kind == "keyword" and s.active), key=lambda s: s.value
    )
    result: dict = {
        "slug": topic.slug,
        "name": topic.display_name,
        "threshold": topic.threshold,
        "exemplars": exemplars,
        "keywords": [{"value": s.value, "weight": s.weight} for s in keywords],
    }
    if topic.description:
        result["description"] = topic.description
    return result


def _node_to_dict(node: TaxonomyNodeSnapshot) -> dict:
    result: dict = {"slug": node.slug, "name": node.display_name}
    if node.topic_slugs:
        result["topics"] = list(node.topic_slugs)
    if node.children:
        result["children"] = [_node_to_dict(child) for child in node.children]
    return result
