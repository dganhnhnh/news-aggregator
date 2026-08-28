import asyncio
import os

import pytest
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import create_async_engine

from adapters.storage.postgres import PostgresRawStorage
from adapters.storage.schema import (
    taxonomies,
    taxonomy_node_topics,
    taxonomy_nodes,
    topic_prototypes,
    topic_signals,
    topics,
)
from services.taxonomy import SeedKeyword, SeedTaxonomy, SeedTaxonomyNode, SeedTopic, TaxonomySeed


DATABASE_URL = os.getenv("TEST_DATABASE_URL")
pytestmark = pytest.mark.integration

TOPIC_SLUG = "taxonomy-integration-topic"
TAXONOMY_SLUG = "taxonomy-integration-tree"


def _topic(
    slug: str = TOPIC_SLUG,
    *,
    exemplars: tuple[str, ...] = ("one exemplar here", "second exemplar here", "third exemplar here"),
    threshold: float = 0.55,
) -> SeedTopic:
    return SeedTopic(
        slug=slug,
        name="Integration Topic",
        description="A topic used only by the taxonomy storage integration test.",
        threshold=threshold,
        exemplars=exemplars,
        keywords=(SeedKeyword("integration-test-keyword"),),
        active=True,
    )


def _seed(
    *,
    topic: SeedTopic,
    leaf_slug: str = "leaf-a",
) -> TaxonomySeed:
    tree = SeedTaxonomy(
        slug=TAXONOMY_SLUG,
        name="Integration Tree",
        nodes=(
            SeedTaxonomyNode(
                slug="root",
                name="Root",
                topics=(),
                children=(
                    SeedTaxonomyNode(slug=leaf_slug, name="Leaf", topics=(topic.slug,), children=()),
                ),
            ),
        ),
    )
    return TaxonomySeed(version=1, topics=(topic,), taxonomies=(tree,))


def _seed_with_topic_in_leaf(topic: SeedTopic, *, active_leaf: str) -> TaxonomySeed:
    """Both leaf nodes always present; only which leaf lists the topic changes.

    This is what "moving a topic between taxonomy nodes" means in practice --
    the node structure is stable and only membership shifts, unlike
    `_seed()`, which swaps which single leaf exists in the tree at all.
    """
    tree = SeedTaxonomy(
        slug=TAXONOMY_SLUG,
        name="Integration Tree",
        nodes=(
            SeedTaxonomyNode(
                slug="root",
                name="Root",
                topics=(),
                children=(
                    SeedTaxonomyNode(
                        slug="leaf-a",
                        name="Leaf A",
                        topics=(topic.slug,) if active_leaf == "leaf-a" else (),
                        children=(),
                    ),
                    SeedTaxonomyNode(
                        slug="leaf-b",
                        name="Leaf B",
                        topics=(topic.slug,) if active_leaf == "leaf-b" else (),
                        children=(),
                    ),
                ),
            ),
        ),
    )
    return TaxonomySeed(version=1, topics=(topic,), taxonomies=(tree,))


async def _cleanup(engine) -> None:
    async with engine.begin() as connection:
        topic_rows = list(
            (
                await connection.execute(select(topics.c.id).where(topics.c.slug == TOPIC_SLUG))
            ).scalars()
        )
        if topic_rows:
            await connection.execute(
                delete(topic_prototypes).where(topic_prototypes.c.topic_id.in_(topic_rows))
            )
            await connection.execute(
                delete(topic_signals).where(topic_signals.c.topic_id.in_(topic_rows))
            )
        taxonomy_row = (
            await connection.execute(
                select(taxonomies.c.id).where(taxonomies.c.slug == TAXONOMY_SLUG)
            )
        ).first()
        if taxonomy_row is not None:
            node_rows = list(
                (
                    await connection.execute(
                        select(taxonomy_nodes.c.id).where(
                            taxonomy_nodes.c.taxonomy_id == taxonomy_row.id
                        )
                    )
                ).scalars()
            )
            if node_rows:
                await connection.execute(
                    delete(taxonomy_node_topics).where(
                        taxonomy_node_topics.c.node_id.in_(node_rows)
                    )
                )
            await connection.execute(
                delete(taxonomy_nodes).where(taxonomy_nodes.c.taxonomy_id == taxonomy_row.id)
            )
            await connection.execute(delete(taxonomies).where(taxonomies.c.id == taxonomy_row.id))
        await connection.execute(delete(topics).where(topics.c.slug == TOPIC_SLUG))


def test_importing_the_same_seed_twice_creates_no_duplicates() -> None:
    if not DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL is not set")
    asyncio.run(_test_importing_the_same_seed_twice_creates_no_duplicates())


async def _test_importing_the_same_seed_twice_creates_no_duplicates() -> None:
    storage = PostgresRawStorage(DATABASE_URL)
    engine = create_async_engine(DATABASE_URL)
    try:
        await storage.check_ready()
        await _cleanup(engine)
        seed = _seed(topic=_topic())

        first = await storage.import_taxonomy(seed)
        second = await storage.import_taxonomy(seed)

        assert first.topics[0].created is True
        assert second.topics[0].created is False
        assert second.topics[0].revision_bumped is False

        async with engine.connect() as connection:
            topic_count = (
                await connection.execute(
                    select(topics.c.id).where(topics.c.slug == TOPIC_SLUG)
                )
            ).all()
            assert len(topic_count) == 1

            signal_rows = (
                await connection.execute(
                    select(topic_signals).where(
                        topic_signals.c.topic_id == topic_count[0].id,
                        topic_signals.c.active.is_(True),
                    )
                )
            ).all()
            # one exemplar signal per exemplar plus one keyword, never duplicated
            assert len(signal_rows) == 4

            node_rows = (
                await connection.execute(
                    select(taxonomy_nodes.c.slug).where(
                        taxonomy_nodes.c.taxonomy_id.in_(
                            select(taxonomies.c.id).where(taxonomies.c.slug == TAXONOMY_SLUG)
                        )
                    )
                )
            ).all()
            assert sorted(row.slug for row in node_rows) == ["leaf-a", "root"]
    finally:
        await _cleanup(engine)
        await engine.dispose()
        await storage.close()


def test_moving_a_topic_between_nodes_does_not_bump_classifier_revision() -> None:
    if not DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL is not set")
    asyncio.run(_test_moving_a_topic_between_nodes_does_not_bump_classifier_revision())


async def _test_moving_a_topic_between_nodes_does_not_bump_classifier_revision() -> None:
    storage = PostgresRawStorage(DATABASE_URL)
    engine = create_async_engine(DATABASE_URL)
    try:
        await storage.check_ready()
        await _cleanup(engine)
        topic = _topic()

        await storage.import_taxonomy(_seed_with_topic_in_leaf(topic, active_leaf="leaf-a"))
        topics_before, _ = await storage.export_taxonomy()
        revision_before = next(t for t in topics_before if t.slug == topic.slug).classifier_revision

        moved = await storage.import_taxonomy(_seed_with_topic_in_leaf(topic, active_leaf="leaf-b"))

        assert moved.topics[0].revision_bumped is False

        topics_after, taxonomies_after = await storage.export_taxonomy()
        topic_after = next(t for t in topics_after if t.slug == topic.slug)
        assert topic_after.classifier_revision == revision_before

        tree = next(t for t in taxonomies_after if t.slug == TAXONOMY_SLUG)
        root = next(n for n in tree.nodes if n.slug == "root")
        leaf_slugs_with_topic = {
            child.slug for child in root.children if topic.slug in child.topic_slugs
        }
        assert leaf_slugs_with_topic == {"leaf-b"}
    finally:
        await _cleanup(engine)
        await engine.dispose()
        await storage.close()


def test_editing_an_exemplar_bumps_classifier_revision_exactly_once() -> None:
    if not DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL is not set")
    asyncio.run(_test_editing_an_exemplar_bumps_classifier_revision_exactly_once())


async def _test_editing_an_exemplar_bumps_classifier_revision_exactly_once() -> None:
    storage = PostgresRawStorage(DATABASE_URL)
    engine = create_async_engine(DATABASE_URL)
    try:
        await storage.check_ready()
        await _cleanup(engine)
        topic = _topic()

        await storage.import_taxonomy(_seed(topic=topic))
        topics_before, _ = await storage.export_taxonomy()
        revision_before = next(t for t in topics_before if t.slug == topic.slug).classifier_revision

        edited = _topic(
            exemplars=("one exemplar here", "second exemplar here", "a brand new exemplar")
        )
        outcome = await storage.import_taxonomy(_seed(topic=edited))
        assert outcome.topics[0].revision_bumped is True

        topics_after, _ = await storage.export_taxonomy()
        revision_after = next(t for t in topics_after if t.slug == topic.slug).classifier_revision
        assert revision_after == revision_before + 1

        # Re-importing the already-edited seed again must not bump a second time.
        outcome_repeat = await storage.import_taxonomy(_seed(topic=edited))
        assert outcome_repeat.topics[0].revision_bumped is False
        topics_repeat, _ = await storage.export_taxonomy()
        revision_repeat = next(t for t in topics_repeat if t.slug == topic.slug).classifier_revision
        assert revision_repeat == revision_after
    finally:
        await _cleanup(engine)
        await engine.dispose()
        await storage.close()


def test_prune_deactivates_missing_topics_and_removes_missing_nodes() -> None:
    if not DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL is not set")
    asyncio.run(_test_prune_deactivates_missing_topics_and_removes_missing_nodes())


def _passthrough(topic_snapshot) -> SeedTopic:
    """Rebuild a SeedTopic identical to a stored topic, so re-importing it
    with prune=True is a no-op for that topic. `prune` prunes every topic
    missing from `seed.topics` *registry-wide* (topics aren't scoped to a
    taxonomy), so a prune test against a shared database must pass through
    every topic it doesn't intend to touch or it will deactivate them too.
    """
    exemplars = tuple(
        s.value for s in topic_snapshot.signals if s.kind == "exemplar" and s.active
    )
    keywords = tuple(
        SeedKeyword(s.value, s.weight)
        for s in topic_snapshot.signals
        if s.kind == "keyword" and s.active
    )
    return SeedTopic(
        slug=topic_snapshot.slug,
        name=topic_snapshot.display_name,
        description=topic_snapshot.description,
        threshold=topic_snapshot.threshold,
        exemplars=exemplars,
        keywords=keywords,
        active=topic_snapshot.active,
    )


async def _test_prune_deactivates_missing_topics_and_removes_missing_nodes() -> None:
    storage = PostgresRawStorage(DATABASE_URL)
    engine = create_async_engine(DATABASE_URL)
    try:
        await storage.check_ready()
        await _cleanup(engine)
        topic = _topic()
        await storage.import_taxonomy(_seed(topic=topic, leaf_slug="leaf-a"))

        topics_before, _ = await storage.export_taxonomy()
        other_active_topics = tuple(
            _passthrough(t) for t in topics_before if t.slug != topic.slug and t.active
        )

        empty_seed = TaxonomySeed(
            version=1,
            topics=other_active_topics,
            taxonomies=(
                SeedTaxonomy(
                    slug=TAXONOMY_SLUG,
                    name="Integration Tree",
                    nodes=(
                        SeedTaxonomyNode(slug="root", name="Root", topics=(), children=()),
                    ),
                ),
            ),
        )
        pruned = await storage.import_taxonomy(empty_seed, prune=True)

        assert pruned.pruned_topics == [topic.slug]
        assert "leaf-a" in pruned.pruned_nodes

        topics_after, taxonomies_after = await storage.export_taxonomy()
        topic_after = next(t for t in topics_after if t.slug == topic.slug)
        assert topic_after.active is False

        tree = next(t for t in taxonomies_after if t.slug == TAXONOMY_SLUG)
        root = next(n for n in tree.nodes if n.slug == "root")
        assert root.children == []
    finally:
        await _cleanup(engine)
        await engine.dispose()
        await storage.close()


def test_prototype_rebuild_round_trip_persists_and_updates_prototype() -> None:
    if not DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL is not set")
    asyncio.run(_test_prototype_rebuild_round_trip_persists_and_updates_prototype())


async def _test_prototype_rebuild_round_trip_persists_and_updates_prototype() -> None:
    from models.storage_models import TopicPrototypeRecord

    storage = PostgresRawStorage(DATABASE_URL)
    engine = create_async_engine(DATABASE_URL)
    try:
        await storage.check_ready()
        await _cleanup(engine)
        topic = _topic()
        await storage.import_taxonomy(_seed(topic=topic))

        exemplar_sets = await storage.topics_for_prototype_rebuild(slug=topic.slug)
        assert len(exemplar_sets) == 1
        exemplar_set = exemplar_sets[0]
        assert exemplar_set.current_definition_hash is None
        assert sorted(exemplar_set.exemplars) == sorted(topic.exemplars)

        vector = [1.0] + [0.0] * 383
        await storage.upsert_topic_prototypes(
            [
                TopicPrototypeRecord(
                    topic_id=exemplar_set.topic_id,
                    model="test-model",
                    dim=384,
                    definition_hash=b"hash-1",
                    embedding=vector,
                )
            ]
        )

        rebuilt_sets = await storage.topics_for_prototype_rebuild(slug=topic.slug)
        assert rebuilt_sets[0].current_definition_hash == b"hash-1"
        assert rebuilt_sets[0].current_model == "test-model"
        assert rebuilt_sets[0].current_dim == 384

        # Upserting again for the same topic must update in place, not duplicate.
        await storage.upsert_topic_prototypes(
            [
                TopicPrototypeRecord(
                    topic_id=exemplar_set.topic_id,
                    model="test-model",
                    dim=384,
                    definition_hash=b"hash-2",
                    embedding=vector,
                )
            ]
        )
        async with engine.connect() as connection:
            proto_rows = (
                await connection.execute(
                    select(topic_prototypes).where(
                        topic_prototypes.c.topic_id == exemplar_set.topic_id
                    )
                )
            ).all()
            assert len(proto_rows) == 1
            assert proto_rows[0].definition_hash == b"hash-2"
    finally:
        await _cleanup(engine)
        await engine.dispose()
        await storage.close()
