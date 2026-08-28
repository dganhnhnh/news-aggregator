import pytest

from models.storage_models import FlatTaxonomyNode, TopicSignalSnapshot, TopicSnapshot
from services.taxonomy import (
    SeedKeyword,
    SeedTaxonomy,
    SeedTaxonomyNode,
    SeedTopic,
    TaxonomySeed,
    TaxonomyValidationError,
    build_node_tree,
    compile_prototype,
    definition_hash,
    l2_normalize,
    next_classifier_revision,
    parse_seed,
    scoring_changed,
    to_seed_dict,
    validate_seed,
)


def _seed_topic(
    slug: str = "artificial-intelligence",
    *,
    name: str = "Artificial Intelligence",
    threshold: float = 0.55,
    exemplars: tuple[str, ...] = ("A model learns from data.", "Neural nets train fast.", "AI research advances."),
    keywords: tuple[SeedKeyword, ...] = (SeedKeyword("machine learning"),),
    active: bool = True,
) -> SeedTopic:
    return SeedTopic(
        slug=slug,
        name=name,
        description=None,
        threshold=threshold,
        exemplars=exemplars,
        keywords=keywords,
        active=active,
    )


def _existing_snapshot(topic: SeedTopic, *, revision: int = 3) -> TopicSnapshot:
    return TopicSnapshot(
        id=1,
        slug=topic.slug,
        display_name=topic.name,
        description=topic.description,
        threshold=topic.threshold,
        classifier_revision=revision,
        active=topic.active,
        signals=[
            TopicSignalSnapshot(kind="exemplar", value=e, weight=1.0, active=True)
            for e in topic.exemplars
        ]
        + [
            TopicSignalSnapshot(kind="keyword", value=k.value, weight=k.weight, active=True)
            for k in topic.keywords
        ],
    )


def test_parse_seed_reads_topics_and_taxonomies() -> None:
    raw = {
        "version": 1,
        "topics": [
            {
                "slug": "ai",
                "name": "AI",
                "threshold": 0.6,
                "exemplars": ["one", "two"],
                "keywords": [{"value": "llm", "weight": 2.0}],
            }
        ],
        "taxonomies": [
            {
                "slug": "default",
                "name": "Default",
                "nodes": [
                    {
                        "slug": "tech",
                        "name": "Technology",
                        "topics": ["ai"],
                        "children": [{"slug": "hw", "name": "Hardware"}],
                    }
                ],
            }
        ],
    }

    seed = parse_seed(raw)

    assert seed.version == 1
    assert seed.topics[0].slug == "ai"
    assert seed.topics[0].threshold == 0.6
    assert seed.topics[0].keywords[0] == SeedKeyword("llm", 2.0)
    assert seed.taxonomies[0].nodes[0].topics == ("ai",)
    assert seed.taxonomies[0].nodes[0].children[0].slug == "hw"


def test_validate_seed_requires_minimum_exemplars_for_active_topics() -> None:
    topic = _seed_topic(exemplars=("only one exemplar",))
    seed = TaxonomySeed(version=1, topics=(topic,), taxonomies=())

    with pytest.raises(TaxonomyValidationError, match="at least 3 exemplars"):
        validate_seed(seed)


def test_validate_seed_allows_inactive_topic_with_few_exemplars() -> None:
    topic = _seed_topic(exemplars=("only one",), active=False)
    seed = TaxonomySeed(version=1, topics=(topic,), taxonomies=())

    validate_seed(seed)


def test_validate_seed_rejects_duplicate_exemplars_and_keywords() -> None:
    topic = _seed_topic(
        exemplars=("Same text.", "same text.", "A third one here."),
        keywords=(SeedKeyword("ai"), SeedKeyword("AI")),
    )
    seed = TaxonomySeed(version=1, topics=(topic,), taxonomies=())

    with pytest.raises(TaxonomyValidationError) as exc_info:
        validate_seed(seed)
    assert "duplicate exemplar" in str(exc_info.value)
    assert "duplicate keyword" in str(exc_info.value)


def test_validate_seed_rejects_unknown_topic_reference_in_node() -> None:
    taxonomy = SeedTaxonomy(
        slug="default",
        name="Default",
        nodes=(SeedTaxonomyNode(slug="tech", name="Tech", topics=("missing-slug",), children=()),),
    )
    seed = TaxonomySeed(version=1, topics=(), taxonomies=(taxonomy,))

    with pytest.raises(TaxonomyValidationError, match="unknown topic"):
        validate_seed(seed)


def test_validate_seed_rejects_out_of_range_threshold() -> None:
    topic = _seed_topic(threshold=1.5)
    seed = TaxonomySeed(version=1, topics=(topic,), taxonomies=())

    with pytest.raises(TaxonomyValidationError, match="threshold"):
        validate_seed(seed)


def test_definition_hash_is_order_and_case_insensitive() -> None:
    a = definition_hash(["Hello World", "Second one"])
    b = definition_hash(["second ONE", "hello world"])
    c = definition_hash(["Hello World", "A different one"])

    assert a == b
    assert a != c


def test_l2_normalize_produces_unit_vector() -> None:
    result = l2_normalize([3.0, 4.0])

    assert result == pytest.approx([0.6, 0.8])
    with pytest.raises(ValueError, match="zero vector"):
        l2_normalize([0.0, 0.0])


def test_compile_prototype_matches_the_documented_formula() -> None:
    embeddings = [[1.0, 0.0], [0.0, 1.0], [1.0, 1.0]]

    result = compile_prototype(embeddings)

    normalized = [l2_normalize(v) for v in embeddings]
    mean = [sum(v[i] for v in normalized) / 3 for i in range(2)]
    expected = l2_normalize(mean)
    assert result == pytest.approx(expected)


def test_compile_prototype_requires_minimum_exemplars() -> None:
    with pytest.raises(ValueError, match="at least 3"):
        compile_prototype([[1.0, 0.0], [0.0, 1.0]])


def test_next_classifier_revision_is_1_for_a_new_topic() -> None:
    topic = _seed_topic()

    assert next_classifier_revision(None, topic) == 1


def test_editing_display_name_or_description_never_bumps_revision() -> None:
    """Done criterion: moving/renaming presentation-only fields must not
    bump classifier_revision -- only scoring inputs do."""
    topic = _seed_topic()
    existing = _existing_snapshot(topic, revision=3)
    renamed = SeedTopic(
        slug=topic.slug,
        name="A totally different display name",
        description="new description",
        threshold=topic.threshold,
        exemplars=topic.exemplars,
        keywords=topic.keywords,
        active=topic.active,
    )

    assert scoring_changed(existing, renamed) is False
    assert next_classifier_revision(existing, renamed) == 3


def test_editing_an_exemplar_bumps_revision_exactly_once() -> None:
    topic = _seed_topic()
    existing = _existing_snapshot(topic, revision=3)
    edited = SeedTopic(
        slug=topic.slug,
        name=topic.name,
        description=topic.description,
        threshold=topic.threshold,
        exemplars=("A model learns from data.", "Neural nets train fast.", "A brand new exemplar."),
        keywords=topic.keywords,
        active=topic.active,
    )

    assert next_classifier_revision(existing, edited) == 4
    # Re-importing the already-edited seed a second time must not bump again.
    existing_after = _existing_snapshot(edited, revision=4)
    assert next_classifier_revision(existing_after, edited) == 4


def test_editing_threshold_or_keyword_weight_bumps_revision() -> None:
    topic = _seed_topic()
    existing = _existing_snapshot(topic, revision=1)

    threshold_changed = SeedTopic(
        slug=topic.slug,
        name=topic.name,
        description=topic.description,
        threshold=0.7,
        exemplars=topic.exemplars,
        keywords=topic.keywords,
        active=topic.active,
    )
    assert next_classifier_revision(existing, threshold_changed) == 2

    keyword_weight_changed = SeedTopic(
        slug=topic.slug,
        name=topic.name,
        description=topic.description,
        threshold=topic.threshold,
        exemplars=topic.exemplars,
        keywords=(SeedKeyword("machine learning", weight=2.0),),
        active=topic.active,
    )
    assert next_classifier_revision(existing, keyword_weight_changed) == 2


def test_build_node_tree_nests_children_and_sorts_siblings() -> None:
    flat = [
        FlatTaxonomyNode(1, None, "b-root", "B Root", 1, ()),
        FlatTaxonomyNode(2, None, "a-root", "A Root", 0, ()),
        FlatTaxonomyNode(3, 2, "child", "Child", 0, ("ai",)),
    ]

    tree = build_node_tree(flat)

    assert [node.slug for node in tree] == ["a-root", "b-root"]
    assert tree[0].children[0].slug == "child"
    assert tree[0].children[0].topic_slugs == ["ai"]
    assert tree[1].children == []


def test_to_seed_dict_round_trips_through_parse_seed() -> None:
    from models.storage_models import TaxonomyNodeSnapshot, TaxonomySnapshot

    topic_snapshot = TopicSnapshot(
        id=1,
        slug="ai",
        display_name="AI",
        description=None,
        threshold=0.6,
        classifier_revision=1,
        active=True,
        signals=[
            TopicSignalSnapshot(kind="exemplar", value="one", weight=1.0, active=True),
            TopicSignalSnapshot(kind="keyword", value="llm", weight=2.0, active=True),
            TopicSignalSnapshot(kind="exemplar", value="stale", weight=1.0, active=False),
        ],
    )
    taxonomy_snapshot = TaxonomySnapshot(
        slug="default",
        display_name="Default",
        revision=1,
        active=True,
        nodes=[
            TaxonomyNodeSnapshot(
                slug="tech", display_name="Technology", sort_order=0, topic_slugs=["ai"], children=[]
            )
        ],
    )

    raw = to_seed_dict([topic_snapshot], [taxonomy_snapshot])
    seed = parse_seed(raw)

    assert seed.topics[0].exemplars == ("one",)
    assert seed.topics[0].keywords == (SeedKeyword("llm", 2.0),)
    assert seed.taxonomies[0].nodes[0].topics == ("ai",)
