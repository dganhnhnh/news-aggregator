import math

import pytest

from services.classification import (
    KEYWORD_WEIGHT,
    PROTOTYPE_WEIGHT,
    ClassifierMismatch,
    MemberTopicScores,
    TopicClassifier,
    TopicKeyword,
    aggregate_cluster_topics,
    classifier_set_hash,
    classify_article,
    keyword_score,
    normalize_tokens,
)


MODEL = "sentence-transformers/all-MiniLM-L6-v2"


def _unit(*values: float) -> tuple[float, ...]:
    norm = math.sqrt(sum(value * value for value in values))
    return tuple(value / norm for value in values)


def _classifier(
    topic_id: int,
    slug: str,
    prototype: tuple[float, ...],
    *,
    threshold: float = 0.55,
    keywords: tuple[TopicKeyword, ...] = (),
    model: str = MODEL,
    revision: int = 1,
) -> TopicClassifier:
    return TopicClassifier(
        topic_id=topic_id,
        slug=slug,
        threshold=threshold,
        classifier_revision=revision,
        model=model,
        dim=len(prototype),
        prototype=prototype,
        definition_hash=slug.encode("utf-8"),
        keywords=keywords,
    )


# --- keyword matching ---------------------------------------------------


def test_normalize_tokens_folds_case_and_width():
    assert normalize_tokens("Ｇｅｍｉｎｉ AI") == ["gemini", "ai"]


def test_keyword_matches_on_token_boundaries_not_substrings():
    keywords = (TopicKeyword("ai"),)

    assert keyword_score("he said nothing about chatbots", keywords) == 0.0
    assert keyword_score("the AI model shipped", keywords) > 0.0


def test_multi_word_keyword_requires_adjacent_tokens():
    keywords = (TopicKeyword("machine learning"),)

    assert keyword_score("machine learning models", keywords) > 0.0
    assert keyword_score("a learning machine", keywords) == 0.0


def test_repeating_a_keyword_does_not_increase_its_contribution():
    keywords = (TopicKeyword("kubernetes"),)
    once = keyword_score("kubernetes rollout", keywords)
    many = keyword_score("kubernetes kubernetes kubernetes", keywords)

    assert once == many


def test_keyword_score_saturates_at_one():
    keywords = tuple(TopicKeyword(word, weight=2.0) for word in ("gpu", "chip"))

    assert keyword_score("gpu and chip supply", keywords) == 1.0


def test_keyword_score_is_zero_without_keywords_or_text():
    assert keyword_score("anything", ()) == 0.0
    assert keyword_score("   ", (TopicKeyword("ai"),)) == 0.0


# --- article scoring ----------------------------------------------------


def test_multi_label_article_is_assigned_every_topic_over_threshold():
    embedding = _unit(1.0, 1.0, 0.0)
    classifiers = (
        _classifier(1, "ai", _unit(1.0, 0.0, 0.0)),
        _classifier(2, "chips", _unit(0.0, 1.0, 0.0)),
        _classifier(3, "cloud", _unit(0.0, 0.0, 1.0)),
    )

    result = classify_article(
        embedding=embedding,
        model=MODEL,
        dim=3,
        text="a story about models and silicon",
        classifiers=classifiers,
    )

    assert {score.slug for score in result.assigned} == {"ai", "chips"}
    assert result.assigned_count == 2
    # Near-misses are kept, not dropped, so evaluation can see them.
    assert {score.slug for score in result.scores} == {"ai", "chips", "cloud"}
    assert result.max_score == pytest.approx(result.scores[0].score)


def test_article_matching_nothing_is_uncategorized():
    classifiers = (_classifier(1, "ai", _unit(1.0, 0.0)),)

    result = classify_article(
        embedding=_unit(0.0, 1.0),
        model=MODEL,
        dim=2,
        text="an unrelated story",
        classifiers=classifiers,
    )

    assert result.assigned_count == 0
    assert result.assigned == ()
    assert result.max_score == pytest.approx(0.0)


def test_score_combines_prototype_and_keyword_with_the_declared_weights():
    classifiers = (
        _classifier(
            1, "ai", _unit(1.0, 0.0), keywords=(TopicKeyword("ai", weight=2.0),)
        ),
    )

    result = classify_article(
        embedding=_unit(1.0, 0.0),
        model=MODEL,
        dim=2,
        text="AI everywhere",
        classifiers=classifiers,
    )

    score = result.scores[0]
    assert score.prototype_score == pytest.approx(1.0)
    assert score.keyword_score == pytest.approx(1.0)
    assert score.score == pytest.approx(PROTOTYPE_WEIGHT + KEYWORD_WEIGHT)


def test_keywords_alone_cannot_carry_a_topic_over_the_threshold():
    classifiers = (
        _classifier(
            1,
            "ai",
            _unit(1.0, 0.0),
            keywords=(TopicKeyword("ai", weight=5.0),),
        ),
    )

    result = classify_article(
        embedding=_unit(0.0, 1.0),  # orthogonal: no prototype evidence at all
        model=MODEL,
        dim=2,
        text="ai ai ai",
        classifiers=classifiers,
    )

    assert result.scores[0].keyword_score == pytest.approx(1.0)
    assert result.scores[0].score == pytest.approx(KEYWORD_WEIGHT)
    assert result.assigned_count == 0


def test_negative_cosine_is_clamped_to_zero():
    classifiers = (_classifier(1, "ai", _unit(1.0, 0.0)),)

    result = classify_article(
        embedding=_unit(-1.0, 0.0),
        model=MODEL,
        dim=2,
        text="",
        classifiers=classifiers,
    )

    assert result.scores[0].prototype_score == 0.0


def test_threshold_is_inclusive_at_the_boundary():
    # A pure prototype match of exactly 0.6875 scores 0.8 * 0.6875 = 0.55.
    classifiers = (_classifier(1, "ai", _unit(1.0, 0.0), threshold=0.55),)
    embedding = (0.6875, math.sqrt(1 - 0.6875**2))

    result = classify_article(
        embedding=embedding, model=MODEL, dim=2, text="", classifiers=classifiers
    )

    assert result.scores[0].score == pytest.approx(0.55)
    assert result.scores[0].assigned


def test_model_mismatch_raises_instead_of_scoring():
    classifiers = (_classifier(1, "ai", _unit(1.0, 0.0), model="other-model"),)

    with pytest.raises(ClassifierMismatch, match="taxonomy-rebuild"):
        classify_article(
            embedding=_unit(1.0, 0.0),
            model=MODEL,
            dim=2,
            text="",
            classifiers=classifiers,
        )


def test_dimension_mismatch_raises_instead_of_scoring():
    classifiers = (_classifier(1, "ai", _unit(1.0, 0.0, 0.0)),)

    with pytest.raises(ClassifierMismatch, match="dimension"):
        classify_article(
            embedding=_unit(1.0, 0.0),
            model=MODEL,
            dim=2,
            text="",
            classifiers=classifiers,
        )


# --- cluster aggregation ------------------------------------------------


def test_one_publisher_cannot_outvote_the_rest_by_posting_more():
    """Six articles from one outlet weigh the same as that outlet's best one."""
    thresholds = {1: 0.55}
    stuffed = [
        MemberTopicScores(article_id=index, source_id=10, scores={1: 0.9})
        for index in range(6)
    ]
    others = [
        MemberTopicScores(article_id=100 + index, source_id=20 + index, scores={1: 0.1})
        for index in range(2)
    ]

    result = aggregate_cluster_topics(stuffed + others, thresholds)

    # (0.9 + 0.1 + 0.1) / 3 sources, not (6*0.9 + 0.1 + 0.1) / 8 articles.
    assert result[0].score == pytest.approx((0.9 + 0.1 + 0.1) / 3)
    assert not result[0].assigned


def test_source_vote_is_the_max_not_the_mean_within_a_source():
    thresholds = {1: 0.55}
    members = [
        MemberTopicScores(article_id=1, source_id=10, scores={1: 0.2}),
        MemberTopicScores(article_id=2, source_id=10, scores={1: 0.8}),
    ]

    result = aggregate_cluster_topics(members, thresholds)

    assert result[0].score == pytest.approx(0.8)
    assert result[0].assigned


def test_member_without_a_row_for_a_topic_votes_zero():
    thresholds = {1: 0.55}
    members = [
        MemberTopicScores(article_id=1, source_id=10, scores={1: 0.9}),
        MemberTopicScores(article_id=2, source_id=20, scores={}),
    ]

    result = aggregate_cluster_topics(members, thresholds)

    assert result[0].score == pytest.approx(0.45)
    assert not result[0].assigned


def test_sourceless_members_each_count_as_their_own_voter():
    thresholds = {1: 0.55}
    members = [
        MemberTopicScores(article_id=1, source_id=None, scores={1: 0.9}),
        MemberTopicScores(article_id=2, source_id=None, scores={1: 0.3}),
    ]

    result = aggregate_cluster_topics(members, thresholds)

    assert result[0].score == pytest.approx(0.6)


def test_aggregation_is_independent_of_member_order():
    thresholds = {1: 0.55, 2: 0.55}
    members = [
        MemberTopicScores(article_id=1, source_id=10, scores={1: 0.7, 2: 0.2}),
        MemberTopicScores(article_id=2, source_id=20, scores={1: 0.5}),
        MemberTopicScores(article_id=3, source_id=10, scores={2: 0.9}),
    ]

    assert aggregate_cluster_topics(members, thresholds) == aggregate_cluster_topics(
        list(reversed(members)), thresholds
    )


def test_topic_without_a_threshold_is_scored_but_never_assigned():
    members = [MemberTopicScores(article_id=1, source_id=10, scores={99: 1.0})]

    result = aggregate_cluster_topics(members, {})

    assert result[0].score == pytest.approx(1.0)
    assert not result[0].assigned


def test_empty_cluster_has_no_labels():
    assert aggregate_cluster_topics([], {1: 0.55}) == []


# --- classifier set hash ------------------------------------------------


def test_set_hash_is_stable_and_order_independent():
    first = _classifier(1, "ai", _unit(1.0, 0.0))
    second = _classifier(2, "chips", _unit(0.0, 1.0))

    assert classifier_set_hash([first, second]) == classifier_set_hash([second, first])


def test_set_hash_changes_when_any_input_to_a_score_changes():
    base = [_classifier(1, "ai", _unit(1.0, 0.0))]
    baseline = classifier_set_hash(base)

    retuned = [_classifier(1, "ai", _unit(1.0, 0.0), threshold=0.6)]
    recompiled = [_classifier(1, "ai", _unit(1.0, 0.0), revision=2)]
    remodelled = [_classifier(1, "ai", _unit(1.0, 0.0), model="other")]
    extended = base + [_classifier(2, "chips", _unit(0.0, 1.0))]

    assert baseline not in {
        classifier_set_hash(retuned),
        classifier_set_hash(recompiled),
        classifier_set_hash(remodelled),
        classifier_set_hash(extended),
    }
