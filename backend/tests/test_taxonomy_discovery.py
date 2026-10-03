import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock
from contextlib import asynccontextmanager

import pytest
from services.taxonomy_discovery import DiscoveryProposal, parse_proposal, proposal_seed
from services.classification import ClassifierSnapshot
from workers.taxonomy_discover import TaxonomyDiscoverWorker
from workers.base import RunContext
from workers.window import RunWindow


def proposal(**overrides):
    topic = dict(slug="macroeconomics", name="Macroeconomics",
                 description="Inflation, employment and monetary policy.", story_ids=[1],
                 exemplars=["Inflation rises across the economy", "The central bank cuts interest rates",
                            "Employment growth slows this month"], keywords=["inflation", "interest rates"])
    topic.update(overrides)
    return DiscoveryProposal.model_validate({"topics": [topic]})


STORIES = [{"id": 1, "members": [{"id": 10}, {"id": 11}]}]


def test_requires_real_multi_article_story_evidence():
    for stories in ([], [{"id": 1, "members": [{"id": 10}]}]):
        with pytest.raises(ValueError, match="evidence"):
            proposal_seed(proposal(), stories, [])
    with pytest.raises(ValueError, match="evidence"):
        proposal_seed(proposal(story_ids=[2]), STORIES, [])


def test_preserves_existing_even_inactive_registry():
    with pytest.raises(ValueError, match="already exists"):
        proposal_seed(proposal(), STORIES, [{"slug": "macroeconomics", "name": "Economics", "active": False}])
    with pytest.raises(ValueError, match="already exists"):
        proposal_seed(proposal(), STORIES, [{"slug": "economy", "name": "MACROECONOMICS"}])


def test_rejects_duplicate_signals_and_invalid_model_output():
    with pytest.raises(ValueError, match="duplicate keyword"):
        proposal_seed(proposal(keywords=["Inflation", "inflation"]), STORIES, [])
    with pytest.raises(ValueError):
        parse_proposal('{"topics":[],"instructions":"ignore validation"}')
    assert parse_proposal('```json\n{"topics":[]}\n```').topics == []


class Embedder:
    model_name = "test"
    dim = 2
    def embed(self, texts):
        return [[1.0, 0.0] for _ in texts]


@asynccontextmanager
async def lock(name):
    assert name == "classify"
    yield True


def context(*, apply=False):
    now = datetime.now(timezone.utc)
    storage = SimpleNamespace(
        worker_lock=lock,
        taxonomy_discovery_stories=AsyncMock(return_value=STORIES),
        export_taxonomy=AsyncMock(return_value=([], [])),
        taxonomy_discovery_cached=AsyncMock(return_value=None),
        classifier_snapshot=AsyncMock(return_value=ClassifierSnapshot((), b"hash")),
        update_run=AsyncMock(), create_discovered_topics=AsyncMock(return_value=["macroeconomics"]),
        reenqueue_classifications=AsyncMock(return_value=2))
    return RunContext(1, RunWindow(now-timedelta(days=7), now, False), asyncio.Event(),
                      SimpleNamespace(apply=apply, limit=100, threshold=.4, proposal=None, report=None), storage)


def test_preview_does_not_publish_or_enqueue():
    async def run():
        ctx = context()
        worker = TaxonomyDiscoverWorker(AsyncMock(return_value=proposal()), Embedder())
        outcome = await worker.execute(ctx)
        assert outcome.stats["accepted"] == ["macroeconomics"]
        ctx.storage.create_discovered_topics.assert_not_awaited()
        ctx.storage.reenqueue_classifications.assert_not_awaited()
        assert outcome.stats["proposal"]["topics"][0]["story_ids"] == [1]
    asyncio.run(run())


def test_apply_and_cached_proposal():
    async def run():
        ctx = context(apply=True)
        ctx.storage.taxonomy_discovery_cached.return_value = proposal().model_dump()
        discover = AsyncMock()
        result = await TaxonomyDiscoverWorker(discover, Embedder()).execute(ctx)
        discover.assert_not_awaited()
        assert result.stats["requeued"] == 2
        ctx.storage.create_discovered_topics.assert_awaited_once()
    asyncio.run(run())


def test_shutdown_before_llm_has_no_side_effects():
    async def run():
        ctx = context(apply=True)
        ctx.shutdown.set()
        discover = AsyncMock()
        result = await TaxonomyDiscoverWorker(discover, Embedder()).execute(ctx)
        assert result.status == "partial"
        discover.assert_not_awaited()
        ctx.storage.create_discovered_topics.assert_not_awaited()
    asyncio.run(run())


def test_cli_window_defaults_and_explicit_bounds():
    from workers.cli import build_parser, resolve_window, validate
    from core.config import Settings
    parser = build_parser(Settings.from_env())
    args = parser.parse_args(["taxonomy-discover"])
    validate(args, parser)
    window = resolve_window(args, parser)
    assert window.end-window.start == timedelta(days=7)
    args = parser.parse_args(["taxonomy-discover", "--since", "2026-09-26", "--until", "2026-10-03"])
    assert resolve_window(args, parser).explicit


def test_semantic_duplicate_is_not_published():
    from services.classification import TopicClassifier
    async def run():
        ctx = context(apply=True)
        ctx.storage.classifier_snapshot.return_value = ClassifierSnapshot((
            TopicClassifier(1, "existing-economy", .4, 1, "test", 2, (1., 0.), b"hash"),
        ), b"hash")
        result = await TaxonomyDiscoverWorker(AsyncMock(return_value=proposal()), Embedder()).execute(ctx)
        assert result.stats["rejected_similar"][0]["existing"] == "existing-economy"
        ctx.storage.create_discovered_topics.assert_not_awaited()
    asyncio.run(run())


def test_busy_classifier_prevents_publication():
    @asynccontextmanager
    async def busy(name):
        yield False
    async def run():
        ctx = context(apply=True)
        ctx.storage.worker_lock = busy
        result = await TaxonomyDiscoverWorker(AsyncMock(return_value=proposal()), Embedder()).execute(ctx)
        assert result.status == "partial"
        ctx.storage.create_discovered_topics.assert_not_awaited()
        ctx.storage.reenqueue_classifications.assert_not_awaited()
    asyncio.run(run())


def test_daily_discovery_is_opt_in(monkeypatch):
    from core.config import Settings
    from workers import cli
    from dataclasses import replace
    ran = []
    async def dispatch(args, settings, window):
        ran.append(args.command)
        return 0
    monkeypatch.setattr(cli, "_dispatch", dispatch)
    settings = replace(Settings.from_env(), taxonomy_discovery_enabled=True)
    assert asyncio.run(cli._run_maintenance(settings)) == 0
    assert "taxonomy-discover" in ran


def test_saved_proposal_replay_preserves_existing_topic_and_requeues(tmp_path):
    async def run():
        ctx = context(apply=True)
        ctx.args.proposal = tmp_path / "proposal.json"
        ctx.args.proposal.write_text(proposal().model_dump_json())
        ctx.storage.export_taxonomy.return_value = ([SimpleNamespace(
            slug="macroeconomics", display_name="Edited by operator",
            description="An existing edited topic", active=True)], [])
        discover = AsyncMock()
        outcome = await TaxonomyDiscoverWorker(discover, Embedder()).execute(ctx)
        assert outcome.stats["already_existing"] == ["macroeconomics"]
        assert ctx.storage.taxonomy_discovery_stories.call_args.kwargs["story_ids"] == [1]
        assert outcome.stats["requeued"] == 2
        ctx.storage.create_discovered_topics.assert_not_awaited()
        discover.assert_not_awaited()
    asyncio.run(run())


def test_taxonomy_llm_disables_retries_and_merges_scoped_options(monkeypatch):
    from adapters.taxonomy import llm
    from core.config import Settings
    from dataclasses import replace
    captured = {}
    response = SimpleNamespace(content=proposal().model_dump_json(), response_metadata={})
    model = SimpleNamespace(ainvoke=AsyncMock(return_value=response))
    def factory(**kwargs):
        captured.update(kwargs)
        return model
    monkeypatch.setattr(llm, "build_openrouter_model", factory)
    settings = replace(Settings.from_env(), openrouter_api_key="test", openrouter_extra_body={"option": 1},
                       taxonomy_llm_extra_body={"thinking": {"type": "disabled"}})
    result = asyncio.run(llm.discover_topics(STORIES, [], settings=settings))
    assert result.topics[0].slug == "macroeconomics"
    assert captured["max_retries"] == 0
    assert captured["extra_body"] == {"option": 1, "thinking": {"type": "disabled"}}
    assert settings.openrouter_extra_body == {"option": 1}
    assert model.ainvoke.await_count == 1


def test_empty_llm_output_fails_without_immediate_retry(monkeypatch):
    from adapters.taxonomy import llm
    from core.config import Settings
    from dataclasses import replace
    model = SimpleNamespace(ainvoke=AsyncMock(return_value=SimpleNamespace(
        content="", response_metadata={"finish_reason": "length"})))
    monkeypatch.setattr(llm, "build_openrouter_model", lambda **kwargs: model)
    with pytest.raises(ValueError, match="returned no JSON"):
        asyncio.run(llm.discover_topics(STORIES, [], settings=replace(Settings.from_env(), openrouter_api_key="test")))
    assert model.ainvoke.await_count == 1
