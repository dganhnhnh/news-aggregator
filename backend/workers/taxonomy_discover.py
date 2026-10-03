"""Daily story-grounded discovery; LLM runs once per bounded, changed snapshot."""
from __future__ import annotations

import asyncio
import hashlib
import json

from core.config import Settings
from services.clustering import cosine_similarity
from services.taxonomy import TaxonomySeed, compile_prototype
from services.taxonomy_discovery import (
    POLICY_VERSION, DUPLICATE_SIMILARITY, DiscoveryProposal, parse_proposal, proposal_seed,
)
from workers.base import RunOutcome, Worker


class TaxonomyDiscoverWorker(Worker):
    name = "taxonomy-discover"

    def __init__(self, discover=None, embedder=None):
        self._discover = discover
        self._embedder = embedder

    def run_params(self, args):
        return {"apply": args.apply, "limit": args.limit,
                "threshold": args.threshold, "policy_version": POLICY_VERSION,
                "model": Settings.from_env().news_agent_model}

    async def execute(self, ctx):
        from adapters.taxonomy.llm import discover_topics
        settings = Settings.from_env()
        stats = {"created": [], "rejected_similar": [], "rejected_capacity": [], "requeued": 0,
                 "threshold": ctx.args.threshold,
                 "window": {"since": ctx.window.start.isoformat(), "until": ctx.window.end.isoformat()}}
        saved_proposal = parse_proposal(ctx.args.proposal.read_text()) if ctx.args.proposal else None
        evidence_ids = sorted({sid for t in saved_proposal.topics for sid in t.story_ids}) if saved_proposal else None
        if evidence_ids is not None and len(evidence_ids) > 100:
            raise ValueError("Saved proposal exceeds the 100-story evidence budget")
        # Saved evidence need not remain in today's top-N selection. Closed
        # historical clusters still qualify; merged/singleton ones do not.
        stories = await ctx.storage.taxonomy_discovery_stories(
            since=ctx.window.start, until=ctx.window.end,
            limit=100 if saved_proposal else ctx.args.limit, story_ids=evidence_ids)
        stats["stories"] = len(stories)
        if not stories and saved_proposal is None:
            return RunOutcome("succeeded", stats)
        topics, _ = await ctx.storage.export_taxonomy()
        registry = [{"slug": t.slug, "name": t.display_name,
                     "description": (t.description or "")[:600], "active": t.active} for t in topics]
        # Keep prompt size and registry growth bounded; human consolidation can
        # raise this limit later, instead of accumulating a new tail every day.
        if len(registry) >= 100:
            return RunOutcome("partial", stats, "Registry reached 100 topics; consolidate before discovery")
        fingerprint = {"stories": stories, "registry": sorted(registry, key=lambda t: t["slug"]),
                       "policy": POLICY_VERSION, "model": settings.news_agent_model,
                       "llm_options": settings.taxonomy_llm_extra_body,
                       "threshold": ctx.args.threshold}
        stats["input_hash"] = hashlib.sha256(json.dumps(fingerprint, sort_keys=True).encode()).hexdigest()
        await ctx.checkpoint(stats)
        if ctx.shutdown.is_set():
            return RunOutcome("partial", stats, "Stopped before discovery")
        if ctx.args.proposal:
            proposal = saved_proposal
        else:
            cached = await ctx.storage.taxonomy_discovery_cached(stats["input_hash"])
            stats["cache_hit"] = cached is not None
            proposal = DiscoveryProposal.model_validate(cached) if cached is not None else await (
                self._discover or discover_topics)(stories, registry)
        # Replaying a saved proposal after publication is safe and also recovers
        # a run interrupted before the stale requeue. Never overwrite old slugs.
        existing_slugs = {t["slug"] for t in registry}
        stats["already_existing"] = [t.slug for t in proposal.topics if t.slug in existing_slugs]
        proposal = proposal.model_copy(update={
            "topics": [t for t in proposal.topics if t.slug not in existing_slugs]
        })
        seed = proposal_seed(proposal, stories, registry, threshold=ctx.args.threshold)
        stats["proposal"] = proposal.model_dump()
        await ctx.checkpoint(stats)
        if ctx.shutdown.is_set():
            return RunOutcome("partial", stats, "Stopped after discovery")
        vectors = {}
        accepted = []
        if seed.topics:
            if self._embedder is None:
                from adapters.embedding.fastembed_embedder import FastEmbedEmbedder
                self._embedder = await asyncio.to_thread(FastEmbedEmbedder)
            snapshot = await ctx.storage.classifier_snapshot()
            for classifier in snapshot.classifiers:
                if classifier.model != self._embedder.model_name or classifier.dim != self._embedder.dim:
                    raise ValueError("Rebuild existing taxonomy prototypes before discovery: embedding model mismatch")
            comparisons = [(c.slug, c.prototype) for c in snapshot.classifiers]
            for topic in seed.topics:
                if ctx.shutdown.is_set():
                    return RunOutcome("partial", stats, "Stopped compiling proposals")
                embedded = await asyncio.to_thread(self._embedder.embed, topic.exemplars)
                vector = compile_prototype(embedded)
                closest = max(((slug, cosine_similarity(vector, other)) for slug, other in comparisons),
                              key=lambda item: item[1], default=(None, 0.0))
                if closest[1] >= DUPLICATE_SIMILARITY:
                    stats["rejected_similar"].append({"slug": topic.slug, "existing": closest[0], "cosine": closest[1]})
                elif len(registry) + len(accepted) >= 100:
                    stats["rejected_capacity"].append(topic.slug)
                else:
                    accepted.append(topic)
                    vectors[topic.slug] = vector
                    comparisons.append((topic.slug, vector))
                await ctx.checkpoint(stats)
        stats["accepted"] = [t.slug for t in accepted]
        if ctx.args.report:
            ctx.args.report.write_text(json.dumps(stats, indent=2, ensure_ascii=False))
        if ctx.args.apply and not ctx.shutdown.is_set():
            # Classify loads a snapshot once per run. Exclude old-snapshot
            # drains while publishing/requeueing, or a running job could finish
            # with old labels after the stale pass had already visited it.
            async with ctx.storage.worker_lock("classify") as acquired:
                if not acquired:
                    return RunOutcome("partial", stats, "Classify is busy; retry publication")
                if accepted:
                    stats["created"] = await ctx.storage.create_discovered_topics(
                        TaxonomySeed(1, tuple(accepted), ()), vectors,
                        model=self._embedder.model_name, dim=self._embedder.dim)
                    await ctx.checkpoint(stats)
                # Recover a previous run interrupted after publishing topics.
                snapshot = await ctx.storage.classifier_snapshot()
                stats["requeued"] = await ctx.storage.reenqueue_classifications(
                    since=ctx.window.start, until=ctx.window.end, set_hash=snapshot.set_hash)
        if ctx.args.report:
            ctx.args.report.write_text(json.dumps(stats, indent=2, ensure_ascii=False))
        return RunOutcome("partial" if ctx.shutdown.is_set() else "succeeded", stats)
