"""Read-only evaluation: snapshot once, compare proposals on identical inputs.

uv run python scripts/evaluate_taxonomy.py --snapshot /tmp/corpus.json --capture
uv run python scripts/evaluate_taxonomy.py --snapshot /tmp/corpus.json \
    --proposal /tmp/proposal.json --output /tmp/evaluation.json

No LLM calls or database writes. Precision/recall require an independently
reviewed --gold JSON object mapping article IDs to complete relevant slug lists.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import random
from collections import Counter
from dataclasses import asdict, replace
from pathlib import Path

from adapters.storage.postgres import PostgresRawStorage
from core.config import Settings
from services.classification import TopicClassifier, TopicKeyword, classify_article
from services.taxonomy import compile_prototype, definition_hash, to_seed_dict
from services.taxonomy_discovery import DUPLICATE_SIMILARITY, parse_proposal, proposal_seed
from services.clustering import cosine_similarity
from services.text import article_embed_text
from workers.window import build_window


async def capture(args):
    window = build_window(last=args.last if not args.since else None, since=args.since, until=args.until)
    storage = PostgresRawStorage(Settings.from_env().database_url)
    try:
        snapshot = await storage.classifier_snapshot()
        topics, trees = await storage.export_taxonomy()
        stories = await storage.taxonomy_discovery_stories(since=window.start, until=window.end)
        profiles = await storage.boilerplate_profiles()
        articles, after = [], 0
        while ids := await storage.classification_evaluation_ids(
            since=window.start, until=window.end, after_id=after):
            rows = await storage.classifiable_articles(ids)
            for row in rows.values():
                text, _ = article_embed_text(title=row.title, content=row.content,
                    feed_summary=row.feed_summary, content_source=row.content_source,
                    boilerplate=profiles.get(row.source_id, frozenset()))
                articles.append(dict(id=row.article_id, title=row.title, text=text,
                    embedding=list(row.embedding), model=row.model, dim=row.dim))
            after = ids[-1]
        payload = dict(bounds={"since": window.start.isoformat(), "until": window.end.isoformat()},
            articles=articles, stories=stories, seed=to_seed_dict(topics, trees),
            classifiers=[asdict(c) for c in snapshot.classifiers])
        args.snapshot.write_text(json.dumps(payload, default=lambda x: x.hex() if isinstance(x, bytes) else str(x)))
    finally:
        await storage.close()


def evaluate(args):
    data = json.loads(args.snapshot.read_text())
    baseline = [TopicClassifier(**{**c, "definition_hash": bytes.fromhex(c["definition_hash"]),
        "keywords": tuple(TopicKeyword(**k) for k in c["keywords"])}) for c in data["classifiers"]]
    candidates = list(baseline)
    if args.threshold is not None:
        candidates = [replace(c, threshold=args.threshold) for c in candidates]
    if args.proposal:
        from adapters.embedding.fastembed_embedder import FastEmbedEmbedder
        embedder = FastEmbedEmbedder()
        proposal = parse_proposal(args.proposal.read_text())
        seed = proposal_seed(proposal, data["stories"], data["seed"]["topics"], threshold=args.new_threshold)
        for i, topic in enumerate(seed.topics):
            prototype = compile_prototype(embedder.embed(topic.exemplars))
            if any(cosine_similarity(prototype, c.prototype) >= DUPLICATE_SIMILARITY for c in candidates):
                continue
            candidates.append(TopicClassifier(-i-1, topic.slug, topic.threshold, 1,
                embedder.model_name, embedder.dim, tuple(prototype), definition_hash(topic.exemplars),
                tuple(TopicKeyword(k.value, k.weight) for k in topic.keywords)))
    evidence_ids = {a["id"] for s in data["stories"] for a in s["members"]}
    # If capture limits representatives, all other members of a discovery story
    # must also be excluded from a claimed holdout. Older experiment snapshots
    # contain cluster_id; otherwise call the split 'outside prompt', not holdout.
    story_ids = {s["id"] for s in data["stories"]}
    results = []
    for a in data["articles"]:
        row = {"id": a["id"], "title": a["title"],
               "discovery_input": a["id"] in evidence_ids or a.get("cluster_id") in story_ids}
        for name, classifiers in (("baseline", baseline), ("candidate", candidates)):
            result = classify_article(embedding=a["embedding"], model=a["model"], dim=a["dim"],
                                      text=a["text"], classifiers=classifiers)
            row[name] = [{"slug": s.slug, "score": round(s.score, 4)} for s in result.assigned]
        results.append(row)
    summary = {"window": data["bounds"], "articles": len(results), "coverage": {}}
    for group, rows in (("all", results), ("outside_discovery_input", [r for r in results if not r["discovery_input"]])):
        summary["coverage"][group] = {"articles": len(rows)}
        for name in ("baseline", "candidate"):
            assigned = sum(bool(r[name]) for r in rows)
            summary["coverage"][group][name] = {"assigned": assigned,
                "fraction": assigned / len(rows) if rows else 0,
                "by_topic": dict(Counter(x["slug"] for r in rows for x in r[name]))}
    if args.gold:
        gold = json.loads(args.gold.read_text())
        for name in ("baseline", "candidate"):
            tp = fp = fn = n = 0
            for row in results:
                if str(row["id"]) not in gold:
                    continue
                truth, pred = set(gold[str(row["id"])]), {s["slug"] for s in row[name]}
                tp += len(truth & pred); fp += len(pred-truth); fn += len(truth-pred); n += 1
            summary[name + "_gold"] = {"reviewed": n, "tp": tp, "fp": fp, "fn": fn,
                "precision": tp/(tp+fp) if tp+fp else None, "recall": tp/(tp+fn) if tp+fn else None}
    changed = [r for r in results if {s["slug"] for s in r["candidate"]} - {s["slug"] for s in r["baseline"]}]
    review = random.Random(42).sample(changed, min(60, len(changed)))
    args.output.write_text(json.dumps({"summary": summary, "review_sample": review, "results": results}, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--snapshot", type=Path, required=True)
    p.add_argument("--capture", action="store_true")
    p.add_argument("--last", default="7d")
    p.add_argument("--since")
    p.add_argument("--until")
    p.add_argument("--proposal", type=Path)
    p.add_argument("--threshold", type=float, help="Ablation only: override OLD topic thresholds")
    p.add_argument("--new-threshold", type=float, default=.4)
    p.add_argument("--gold", type=Path)
    p.add_argument("--output", type=Path, default=Path("taxonomy-evaluation.json"))
    args = p.parse_args()
    if args.capture:
        asyncio.run(capture(args))
    evaluate(args)
