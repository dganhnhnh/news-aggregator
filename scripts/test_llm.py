#!/usr/bin/env python3
"""Invoke the chat LLM used by ``backend/agent/graph.py`` directly.

Examples:
    uv run --env-file .env python scripts/test_llm.py "Say hello in Vietnamese"
    uv run --env-file .env python scripts/test_llm.py

This intentionally tests only the OpenRouter-backed ChatOpenAI client. It does
not construct or invoke the LangGraph agent, retrieval tools, or embeddings.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "backend"))

import os
from dotenv import load_dotenv
load_dotenv()

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY","")
OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL","")


DEFAULT_PROMPT = "What is the capital of France?"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Invoke the OpenRouter LLM client without running the graph."
    )
    parser.add_argument(
        "prompt",
        nargs="?",
        default=DEFAULT_PROMPT,
        help=f"Prompt to send (default: {DEFAULT_PROMPT!r})",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if not OPENROUTER_API_KEY:
        print(
            "OPENROUTER_API_KEY is not set. Load it from .env with "
            "`uv run --env-file .env python scripts/test_llm.py ...`.",
            file=sys.stderr,
        )
        return 2

    from langchain_openai import ChatOpenAI

    client = ChatOpenAI(
        model=OPENROUTER_MODEL,
        base_url=OPENROUTER_BASE_URL,
        api_key=OPENROUTER_API_KEY,
        temperature=0,
        streaming=True,
        max_tokens=8000,
    )

    print(f"Model: {OPENROUTER_MODEL}", file=sys.stderr)
    response = client.invoke(args.prompt)
    print(response.text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
