"""Async, process-shared query embedding over the production article model."""

from __future__ import annotations

import asyncio
from asyncio import to_thread
from collections.abc import Sequence
from typing import Protocol

from adapters.embedding.fastembed_embedder import EMBEDDING_DIM, MODEL_NAME


class QueryEmbeddingError(RuntimeError):
    """Raised when the local query embedding model cannot serve a query."""


class QueryEmbedderProtocol(Protocol):
    model_name: str
    dim: int

    async def embed_query(self, query: str) -> Sequence[float]: ...


class QueryEmbedder:
    """Load FastEmbed lazily and serialize access to its ONNX session."""

    model_name = MODEL_NAME
    dim = EMBEDDING_DIM

    def __init__(self) -> None:
        self._embedder = None
        self._lock = asyncio.Lock()

    def _embed(self, query: str) -> list[float]:
        if self._embedder is None:
            from adapters.embedding.fastembed_embedder import FastEmbedEmbedder

            self._embedder = FastEmbedEmbedder()
        vectors = self._embedder.embed([query])
        if len(vectors) != 1 or len(vectors[0]) != self.dim:
            raise QueryEmbeddingError("Query embedding returned an invalid shape")
        return vectors[0]

    async def embed_query(self, query: str) -> Sequence[float]:
        try:
            async with self._lock:
                return await to_thread(self._embed, query)
        except QueryEmbeddingError:
            raise
        except Exception as exc:
            raise QueryEmbeddingError("Query embedding is unavailable") from exc
