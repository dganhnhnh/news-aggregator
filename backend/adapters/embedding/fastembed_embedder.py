"""fastembed (ONNX Runtime, ~50MB) instead of sentence-transformers (~2GB of
torch) for the same MiniLM model and dimension -- see week1-foundation.md §2.4.
"""

from __future__ import annotations

from collections.abc import Sequence

from fastembed import TextEmbedding

from adapters.embedding.embedder import Embedder


MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
EMBEDDING_DIM = 384


class FastEmbedEmbedder(Embedder):
    def __init__(self, model_name: str = MODEL_NAME) -> None:
        self.model_name = model_name
        self.dim = EMBEDDING_DIM
        self._model = TextEmbedding(model_name=model_name)

    def embed(self, texts: Sequence[str]) -> list[list[float]]:
        return [vector.tolist() for vector in self._model.embed(list(texts))]
