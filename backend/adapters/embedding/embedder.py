"""Port every embedding backend implements; lets `EmbedWorker` stay agnostic
of fastembed and take a fake in tests.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence


class Embedder(ABC):
    model_name: str
    dim: int

    @abstractmethod
    def embed(self, texts: Sequence[str]) -> list[list[float]]:
        """Blocking; callers run this off the event loop."""
