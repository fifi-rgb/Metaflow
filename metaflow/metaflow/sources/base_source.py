"""Base classes for source connectors."""

from typing import Iterable, Dict, Any


class BaseSource:
    def read(self) -> Iterable[Dict[str, Any]]:
        raise NotImplementedError
