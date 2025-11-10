"""Delta lake sink placeholder."""

from typing import Iterable, Dict, Any


class DeltaSink:
    def __init__(self, path: str):
        self.path = path

    def write(self, rows: Iterable[Dict[str, Any]]):
        # placeholder: do nothing
        pass
