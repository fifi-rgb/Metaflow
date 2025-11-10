"""Parquet file source placeholder."""

from .base import BaseSource


class ParquetSource(BaseSource):
    def __init__(self, path: str):
        self.path = path

    def read(self):
        return iter([])
