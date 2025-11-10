"""Simple JDBC-like source placeholder."""

from .base import BaseSource


class JDBCSource(BaseSource):
    def __init__(self, conn_str: str):
        self.conn_str = conn_str

    def read(self):
        # placeholder: return empty iterator
        return iter([])
