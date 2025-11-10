"""
MetaFlow Data Sinks

This module provides data sink implementations for writing processed data
to various target systems:
- DeltaSink: Delta Lake tables
- JdbcSink: Relational databases (PostgreSQL, MySQL, etc.)
- FileSink: Files (Parquet, CSV, JSON, Avro)
- KafkaSink: Kafka topics
- ApiSink: REST APIs
"""

from metaflow.sinks.base_sink import BaseSink, SinkConfig, WriteMode
from metaflow.sinks.delta_sink import DeltaSink
from metaflow.sinks.jdbc_sink import JdbcSink
from metaflow.sinks.file_sink import FileSink
from metaflow.sinks.kafka_sink import KafkaSink
from metaflow.sinks.api_sink import ApiSink

__all__ = [
    'BaseSink',
    'SinkConfig',
    'WriteMode',
    'DeltaSink',
    'JdbcSink',
    'FileSink',
    'KafkaSink',
    'ApiSink',
]