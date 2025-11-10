"""Simple Kafka-like source placeholder."""

from .base import BaseSource


class KafkaSource(BaseSource):
    def __init__(self, brokers: str, topic: str):
        self.brokers = brokers
        self.topic = topic

    def read(self):
        return iter([])
