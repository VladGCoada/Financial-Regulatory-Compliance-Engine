"""External service clients for FRCE."""

from frce.clients.ecb_client import ECBClient, EcbClient
from frce.clients.kafka_consumer import KafkaConsumer

__all__ = ["ECBClient", "EcbClient", "KafkaConsumer"]
