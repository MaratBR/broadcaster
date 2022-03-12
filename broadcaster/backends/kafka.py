from typing import Any, Optional, Set
from urllib.parse import urlparse

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from broadcaster.event import Event

from .base import BroadcastBackend


class KafkaBackend(BroadcastBackend):
    _producer: Optional[AIOKafkaProducer]
    _consumer: Optional[AIOKafkaConsumer]

    def __init__(self, url: str):
        self._producer = None
        self._consumer = None
        self._servers = [urlparse(url).netloc]
        self._consumer_channels: Set[str] = set()

    async def connect(self) -> None:
        self._producer = AIOKafkaProducer(bootstrap_servers=self._servers)
        self._consumer = AIOKafkaConsumer(bootstrap_servers=self._servers)
        await self._producer.start()
        await self._consumer.start()

    async def disconnect(self) -> None:
        # to calm mypy down and just in case
        assert self._producer is not None
        assert self._consumer is not None
        await self._producer.stop()
        await self._consumer.stop()

    async def subscribe(self, channel: str) -> None:
        assert self._consumer is not None
        self._consumer_channels.add(channel)
        self._consumer.subscribe(topics=self._consumer_channels)

    async def unsubscribe(self, channel: str) -> None:
        if self._consumer:
            self._consumer.unsubscribe()

    async def publish(self, channel: str, message: Any) -> None:
        assert self._producer is not None
        await self._producer.send_and_wait(channel, message.encode("utf8"))

    async def next_published(self) -> Event:
        assert self._consumer is not None
        message = await self._consumer.getone()
        return Event(channel=message.topic, message=message.value.decode("utf8"))
