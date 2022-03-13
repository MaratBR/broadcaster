import asyncio
from typing import Optional, Union

import aioredis

from broadcaster import Event
from broadcaster.backends.base import BroadcastBackend


class AIORedisBackend(BroadcastBackend):
    _subscribed_event: asyncio.Event
    _stop_event: asyncio.Event

    def __init__(self, url_or_client: Union[str, aioredis.Redis]):
        self._pubsub: Optional[aioredis.client.PubSub] = None
        self._url: Optional[str]
        if isinstance(url_or_client, str):
            self._owned_client = True
            self._url = url_or_client
            self._client = None
        else:
            self._owned_client = False
            self._client = url_or_client
            self._url = None

    async def connect(self) -> None:
        if self._client is None:
            self._client = aioredis.from_url(self._url)
            await self._client.ping()
        self._pubsub = self._client.pubsub()
        self._subscribed_event = asyncio.Event()
        self._stop_event = asyncio.Event()

    async def disconnect(self) -> None:
        assert self._pubsub is not None
        self._stop_event.set()
        await self._pubsub.close()
        self._pubsub = None
        if self._owned_client:
            await self._client.close()
        self._client = None

    async def subscribe(self, group: str) -> None:
        assert self._pubsub is not None
        await self._pubsub.subscribe(group)
        self._subscribed_event.set()

    async def unsubscribe(self, group: str) -> None:
        assert self._pubsub is not None
        await self._pubsub.unsubscribe(group)

    async def publish(self, channel: str, message: aioredis.client.EncodableT) -> None:
        assert self._client is not None
        await self._client.publish(channel, message)

    async def next_published(self) -> Event:
        event: Optional[Event] = None

        while event is None:
            event = await self._next_published()
        return event

    async def _next_published(self) -> Optional[Event]:
        assert self._pubsub is not None
        while not self._pubsub.subscribed or self._pubsub.connection is None:
            await self._subscribed_event.wait()
        self._subscribed_event.clear()
        message = await self._pubsub.get_message(
            ignore_subscribe_messages=True, timeout=30
        )
        if message is None:
            return None
        return Event(
            channel=message["channel"].decode("utf8"),
            message=message["data"].decode("utf8"),
        )
