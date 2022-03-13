import asyncio
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, AsyncIterator, Dict, Optional, Type, Union
from urllib.parse import urlparse

from broadcaster.backends.base import BroadcastBackend
from broadcaster.encoder import MessageEncoder

from .event import Event


class Unsubscribed(Exception):
    pass


def _infer_backend(url: str) -> BroadcastBackend:
    parsed_url = urlparse(url)

    if parsed_url.scheme in ("redis", "rediss"):
        from broadcaster.backends.aio_redis import AIORedisBackend

        return AIORedisBackend(url)

    elif parsed_url.scheme in ("postgres", "postgresql"):
        from broadcaster.backends.postgres import PostgresBackend

        return PostgresBackend(url)

    if parsed_url.scheme == "kafka":
        from broadcaster.backends.kafka import KafkaBackend

        return KafkaBackend(url)

    elif parsed_url.scheme == "memory":
        from broadcaster.backends.memory import MemoryBackend

        return MemoryBackend()
    raise ValueError(f"Invalid/unsupported url: {url}")


class Broadcast:
    _backend: BroadcastBackend

    def __init__(
        self,
        backend: Union[str, BroadcastBackend, Type[BroadcastBackend]],
        encoder: Optional[MessageEncoder] = None,
    ):
        if isinstance(backend, str):
            self._backend = _infer_backend(backend)
        elif isinstance(backend, BroadcastBackend):
            self._backend = backend
        else:
            # assume it has no arguments
            self._backend = backend()
        self._subscribers: Dict[str, Any] = {}
        self._encoder = encoder

    async def __aenter__(self) -> "Broadcast":
        await self.connect()
        return self

    async def __aexit__(self, *args: Any, **kwargs: Any) -> None:
        await self.disconnect()

    async def connect(self) -> None:
        await self._backend.connect()
        self._listener_task = asyncio.create_task(self._listener())

    async def disconnect(self) -> None:
        if self._listener_task.done():
            self._listener_task.result()
        else:
            self._listener_task.cancel()
        await self._backend.disconnect()

    async def _listener(self) -> None:
        while True:
            event = await self._backend.next_published()
            if self._encoder and event is not None:
                event = Event(
                    channel=event.channel, message=self._encoder.decode(event.message)
                )
            for queue in list(self._subscribers.get(event.channel, [])):
                await queue.put(event)

    async def publish(self, channel: str, message: Any) -> None:
        if self._encoder:
            message = self._encoder.encode(message)
        await self._backend.publish(channel, message)

    @asynccontextmanager
    async def subscribe(self, channel: str) -> AsyncIterator["Subscriber"]:
        queue: asyncio.Queue = asyncio.Queue()

        try:
            if not self._subscribers.get(channel):
                await self._backend.subscribe(channel)
                self._subscribers[channel] = {queue}
            else:
                self._subscribers[channel].add(queue)

            yield Subscriber(queue)

            self._subscribers[channel].remove(queue)
            if not self._subscribers.get(channel):
                del self._subscribers[channel]
                await self._backend.unsubscribe(channel)
        finally:
            await queue.put(None)


class Subscriber:
    def __init__(
        self, queue: asyncio.Queue, encoder: Optional[MessageEncoder] = None
    ) -> None:
        self._queue = queue
        self._encoder = encoder

    async def __aiter__(self) -> AsyncGenerator:
        try:
            while True:
                yield await self.get()
        except Unsubscribed:
            pass

    async def get(self) -> Event:
        item = await self._queue.get()
        if item is None:
            raise Unsubscribed()
        return item
