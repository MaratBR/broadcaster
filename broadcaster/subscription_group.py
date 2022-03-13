import asyncio
from types import TracebackType
from typing import AsyncIterator, Dict, Optional, Set, Type

from broadcaster import Broadcast, Event


class SubscriptionGroup:
    def __init__(self, instance: Broadcast, *channels: str):
        self._broadcast = instance
        self._queue: Optional[asyncio.Queue] = None
        self._tasks: Dict[str, asyncio.Task] = {}
        self._subscribed: Set[str] = set(channels)

    async def _start(self) -> None:
        if self._queue:
            return
        self._queue = asyncio.Queue()
        for channel in self._subscribed:
            self._tasks[channel] = asyncio.create_task(self._drain_channel(channel))

    async def _reset(self) -> None:
        for task in self._tasks.values():
            if not task.done():
                task.cancel()
        self._tasks = {}
        if self._queue is not None:
            await self._queue.put(None)
        self._queue = None

    def add(self, channel: str) -> None:
        if channel in self._subscribed:
            return
        self._subscribed.add(channel)
        if self._queue:
            # if we have a queue it means the subscription group
            # is ready to accept events
            self._tasks[channel] = asyncio.create_task(self._drain_channel(channel))

    def remove(self, channel: str) -> None:
        if channel not in self._subscribed:
            return
        self._subscribed.remove(channel)
        if self._queue:
            self._tasks[channel].cancel()
            del self._tasks[channel]

    async def _drain_channel(self, channel: str) -> None:
        assert self._queue is not None
        async with self._broadcast.subscribe(channel) as subscriber:
            async for event in subscriber:
                await self._queue.put(event)

    async def __aenter__(self) -> None:
        await self._start()

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        await self._reset()

    async def __aiter__(self) -> AsyncIterator[Event]:
        while self._queue:
            event = await self._queue.get()
            if event is None:
                break
            yield event

    async def next_published(self) -> Optional[Event]:
        assert self._queue is not None
        return await self._queue.get()
