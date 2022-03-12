from typing import Any

from broadcaster.event import Event


class BroadcastBackend:
    async def connect(self) -> None:
        raise NotImplementedError()

    async def disconnect(self) -> None:
        raise NotImplementedError()

    async def subscribe(self, group: str) -> None:
        raise NotImplementedError()

    async def unsubscribe(self, group: str) -> None:
        raise NotImplementedError()

    async def publish(self, channel: str, message: Any) -> None:
        raise NotImplementedError()

    async def next_published(self) -> Event:
        raise NotImplementedError()
