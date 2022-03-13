import asyncio

import pytest

from broadcaster import Broadcast
from broadcaster.subscription_group import SubscriptionGroup


@pytest.mark.asyncio
async def test_simple_group():
    async with Broadcast("memory://") as broadcast:
        group = SubscriptionGroup(broadcast)
        group.add("chatroom")
        group.add("notifications")
        async with group:
            # give it some time to actually start all the tasks
            # normally you don't have to do this since the producer and the
            # consumer of the events are not the same and the delay is neglectable,
            # but in this case we have to sleep for a bit here
            await asyncio.sleep(0.1)
            await broadcast.publish("notifications", "welcome to the chat!")
            await broadcast.publish("chatroom", "hello")
            received_events = []

            async def _loop():
                async for event in group:
                    received_events.append(event)

            await asyncio.wait(
                [asyncio.create_task(_loop()), asyncio.create_task(asyncio.sleep(1))],
                return_when=asyncio.FIRST_COMPLETED,
            )
            assert len(received_events) == 2


@pytest.mark.asyncio
async def test_simple_group_remove():
    async with Broadcast("memory://") as broadcast:
        group = SubscriptionGroup(broadcast)
        group.add("chatroom")
        group.add("notifications")
        async with group:
            await asyncio.sleep(0.1)
            group.remove("chatroom")
            await asyncio.sleep(0.1)
            await broadcast.publish("notifications", "welcome to the chat!")
            await broadcast.publish("chatroom", "hello")
            received_events = []

            async def _loop():
                async for event in group:
                    received_events.append(event)

            await asyncio.wait(
                [asyncio.create_task(_loop()), asyncio.create_task(asyncio.sleep(1))],
                return_when=asyncio.FIRST_COMPLETED,
            )
            assert len(received_events) == 1
