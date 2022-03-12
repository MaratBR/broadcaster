from typing import Any

import pytest

from broadcaster import Broadcast
from broadcaster.backends.kafka import KafkaBackend
from broadcaster.backends.memory import MemoryBackend
from broadcaster.backends.postgres import PostgresBackend
from broadcaster.backends.redis import RedisBackend
from broadcaster.encoder import MessageEncoder


@pytest.mark.asyncio
async def test_memory():
    async with Broadcast("memory://") as broadcast:
        async with broadcast.subscribe("chatroom") as subscriber:
            await broadcast.publish("chatroom", "hello")
            event = await subscriber.get()
            assert event.channel == "chatroom"
            assert event.message == "hello"


@pytest.mark.asyncio
async def test_memory_through_instantiation():
    async with Broadcast(MemoryBackend) as broadcast:
        async with broadcast.subscribe("chatroom") as subscriber:
            await broadcast.publish("chatroom", "hello")
            event = await subscriber.get()
            assert event.channel == "chatroom"
            assert event.message == "hello"


@pytest.mark.asyncio
async def test_encoder():
    import pickle

    raw = None
    expected_raw = pickle.dumps("hello")
    original_message = None

    class CustomEncoder(MessageEncoder):
        def decode(self, raw_message: Any) -> Any:
            nonlocal raw
            raw = raw_message
            return pickle.loads(raw_message)

        def encode(self, message: Any) -> Any:
            nonlocal original_message
            original_message = message
            return pickle.dumps(message)

    async with Broadcast("memory://", encoder=CustomEncoder()) as broadcast:
        async with broadcast.subscribe("chatroom") as subscriber:
            await broadcast.publish("chatroom", "hello")
            event = await subscriber.get()
            assert event.channel == "chatroom"
            assert event.message == "hello"
            assert raw == expected_raw
            assert original_message == "hello"


@pytest.mark.asyncio
async def test_redis():
    async with Broadcast("redis://localhost:6379") as broadcast:
        async with broadcast.subscribe("chatroom") as subscriber:
            await broadcast.publish("chatroom", "hello")
            event = await subscriber.get()
            assert event.channel == "chatroom"
            assert event.message == "hello"


@pytest.mark.asyncio
async def test_redis_directly():
    async with Broadcast(RedisBackend("redis://localhost:6379")) as broadcast:
        async with broadcast.subscribe("chatroom") as subscriber:
            await broadcast.publish("chatroom", "hello")
            event = await subscriber.get()
            assert event.channel == "chatroom"
            assert event.message == "hello"


@pytest.mark.asyncio
async def test_postgres():
    async with Broadcast(
        "postgres://postgres:postgres@localhost:5432/broadcaster"
    ) as broadcast:
        async with broadcast.subscribe("chatroom") as subscriber:
            await broadcast.publish("chatroom", "hello")
            event = await subscriber.get()
            assert event.channel == "chatroom"
            assert event.message == "hello"


@pytest.mark.asyncio
async def test_postgres_directly():
    async with Broadcast(
        PostgresBackend("postgres://postgres:postgres@localhost:5432/broadcaster")
    ) as broadcast:
        async with broadcast.subscribe("chatroom") as subscriber:
            await broadcast.publish("chatroom", "hello")
            event = await subscriber.get()
            assert event.channel == "chatroom"
            assert event.message == "hello"


@pytest.mark.asyncio
async def test_kafka():
    async with Broadcast("kafka://localhost:9092") as broadcast:
        async with broadcast.subscribe("chatroom") as subscriber:
            await broadcast.publish("chatroom", "hello")
            event = await subscriber.get()
            assert event.channel == "chatroom"
            assert event.message == "hello"


@pytest.mark.asyncio
async def test_kafka_directly():
    async with Broadcast(KafkaBackend("kafka://localhost:9092")) as broadcast:
        async with broadcast.subscribe("chatroom") as subscriber:
            await broadcast.publish("chatroom", "hello")
            event = await subscriber.get()
            assert event.channel == "chatroom"
            assert event.message == "hello"
