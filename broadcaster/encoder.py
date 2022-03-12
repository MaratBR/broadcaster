import json
import pickle
from abc import ABC, abstractmethod
from typing import Any


class MessageEncoder(ABC):
    @abstractmethod
    def encode(self, message: Any) -> Any:
        ...

    @abstractmethod
    def decode(self, raw_message: Any) -> Any:
        ...


class PickleEncoder(MessageEncoder):
    """
    Encodes the message as bytes using
    built-in pickle module
    """

    def encode(self, message: Any) -> bytes:
        return pickle.dumps(message)

    def decode(self, raw_message: bytes) -> Any:
        return pickle.loads(raw_message)


class JSONEncoder(MessageEncoder):
    """
    Encodes the message as string in JSON
    format and decodes it from JSON.
    Uses built-in module "json"
    """

    def encode(self, message: Any) -> str:
        return json.dumps(message)

    def decode(self, raw_message: str) -> Any:
        return json.loads(raw_message)
