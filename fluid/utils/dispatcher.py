from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Awaitable, Callable, Generic, NamedTuple, Self, TypeVar

MessageType = TypeVar("MessageType")
MessageHandlerType = TypeVar("MessageHandlerType")


class Event(NamedTuple):
    type: str
    tag: str

    @classmethod
    def from_string_or_event(cls, event: str | Self) -> Self:
        if isinstance(event, str):
            return cls.from_string(event)
        return event

    @classmethod
    def from_string(cls, event: str) -> Self:
        bits = event.split(".")
        return cls(bits[0], bits[1] if len(bits) > 1 else "")


class BaseDispatcher(Generic[MessageType, MessageHandlerType], ABC):
    def __init__(self) -> None:
        self._msg_handlers: defaultdict[str, dict[str, MessageHandlerType]] = (
            defaultdict(
                dict,
            )
        )

    def register_handler(
        self,
        event: Event | str,
        handler: MessageHandlerType,
    ) -> MessageHandlerType | None:
        event = Event.from_string_or_event(event)
        previous = self._msg_handlers[event.type].get(event.tag)
        self._msg_handlers[event.type][event.tag] = handler
        return previous

    def unregister_handler(self, event: Event | str) -> MessageHandlerType | None:
        event = Event.from_string_or_event(event)
        return self._msg_handlers[event.type].pop(event.tag, None)

    def get_handlers(
        self,
        message: MessageType,
    ) -> dict[str, MessageHandlerType] | None:
        message_type = str(self.message_type(message))
        return self._msg_handlers.get(message_type)

    @abstractmethod
    def message_type(self, message: MessageType) -> str:
        """return the message type"""


class Dispatcher(BaseDispatcher[MessageType, Callable[[MessageType], None]]):
    """Dispatcher for sync handlers"""

    def dispatch(self, message: MessageType) -> int:
        """dispatch the message"""
        handlers = self.get_handlers(message)
        if handlers:
            for handler in handlers.values():
                handler(message)
        return len(handlers or ())


class AsyncDispatcher(
    BaseDispatcher[MessageType, Callable[[MessageType], Awaitable[None]]],
):
    """Dispatcher for async handlers"""

    async def dispatch(self, message: MessageType) -> int:
        """Dispatch the message and wait for all handlers to complete"""
        handlers = self.get_handlers(message)
        if handlers:
            await asyncio.gather(*[handler(message) for handler in handlers.values()])
        return len(handlers or ())


class SimpleDispatcher(Dispatcher[MessageType]):
    def message_type(self, message: MessageType) -> str:
        return "*"
