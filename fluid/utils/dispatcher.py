from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Annotated, Awaitable, Callable, Generic, NamedTuple, Self, TypeVar

from typing_extensions import Doc

MessageType = TypeVar("MessageType")
MessageHandlerType = TypeVar("MessageHandlerType")


class Event(NamedTuple):
    type: Annotated[str, Doc("The event type")]
    tag: Annotated[
        str,
        Doc("The event tag - for registering multiple handlers for a given event type"),
    ] = ""

    @classmethod
    def from_string_or_event(cls, event: str | Self) -> Self:
        if isinstance(event, str):
            return cls.from_string(event)
        return event

    @classmethod
    def from_string(
        cls,
        event: Annotated[
            str,
            Doc(
                "The event string has the form {event_type} or {event_type}.{event_tag}"
            ),
        ],
    ) -> Self:
        bits = event.split(".")
        return cls(bits[0], bits[1] if len(bits) > 1 else "")


class BaseDispatcher(Generic[MessageType, MessageHandlerType], ABC):
    """Base generic abstract class for dispatchers"""

    def __init__(self) -> None:
        self._msg_handlers: defaultdict[str, dict[str, MessageHandlerType]] = (
            defaultdict(
                dict,
            )
        )

    def register_handler(
        self,
        event: Annotated[Event | str, Doc("The event to register the handler for")],
        handler: Annotated[MessageHandlerType, Doc("The handler to register")],
    ) -> MessageHandlerType | None:
        """Register a handler for the given event

        It is possible to register multiple handlers for the same event type by
        providing a different tag for each handler.

        For example, to register two handlers for the event type `foo`:

        ```python
        dispatcher.register_handler("foo.first", handler1)
        dispatcher.register_handler("foo.second", handler2)
        ```
        """
        event = Event.from_string_or_event(event)
        previous = self._msg_handlers[event.type].get(event.tag)
        self._msg_handlers[event.type][event.tag] = handler
        return previous

    def unregister_handler(
        self, event: Annotated[Event | str, Doc("The event to unregister the handler")]
    ) -> MessageHandlerType | None:
        """Unregister a handler for the given event

        It returns the handler that was unregistered or `None` if no handler was
        registered for the given event.
        """
        event = Event.from_string_or_event(event)
        return self._msg_handlers[event.type].pop(event.tag, None)

    def get_handlers(
        self,
        message: Annotated[MessageType, Doc("The message to get the handlers for")],
    ) -> dict[str, MessageHandlerType] | None:
        """Get all event handlers for the given message

        This method returns a dictionary of all handlers registered for the given
        message type. If no handlers are registered for the message type, it returns
        `None`.
        """
        event_type = self.event_type(message)
        return self._msg_handlers.get(event_type)

    @abstractmethod
    def event_type(self, message: MessageType) -> str:
        """return the event type as string"""


class Dispatcher(BaseDispatcher[MessageType, Callable[[MessageType], None]]):
    """Dispatcher for sync handlers"""

    def dispatch(self, message: MessageType) -> int:
        """dispatch the message to all handlers

        It returns the number of handlers that were called
        """
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
        """Dispatch the message and wait for all handlers to complete

        It returns the number of handlers that were called
        """
        handlers = self.get_handlers(message)
        if handlers:
            await asyncio.gather(*[handler(message) for handler in handlers.values()])
        return len(handlers or ())
