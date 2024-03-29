import asyncio
from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Awaitable, Callable, Dict, Generic, TypeVar

MessageType = TypeVar("MessageType")
MessageHandlerType = TypeVar("MessageHandlerType")


class BaseDispatcher(Generic[MessageType, MessageHandlerType], ABC):
    def __init__(self) -> None:
        self._msg_handlers: Dict[str, dict[str, MessageHandlerType]] = defaultdict(
            dict,
        )

    def register_handler(
        self,
        message_type: str,
        handler: MessageHandlerType,
        tag: str = "",
    ) -> MessageHandlerType | None:
        previous = self._msg_handlers[message_type].get(tag)
        self._msg_handlers[message_type][tag] = handler
        return previous

    def unregister_handler(
        self, message_type: str, tag: str = ""
    ) -> MessageHandlerType | None:
        return self._msg_handlers[message_type].pop(tag, None)

    def on(
        self, handler: MessageHandlerType, tag: str = ""
    ) -> MessageHandlerType | None:
        return self.register_handler("*", handler, tag)

    def get_handlers(
        self,
        message: MessageType,
    ) -> dict[str, MessageHandlerType] | None:
        message_type = self.message_type(message)
        return self._msg_handlers.get(message_type)

    @abstractmethod
    def message_type(self, message: MessageType) -> str:
        """return the message type"""


class Dispatcher(BaseDispatcher[MessageType, Callable[[MessageType], None]]):
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
    async def dispatch(self, message: MessageType) -> int:
        """Dispatch the message"""
        handlers = self.get_handlers(message)
        if handlers:
            await asyncio.gather(*[handler(message) for handler in handlers.values()])
        return len(handlers or ())


class SimpleDispatcher(Dispatcher[MessageType]):
    def message_type(self, message: MessageType) -> str:
        return "*"