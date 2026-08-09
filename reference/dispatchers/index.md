# Event Dispatchers

A set of classes for dispatching events, they can be imported from `fluid.utils.dispatcher`:

```python
from fluid.utils.dispatcher import Dispatcher
```

## fluid.utils.dispatcher.Event

Bases: `NamedTuple`

### type

```python
type
```

The event type

### tag

```python
tag = ''
```

The event tag - for registering multiple handlers for a given event type

### from_string_or_event

```python
from_string_or_event(event)
```

| PARAMETER | DESCRIPTION                                                                                |
| --------- | ------------------------------------------------------------------------------------------ |
| `event`   | The Event or a string of the form {event_type} or {event_type}.{event_tag} **TYPE:** \`str |

Source code in `fluid/utils/dispatcher.py`

```python
@classmethod
def from_string_or_event(
    cls,
    event: Annotated[
        str | Self,
        Doc(
            "The [Event][fluid.utils.dispatcher.Event] or a string of"
            " the form `{event_type}` or `{event_type}.{event_tag}`"
        ),
    ],
) -> Self:
    if isinstance(event, str):
        return cls.from_string(event)
    return event
```

### from_string

```python
from_string(event)
```

| PARAMETER | DESCRIPTION                                                                            |
| --------- | -------------------------------------------------------------------------------------- |
| `event`   | The event string has the form {event_type} or {event_type}.{event_tag} **TYPE:** `str` |

Source code in `fluid/utils/dispatcher.py`

```python
@classmethod
def from_string(
    cls,
    event: Annotated[
        str,
        Doc(
            "The event string has the form `{event_type}` "
            "or `{event_type}.{event_tag}`"
        ),
    ],
) -> Self:
    bits = event.split(".")
    return cls(bits[0], bits[1] if len(bits) > 1 else "")
```

## fluid.utils.dispatcher.BaseDispatcher

```python
BaseDispatcher()
```

Bases: `Generic[MessageType, MessageHandlerType]`, `ABC`

Base generic abstract class for dispatchers

Source code in `fluid/utils/dispatcher.py`

```python
def __init__(self) -> None:
    self._msg_handlers: defaultdict[str, dict[str, MessageHandlerType]] = (
        defaultdict(
            dict,
        )
    )
```

### register_handler

```python
register_handler(event, handler)
```

Register a handler for the given event

It is possible to register multiple handlers for the same event type by providing a different tag for each handler.

For example, to register two handlers for the event type `foo`:

```python
dispatcher.register_handler("foo.first", handler1)
dispatcher.register_handler("foo.second", handler2)
```

| PARAMETER | DESCRIPTION                                             |
| --------- | ------------------------------------------------------- |
| `event`   | The event to register the handler for **TYPE:** \`Event |
| `handler` | The handler to register **TYPE:** `MessageHandlerType`  |

Source code in `fluid/utils/dispatcher.py`

````python
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
````

### unregister_handler

```python
unregister_handler(event)
```

Unregister a handler for the given event

It returns the handler that was unregistered or `None` if no handler was registered for the given event.

| PARAMETER | DESCRIPTION                                           |
| --------- | ----------------------------------------------------- |
| `event`   | The event to unregister the handler **TYPE:** \`Event |

Source code in `fluid/utils/dispatcher.py`

```python
def unregister_handler(
    self, event: Annotated[Event | str, Doc("The event to unregister the handler")]
) -> MessageHandlerType | None:
    """Unregister a handler for the given event

    It returns the handler that was unregistered or `None` if no handler was
    registered for the given event.
    """
    event = Event.from_string_or_event(event)
    return self._msg_handlers[event.type].pop(event.tag, None)
```

### get_handlers

```python
get_handlers(message)
```

Get all event handlers for the given message

This method returns a dictionary of all handlers registered for the given message type. If no handlers are registered for the message type, it returns `None`.

| PARAMETER | DESCRIPTION                                                 |
| --------- | ----------------------------------------------------------- |
| `message` | The message to get the handlers for **TYPE:** `MessageType` |

Source code in `fluid/utils/dispatcher.py`

```python
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
```

### event_type

```python
event_type(message)
```

return the event type as string

Source code in `fluid/utils/dispatcher.py`

```python
@abstractmethod
def event_type(self, message: MessageType) -> str:
    """return the event type as string"""
```

## fluid.utils.dispatcher.Dispatcher

```python
Dispatcher()
```

Bases: `BaseDispatcher[MessageType, Callable[[MessageType], None]]`

Dispatcher for sync handlers

Source code in `fluid/utils/dispatcher.py`

```python
def __init__(self) -> None:
    self._msg_handlers: defaultdict[str, dict[str, MessageHandlerType]] = (
        defaultdict(
            dict,
        )
    )
```

### dispatch

```python
dispatch(message)
```

dispatch the message to all handlers

It returns the number of handlers that were called

Source code in `fluid/utils/dispatcher.py`

```python
def dispatch(self, message: MessageType) -> int:
    """dispatch the message to all handlers

    It returns the number of handlers that were called
    """
    handlers = self.get_handlers(message)
    if handlers:
        for handler in handlers.values():
            handler(message)
    return len(handlers or ())
```

### register_handler

```python
register_handler(event, handler)
```

Register a handler for the given event

It is possible to register multiple handlers for the same event type by providing a different tag for each handler.

For example, to register two handlers for the event type `foo`:

```python
dispatcher.register_handler("foo.first", handler1)
dispatcher.register_handler("foo.second", handler2)
```

| PARAMETER | DESCRIPTION                                             |
| --------- | ------------------------------------------------------- |
| `event`   | The event to register the handler for **TYPE:** \`Event |
| `handler` | The handler to register **TYPE:** `MessageHandlerType`  |

Source code in `fluid/utils/dispatcher.py`

````python
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
````

### unregister_handler

```python
unregister_handler(event)
```

Unregister a handler for the given event

It returns the handler that was unregistered or `None` if no handler was registered for the given event.

| PARAMETER | DESCRIPTION                                           |
| --------- | ----------------------------------------------------- |
| `event`   | The event to unregister the handler **TYPE:** \`Event |

Source code in `fluid/utils/dispatcher.py`

```python
def unregister_handler(
    self, event: Annotated[Event | str, Doc("The event to unregister the handler")]
) -> MessageHandlerType | None:
    """Unregister a handler for the given event

    It returns the handler that was unregistered or `None` if no handler was
    registered for the given event.
    """
    event = Event.from_string_or_event(event)
    return self._msg_handlers[event.type].pop(event.tag, None)
```

### get_handlers

```python
get_handlers(message)
```

Get all event handlers for the given message

This method returns a dictionary of all handlers registered for the given message type. If no handlers are registered for the message type, it returns `None`.

| PARAMETER | DESCRIPTION                                                 |
| --------- | ----------------------------------------------------------- |
| `message` | The message to get the handlers for **TYPE:** `MessageType` |

Source code in `fluid/utils/dispatcher.py`

```python
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
```

### event_type

```python
event_type(message)
```

return the event type as string

Source code in `fluid/utils/dispatcher.py`

```python
@abstractmethod
def event_type(self, message: MessageType) -> str:
    """return the event type as string"""
```

## fluid.utils.dispatcher.AsyncDispatcher

```python
AsyncDispatcher()
```

Bases: `BaseDispatcher[MessageType, Callable[[MessageType], Awaitable[None]]]`

Dispatcher for async handlers

Source code in `fluid/utils/dispatcher.py`

```python
def __init__(self) -> None:
    self._msg_handlers: defaultdict[str, dict[str, MessageHandlerType]] = (
        defaultdict(
            dict,
        )
    )
```

### dispatch

```python
dispatch(message)
```

Dispatch the message and wait for all handlers to complete

It returns the number of handlers that were called

Source code in `fluid/utils/dispatcher.py`

```python
async def dispatch(self, message: MessageType) -> int:
    """Dispatch the message and wait for all handlers to complete

    It returns the number of handlers that were called
    """
    handlers = self.get_handlers(message)
    if handlers:
        await asyncio.gather(*[handler(message) for handler in handlers.values()])
    return len(handlers or ())
```

### register_handler

```python
register_handler(event, handler)
```

Register a handler for the given event

It is possible to register multiple handlers for the same event type by providing a different tag for each handler.

For example, to register two handlers for the event type `foo`:

```python
dispatcher.register_handler("foo.first", handler1)
dispatcher.register_handler("foo.second", handler2)
```

| PARAMETER | DESCRIPTION                                             |
| --------- | ------------------------------------------------------- |
| `event`   | The event to register the handler for **TYPE:** \`Event |
| `handler` | The handler to register **TYPE:** `MessageHandlerType`  |

Source code in `fluid/utils/dispatcher.py`

````python
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
````

### unregister_handler

```python
unregister_handler(event)
```

Unregister a handler for the given event

It returns the handler that was unregistered or `None` if no handler was registered for the given event.

| PARAMETER | DESCRIPTION                                           |
| --------- | ----------------------------------------------------- |
| `event`   | The event to unregister the handler **TYPE:** \`Event |

Source code in `fluid/utils/dispatcher.py`

```python
def unregister_handler(
    self, event: Annotated[Event | str, Doc("The event to unregister the handler")]
) -> MessageHandlerType | None:
    """Unregister a handler for the given event

    It returns the handler that was unregistered or `None` if no handler was
    registered for the given event.
    """
    event = Event.from_string_or_event(event)
    return self._msg_handlers[event.type].pop(event.tag, None)
```

### get_handlers

```python
get_handlers(message)
```

Get all event handlers for the given message

This method returns a dictionary of all handlers registered for the given message type. If no handlers are registered for the message type, it returns `None`.

| PARAMETER | DESCRIPTION                                                 |
| --------- | ----------------------------------------------------------- |
| `message` | The message to get the handlers for **TYPE:** `MessageType` |

Source code in `fluid/utils/dispatcher.py`

```python
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
```

### event_type

```python
event_type(message)
```

return the event type as string

Source code in `fluid/utils/dispatcher.py`

```python
@abstractmethod
def event_type(self, message: MessageType) -> str:
    """return the event type as string"""
```
