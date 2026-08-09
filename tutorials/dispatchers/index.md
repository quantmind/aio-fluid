# Event Dispatchers

[Event dispatchers](https://fluid.quantmind.com/reference/dispatchers/index.md) are a way to decouple the event source from the event handler. This is useful when you want to have multiple handlers for the same event, or when you want to have a single handler for multiple events.

## A Simple Dispatcher

In this example we will create a simple dispatcher that will dispatch strings to a list of handlers.

The only requirement for the implementation of a Dispatcher is to implement the `event_type` method.

```python
from fluid.utils.dispatcher import Dispatcher


class SimpleDispatcher(Dispatcher[str]):
    def event_type(self, message: str) -> str:
        return "*"


simple = SimpleDispatcher()

assert simple.dispatch("you can dispatch strings to this dispatcher") == 0


def count_words(x: str) -> None:
    words = [x.strip() for w in x.split(" ") if w.strip()]
    print(f"number of words {len(words)}")


simple.register_handler("*", count_words)

assert simple.dispatch("you can dispatch strings to this dispatcher") == 1


def count_letters(x: str) -> None:
    letters = set(x)
    print(f"number of letters {len(letters)}")


simple.register_handler("*.count_letters", count_letters)


assert simple.dispatch("you can dispatch strings to this dispatcher") == 2
```

In this example we have a simple dispatcher that will dispatch strings to a list of handlers. The `event_type` method returns the type of the event, in this case always a "\*" string.

The registration of multiple handlers is done via the use of tags (see the `count_letters` registration).

## A Data Dispatcher

In this example we will create a dispatcher that will dispatch data to a list of handlers. The event type of the message is given by the type of the data.

```python
from typing import Any

from fluid.utils.dispatcher import Dispatcher


class MessageDispatcher(Dispatcher[Any]):
    def event_type(self, data: Any) -> str:
        return type(data).__name__


dispatcher = MessageDispatcher()

assert dispatcher.dispatch({}) == 0


def count_keys(data: Any) -> None:
    print(f"number of keys {len(data)}")


dispatcher.register_handler("dict", count_keys)


assert dispatcher.dispatch(dict(a=1, b=2)) == 1
```
