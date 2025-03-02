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
