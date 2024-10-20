# Event Dispatchers

Event dispatchers are a way to decouple the event source from the event handler. This is useful when you want to have multiple handlers for the same event, or when you want to have a single handler for multiple events.

```python
from fluid.utils.dispatcher import SimpleDispatcher

simple = SimpleDispatcher[Any]()

simple.dispatch("you can dispatch anything to this generic dispatcher")
```
