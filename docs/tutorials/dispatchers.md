# Event Dispatchers

[Event dispatchers](../reference/dispatchers.md) are a way to decouple the event source from the event handler. This is useful when you want to have multiple handlers for the same event, or when you want to have a single handler for multiple events.

## A Simple Dispatcher

In this example we will create a simple dispatcher that will dispatch strings to a list of handlers.

The only requirement for the implementation of a [Dispatcher][fluid.utils.dispatcher.Dispatcher] is to implement the `event_type` method.

```python
--8<-- "./docs_src/simple_dispatcher.py"
```

In this example we have a simple dispatcher that will dispatch strings to a list of handlers. The `event_type` method returns the type of the event, in this case always a "*" string.

The registration of multiple handlers is done via the use of tags (see the `count_letters` registration).

## A Data Dispatcher

In this example we will create a dispatcher that will dispatch data to a list of handlers.
The event type of the message is given by the type of the data.

```python
--8<-- "./docs_src/data_dispatcher.py"
```
