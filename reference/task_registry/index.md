# Task Registry

## fluid.scheduler.broker.TaskRegistry

Bases: `dict[str, Task[TP]]`

A registry of tasks

### periodic

```python
periodic()
```

Iterate over periodic tasks

Source code in `fluid/scheduler/broker.py`

```python
def periodic(self) -> Iterable[Task]:
    """Iterate over periodic tasks"""
    for task in self.values():
        yield task
```
