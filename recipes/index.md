# Recipes

A cheat sheet of the patterns that cover most applications, and the mistakes that are easy to make. Each entry links to the tutorial that explains it in full.

## Declare a task

A task is an async function taking a single TaskRun argument, decorated with @task.

```python
from fluid.scheduler import TaskRun, task


@task
async def say_hi(ctx: TaskRun) -> None:
    ctx.logger.info("hi")
```

Parameters are a pydantic model, validated before the run starts:

```python
class Greet(BaseModel):
    name: str = "world"


@task
async def greet(ctx: TaskRun[Greet]) -> None:
    ctx.logger.info("hi %s", ctx.params.name)
```

Annotate the second type parameter to get typed dependencies as well, `TaskRun[Greet, Deps]`. Without it `ctx.deps` is `Any`. See [Tasks](https://fluid.quantmind.com/tutorials/tasks/index.md) and [Task Dependencies](https://fluid.quantmind.com/tutorials/task_deps/index.md).

## Choose a task manager

| Process                                | Class         |
| -------------------------------------- | ------------- |
| Queues work, never runs it             | TaskManager   |
| Runs queued work                       | TaskConsumer  |
| Runs queued work and owns the schedule | TaskScheduler |

TaskScheduler is the default choice for a single service. See [Task Managers](https://fluid.quantmind.com/tutorials/task_managers/index.md).

```python
scheduler = TaskScheduler(deps=deps)
scheduler.register_from_module(my_tasks)
```

Tasks must be registered on every process that queues them, not only on the one that runs them.

## Serve over HTTP

```python
from fluid.scheduler import task_manager_fastapi

app = task_manager_fastapi(scheduler)
```

`POST /tasks/{name}` queues a run, `GET /tasks` lists tasks, `GET /tasks-status` reports the running managers. Your own routes reach the manager with `TaskManagerDep`. See [Extending the FastAPI App](https://fluid.quantmind.com/tutorials/task_fastapi/index.md).

## Command line entry point

```python
from fluid.scheduler.cli import TaskManagerCLI

if __name__ == "__main__":
    TaskManagerCLI(scheduler_app)()
```

Gives `serve`, `ls`, `exec` and `enable`. This is **required** for applications with CPU bound tasks. See [Task Queue App](https://fluid.quantmind.com/tutorials/task_app/index.md).

## Schedule

```python
@task(schedule=every(timedelta(seconds=30)))
async def heartbeat(ctx: TaskRun) -> None: ...


@task(schedule=crontab(hours="*/2"))
async def report(ctx: TaskRun) -> None: ...
```

Schedules only fire in a process running a TaskScheduler.

## Offload CPU bound work

```python
@task(cpu_bound=True, timeout_seconds=600)
async def crunch(ctx: TaskRun) -> None:
    heavy_pandas_work()
```

The same declaration runs as a subprocess locally and as a Kubernetes Job in-cluster. See [CPU bound tasks](https://fluid.quantmind.com/tutorials/tasks/#cpu-bound-tasks) and [K8s Jobs](https://fluid.quantmind.com/tutorials/task_k8s/index.md).

## Shared resources

```python
@dataclass
class Deps:
    http_client: HttpxClient = field(default_factory=HttpxClient)


deps = Deps()
scheduler = TaskScheduler(deps=deps)
scheduler.add_async_context_manager(deps.http_client)
```

Anything registered with add_async_context_manager is opened on startup and closed on shutdown. See [Task Dependencies](https://fluid.quantmind.com/tutorials/task_deps/index.md).

## Retries, limits and timeouts

```python
@task(
    timeout_seconds=300,
    max_concurrency=2,
    retry=RetryPolicy(max_attempts=3, wait=2.0, backoff=2.0),
    rate_limit_retry=RetryPolicy(max_attempts=10, wait=5.0),
)
async def fetch(ctx: TaskRun) -> None: ...
```

`timeout_seconds` defaults to 60. `max_concurrency=0` means no limit. See [Task Retries](https://fluid.quantmind.com/tutorials/task_retry/index.md).

## Queue work

```python
await task_manager.queue("greet", name="luca")   # from anywhere
await ctx.queue("next_step", data_id=42)         # from inside a task, records the chain
run = await consumer.queue_and_wait("greet")     # queue and await the result
run = await task_manager.execute("greet")        # run inline, skipping the queue
```

Prefer chaining with `ctx.queue` over waiting. See [Chaining tasks](https://fluid.quantmind.com/tutorials/tasks/#chaining-tasks).

## Persist run history

```python
task_manager.with_plugin(TaskDbPlugin(CrudDB.from_env()))
```

Adds a `/tasks-history` API backed by Postgres. Requires the `db` extra. See [Plugins](https://fluid.quantmind.com/tutorials/task_app/#plugins).

## Common mistakes

- **CPU bound task without a CLI entry point.** The subprocess runs the application entry point, so it has to be a TaskManagerCLI. A consumer started any other way raises CpuBoundEntryPointError on startup.
- **Expecting a plain TaskManager to run anything.** It queues, it does not consume. Nothing executes until a consumer is running somewhere.
- **Event handlers on a plain TaskManager.** register_async_handler is a no-op there, so plugins built on lifecycle events, the database plugin included, record nothing. Attach them to the consumer.
- **More than one scheduler on a broker.** Each one keeps its own record of what it last fired, in memory, so every due task is queued twice. Replicate consumers, not schedulers.
- **Expecting a consumer to fire schedules.** Only a TaskScheduler watches the clock.
- **Sharing state in memory with a CPU bound task.** It runs in another process with its own dependencies. Pass state through the database or the broker.
- **Blocking calls in a normal task.** Anything CPU heavy or blocking freezes the event loop for every other task in the process. Mark it `cpu_bound=True`.
- **Forgetting to register a task on the producer.** Queueing by name requires the task in the registry, and a consumer receiving a run for a task it does not know logs an unknown task error.
