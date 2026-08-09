# Python task queues compared

Python has a crowded field of task queues, and picking one is mostly about matching a library to how your service is shaped. This page lays out the landscape honestly, with real download data, and explains where `aio-fluid` does, and does not, make sense.

If you are choosing for the first time, the short version:

- **You want the biggest ecosystem and battle-tested defaults**: [Celery](https://docs.celeryq.dev/).
- **You want something simple and Redis-only**: [RQ](https://python-rq.org/).
- **You run an async (`asyncio`) service and your tasks never block the loop**: an async-native queue like [arq](https://arq-docs.helpmanual.io/) or [taskiq](https://taskiq-python.github.io/).
- **You run an async service *and* some tasks are CPU-heavy** (parsing, pandas, model scoring, rendering) that would otherwise freeze your event loop: this is exactly what `aio-fluid` is built for. See [CPU bound tasks](https://fluid.quantmind.com/tutorials/tasks/#cpu-bound-tasks) and [K8s Jobs](https://fluid.quantmind.com/tutorials/task_k8s/index.md).
- **You need ordered batch pipelines, backfills and data-aware scheduling**: you want a workflow orchestrator (Airflow, Dagster, Prefect, Luigi), not a task queue. See [Workflow orchestrators](#workflow-orchestrators-a-different-layer).

## Popularity

Is Celery still the one everyone uses? Yes, decisively. It out-downloads the nearest real queue (RQ) by roughly an order of magnitude, and the async-native niche that `aio-fluid` competes in is comparatively small. That is both the opportunity (few libraries own "async-native + CPU-bound") and an honest reality check on the size of that audience.

*Downloads = last 30 days on PyPI, via [pypistats.org](https://pypistats.org). Seeded figures; run `make stats` to refresh and re-rank from live data. Counts are inflated by CI, mirrors and Docker builds, so read them as orders of magnitude, not user counts.*

| Library                                                  | Downloads / mo | Async-native | CPU work off the loop | Notes                                                            |
| -------------------------------------------------------- | -------------- | ------------ | --------------------- | ---------------------------------------------------------------- |
| [Celery](https://pypi.org/project/celery/)               | 48.9M          | partial      | separate worker fleet | The incumbent: biggest ecosystem, broker-agnostic.               |
| [APScheduler](https://pypi.org/project/apscheduler/)     | 37.1M          | ✅           | n/a                   | A *scheduler*, not a distributed queue, listed for scale.        |
| [RQ](https://pypi.org/project/rq/)                       | 2.4M           | no           | separate worker       | Simple, Redis-only; the common 'lite Celery'.                    |
| [Dramatiq](https://pypi.org/project/dramatiq/)           | 270k           | no           | separate worker       | Ergonomic Celery alternative.                                    |
| [Huey](https://pypi.org/project/huey/)                   | 117k           | no           | separate worker       | Lightweight, very few dependencies.                              |
| [taskiq](https://pypi.org/project/taskiq/)               | 34k            | ✅           | no                    | Async-native, pluggable brokers; typed parameters.               |
| [arq](https://pypi.org/project/arq/)                     | n/a            | ✅           | no                    | Async-native, Redis; count pending first `make stats`.           |
| [SAQ](https://pypi.org/project/saq/)                     | n/a            | ✅           | no                    | Async-native, Redis; count pending first `make stats`.           |
| [Procrastinate](https://pypi.org/project/procrastinate/) | n/a            | ✅           | no                    | Async-native, Postgres-backed; count pending first `make stats`. |
| [aio-fluid](https://pypi.org/project/aio-fluid/)         | 4k             | ✅           | subprocess / k8s Job  | This library: CPU-bound work is a first-class task type.         |

## The libraries

**Celery** is the default answer for a reason: the largest ecosystem, broker flexibility (RabbitMQ, Redis, SQS…), and years of production hardening. It predates `asyncio`, though, and its standard answer to CPU-bound work is to run a separate worker fleet. If you need breadth and maturity, reach for Celery.

**RQ** is the "simple Celery": Redis-only, small API, easy to reason about. Synchronous by design, so it is great for straightforward background jobs but not aimed at async services.

**Dramatiq** is an ergonomic, well-designed Celery alternative with a cleaner API and retries/middleware built in. Still a worker-per-process model rather than async-native.

**Huey** is deliberately tiny and dependency-light, a good fit for small projects that want a queue without operational weight.

**arq**, **taskiq**, **SAQ** and **Procrastinate** are the async-native cohort. They run `async def` tasks on the event loop and are excellent for IO-bound work. By design they assume tasks do not block the loop, so CPU-bound work is out of scope. That is the same limitation that motivated `aio-fluid`.

**APScheduler** is included only for scale: it is a *scheduler* (fire a job on an interval or cron), not a broker-backed distributed queue, so it solves a different problem.

### Workflow orchestrators, a different layer

**Luigi**, **Airflow**, **Prefect** and **Dagster** come up in the same searches, but they are workflow orchestrators rather than task queues. You declare a graph of batch steps and the framework owns ordering, backfills and data-aware scheduling. They do execute your code, and several of them delegate that execution to a queue (Airflow's `CeleryExecutor`, Dagster's Celery executor), which is the tell: they sit a layer above the field on this page, and can run on top of it.

`aio-fluid` covers the simple end of that layer. A task queues the next one with TaskRun.queue, and each run records the run it came from, so multi-step pipelines are ordinary Python with no extra machinery and can be traced back to whatever started them. See [Chaining tasks](https://fluid.quantmind.com/tutorials/tasks/#chaining-tasks). If your pipeline is a handful of steps triggered on a schedule, that is very likely all you need.

Reach for a real orchestrator when you need backfills over historical partitions, resuming a run from the step that failed, or lineage across hundreds of datasets. `aio-fluid` models none of those, and pretending otherwise would waste your time.

## Where aio-fluid fits

`aio-fluid` is an async-native queue like arq/taskiq, but it treats CPU-bound work as a first-class task type instead of assuming it away. Mark a task [`cpu_bound=True`](https://fluid.quantmind.com/tutorials/tasks/#cpu-bound-tasks) and it runs in a **fresh subprocess** so heavy work never blocks the event loop; when the consumer runs inside Kubernetes, the *same task* dispatches as a [Kubernetes Job](https://fluid.quantmind.com/tutorials/task_k8s/index.md) instead, with no code change and no parallel worker deployment to maintain.

So the honest positioning is not "Celery killer." It is this: **if you run an async service and have ever watched one CPU-heavy task freeze the whole thing, `aio-fluid` is built for that specific pain.** If you have no CPU-bound work, a lighter async queue will serve you just as well; if you need Celery's ecosystem, use Celery.

## Caveats on the numbers

PyPI download counts are a rough popularity proxy, not a user count. They are inflated by CI/CD pipelines, Docker image builds, and mirrors, and libraries that are transitive dependencies of popular packages (Celery, APScheduler) are inflated the most. Treat every figure above as an order of magnitude. A value of `n/a` means the count has not been fetched yet.

## Refreshing this table

The table above is regenerated from live data by `scripts/task_queue_stats.py`:

```bash
make stats
```

It fetches last-30-days downloads from the [pypistats.org](https://pypistats.org) API for the curated library list, re-ranks the table, and rewrites the region between the `STATS` marker comments in this page. Run it before a release, or whenever the numbers look stale.
