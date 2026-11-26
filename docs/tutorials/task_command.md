# Run a Command Line Tool

A task often has to call a program that is not Python: `gzip`, `pg_dump`,
`ffmpeg`, a vendor CLI. Calling it with `subprocess.run` blocks the event loop,
and every other task in the process stops until it returns.
[kernel.run][fluid.utils.kernel.run] runs the program in a subprocess and
awaits it, so the loop stays free.

## A Task Calling a Tool

This task compresses a file with `gzip`, sends every line the tool writes to the
task logger, and fails the run when the tool exits with an error:

```python
--8<-- "./examples/docs/task_command.py"
```

A few things to notice:

* The arguments are passed as a list, no shell is involved, so a path with
  spaces or quotes in it is passed as it is and cannot inject a command.
* [run][fluid.utils.kernel.run] returns the exit code, it does not raise. Check
  it and raise yourself, so the [TaskRun][fluid.scheduler.TaskRun] ends in the
  failure state and a [RetryPolicy][fluid.scheduler.models.RetryPolicy] can
  retry it.
* The task is not `cpu_bound`. The work already happens in another process, the
  task only waits for it, which is IO bound.

## Collecting the Output

Output reaches the callbacks as bytes, `result_callback` for stdout and
`error_callback` for stderr. Without a callback the output is read and
discarded, so the process never blocks on a full pipe.

With `stream_output=True` the callback is called once per line, as the lines
arrive, which suits logging progress. Without it the callback is called once,
with the whole of stdout, when the process closes it, which suits parsing:

```python
result = kernel.CollectBytes()
code = await kernel.run("git", "rev-parse", "HEAD", result_callback=result)
commit = result.data.decode().strip()
```

[CollectBytes][fluid.utils.kernel.CollectBytes] keeps the output in memory,
[EncodedLog][fluid.utils.kernel.EncodedLog] decodes it and passes it to a log
function. Decode with [Utf8Decoder][fluid.utils.kernel.Utf8Decoder] rather than
`bytes.decode`, a character can be split across two chunks.

## Input and Environment

Stdin is `/dev/null` unless you pass `input`, so a tool which reads stdin, or
prompts, sees an end of file instead of waiting forever. Pass `input` to feed
it:

```python
await kernel.run("psql", "--file", "-", input=sql.encode())
```

`env` **replaces** the environment rather than extending it. Merge it with the
current one when the tool needs `PATH`, `HOME` or credentials:

```python
await kernel.run("aws", "s3", "ls", env={**os.environ, "AWS_REGION": "eu-west-1"})
```

## Timeouts

When the task times out, the coroutine awaiting
[run][fluid.utils.kernel.run] is cancelled and the process is terminated with a
SIGTERM, then killed with a SIGKILL if it is still running after
`terminate_grace_period` seconds. A tool cannot outlive the task that started
it, so set `timeout_seconds` on the [@task][fluid.scheduler.task] decorator to
the longest the tool should take.
