<p align="center">
  <img src="https://suleymanozkeskin.github.io/horsies/galloping-horsie.svg" alt="Horsies Logo" width="200" style="border-radius: 20px" />
</p>

# Horsies

**PostgreSQL-backed background task queue and workflow engine for Python.**

[**Full Documentation**](https://suleymanozkeskin.github.io/horsies/) | [**PyPI**](https://pypi.org/project/horsies/) | [**GitHub**](https://github.com/suleymanozkeskin/horsies)

---

This library is a response to frustrations I had with existing task queue and workflow libraries.

Most businesses need durable, observable systems with strong execution guarantees. Traditional queues do not give you that out of the box. You end up writing tedious state machines around them, and your execution state gets mixed into your business logic, because your execution layer was never durable.

Horsies lets you declare your conditional business logic as tasks and workflows, without managing execution state on top of it.

Python type hints stop at the queue. Once you send a task, its arguments become JSON, and you find out a type was wrong when a worker fails, and only if you run something like Prometheus or Sentry in production.

Horsies solves both. The function signature is the contract, and every value is held to it for its whole life. A type that cannot survive the trip is refused when you define the task. A wrong value is refused when you send it, before anything is stored. What a task returns is checked before it is saved, and what you read back is the type you declared, or a clear error. Workflows get the same treatment: wire a task that returns a string into a parameter that takes an int, and it fails at definition, not halfway through a run.

Horsies makes it incredibly hard to have a runtime failure because of types. If one happens anyway, you do not need a heavy service to see it. The failure is stored with the task as a named error, with the type it expected and the type it got, and the monitoring web UI shows it from a simple FastAPI app that you can mount in your own server.

That promise about execution state is the reason Horsies exists. There is no unclear state, because nothing lives in memory. Any work that needs to be done is claimed and leased with an identity, so even a crash is traceable: you can see which worker held the task, when, and what happened to it.

There is one way for a task to fail. Every task returns its value or a `TaskError` with a code, and that is part of the task contract. Your error cases are defined and handled, and they do not disappear into whatever happened to raise. This matters even more when an AI coding agent writes the task: it cannot skip the error path, because the contract does not let it.

The library's own failures use the same form, sorted into families, so you can tell a failing broker from a task that broke its contract, from a task that timed out, from a business error you chose to return. Retries follow those codes, so you decide which failures are worth another attempt.

Those state machines do two jobs: they remember what happened to each step, and they decide what runs next. Horsies does the first job for you, because every task ends in a typed value or a named error, and that outcome is stored. That leaves the decision, and you declare it. A workflow is a graph of tasks: each node says what it waits for and what happens if that fails, and the workflow says what counts as success. A step can even take a failure as its input and act on it. A whole workflow can be one node in another, so your largest process is built from the same few declarations as your smallest, and your tasks keep only your business logic.

The worker model is extremely resilient. By default, each running task has a child process to itself, so a segfault, an out-of-memory kill or a timeout ends that task and no other. The worker sees it at once and starts a new child in that slot. It also knows whether your code had started: a task that had not goes back to the queue, and a task that had runs again only if you asked for that, because its side effects may already have happened. If the whole worker dies, its children stop with it, so a task cannot keep running next to its own retry, and another worker recovers what it held. Tasks that only wait on the network can share a process, when you mark them and set up the worker for it. That is a different deal, and you choose it per task. At its timeout a shared task is cancelled, or left to run if it runs on a thread. If it does not stop, the process takes no new task, and the worker ends it once no task in it has time left. A crash in that process, or a task that blocks it, ends the tasks in it too. One thing does not change: a retry never starts while the earlier attempt still runs. That covers the work a task awaits and the asyncio tasks it starts. A thread, a scheduled callback or a detached subprocess that a task starts and does not wait for is outside that promise.

Monitoring comes with the library, because the state Horsies runs on is the state you want to see. The web UI reads the same records your workers write, so there is no agent or exporter to set up, and you get the live state and the history of your tasks, queues, workflows, schedules and workers, with the load on each worker over time. Errors are grouped by their code families, so a spike of broker failures does not look like a spike of business errors. A workflow run opens as its graph, with its failed nodes one click away and its child workflows inside it, and a task opens down to each attempt: which worker ran it, when, and what came back.

You cannot boot an invalid configuration. Workers and schedulers validate your app before they start, and `horsies check` runs the same validation in CI, with no worker and no database. It imports your tasks, runs your workflow builders and checks every graph, queue and retry policy, so these mistakes never reach runtime. A problem is reported the way a compiler reports it, with a stable `HRS` code, the source line and a suggested fix, so an AI coding agent can read the error and correct itself.

You can run as many schedulers as you want for high availability, without worrying about double fires. Each schedule is locked in Postgres while it fires, and each slot has one fixed task id, so a second scheduler cannot enqueue it twice.

The feature set of this library is a way to judge how many sharp edges have already been thought through and solved for you.

Horsies is pre-1.0. Breaking API changes can come in minor releases, and the [changelog](https://suleymanozkeskin.github.io/horsies/changelog/) documents each one.
