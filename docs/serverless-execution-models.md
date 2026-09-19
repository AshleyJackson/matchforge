# Serverless execution models

MatchForge must treat an invocation as temporary, interruptible, and
potentially concurrent with other invocations. A process, isolate, or function
instance may be reused, frozen, evicted, reset, or replaced without giving the
application a reliable opportunity to finish in-memory work.

This document summarizes the provider behaviors that affect MatchForge
coordination and recovery.

## Provider comparison

| Platform             | Execution unit           | Reuse and concurrency                                                                                                 | Termination behavior                                                                                                                                                                        | Design consequence                                                                                                 |
| -------------------- | ------------------------ | --------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| Cloudflare Workers   | V8 isolate               | An isolate can serve multiple concurrent requests; requests may be routed to different isolates                       | Isolates may be evicted for resource limits or pressure. Work after a response or client disconnect can be canceled unless passed to `waitUntil()`, which extends work for up to 30 seconds | Global mutable state is only a cache. Durable state and claim recovery must live outside the isolate               |
| Vercel Fluid Compute | Shared function instance | Multiple invocations can share an instance concurrently; the instance may be reused                                   | A function exceeding its maximum duration is terminated. Uncaught errors are isolated from concurrent requests, but the process may later be stopped                                        | Module state cannot be authoritative. A request can disappear after Redis or database work but before the response |
| AWS Lambda           | Execution environment    | Environments are reused and frozen between invocations; standard Lambda invokes one request per environment at a time | Timeout, runtime error, or memory failure resets the environment. Lambda can reinitialize it for a later invocation. A normal shutdown has a bounded cleanup phase                          | Test both abrupt reset and warm reuse. Never depend on shutdown cleanup for correctness                            |

## Cloudflare Workers

Cloudflare runs application code in V8 isolates. Isolates are lightweight and
can be evicted for resource limits or machine pressure. Cloudflare explicitly
advises against relying on mutable global state because requests may run in the
same or different instances.

For HTTP requests, wall-clock duration can continue while the client remains
connected. When the response completes or the client disconnects, pending work
may be canceled. `ctx.waitUntil()` can extend post-response work for up to 30
seconds, but it is not a durable job queue.

Relevant failure simulations:

- isolate eviction during a Redis request;
- client disconnect immediately after the response;
- `waitUntil()` work canceled at its platform limit;
- CPU or memory limit termination;
- two requests handled by different isolates with no shared memory.

## Vercel Fluid Compute

Vercel Fluid Compute changes the traditional one-invocation-per-instance
assumption. Multiple invocations can share a function instance concurrently,
including access to module-level resources such as connection pools. Existing
instances are preferred before new capacity is created.

The instance is still temporary. A function that exceeds its configured maximum
duration is terminated. Fluid Compute supports background work through
`waitUntil()`, but background work remains bounded by the invocation lifecycle.
Vercel also documents that an uncaught exception is isolated from other
concurrent requests, while the process may be stopped afterward.

Relevant failure simulations:

- timeout before the handler commits a claim;
- timeout after durable persistence but before claim commit;
- concurrent invocations sharing one process;
- process replacement after an uncaught error;
- response delivery failure after state has been committed.

Kismet currently runs on Vercel Fluid Compute. This makes the “persisted match,
missing commit” boundary especially important.

## AWS Lambda

Lambda describes an execution environment with `Init`, `Invoke`, and
`Shutdown` phases. After an invocation, the environment can be frozen and later
thawed for reuse. If an invocation crashes or times out, Lambda resets the
environment and may initialize it again for a later request.

Lambda's shutdown event is bounded. A process can also be terminated without
allowing application cleanup to finish. AWS documents that unfinished
background callbacks may resume if the environment is reused, which is another
reason not to use background callbacks as the source of truth.

Relevant failure simulations:

- timeout during `Invoke`;
- runtime crash or memory exhaustion;
- reset followed by a warm reuse;
- ordinary shutdown with cleanup allowed;
- shutdown cleanup forcibly cut off.

## Common contract

Across these platforms, MatchForge should assume:

1. The request may stop at any `await`.
2. The response may never reach the caller.
3. The same process may serve another request concurrently.
4. The next request may run in a different process, isolate, region, or
   execution environment.
5. In-memory cleanup may not run.
6. A durable write may succeed even when the caller observes an error.
7. A durable write may be followed by a process reset before the next state
   transition.

The coordination store and persistence store must therefore be independently
recoverable. A successful HTTP response is not a commit signal.

## Sources

- [Cloudflare: How Workers works](https://developers.cloudflare.com/workers/reference/how-workers-works/)
- [Cloudflare: Workers limits](https://developers.cloudflare.com/workers/platform/limits/)
- [Vercel: Fluid Compute](https://vercel.com/docs/fluid-compute)
- [Vercel: Configuring maximum duration](https://vercel.com/docs/functions/configuring-functions/duration)
- [AWS: Lambda execution environment lifecycle](https://docs.aws.amazon.com/lambda/latest/dg/lambda-runtime-environment.html)
