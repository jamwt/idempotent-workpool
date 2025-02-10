# Convex Idempotent Workpool

[![npm version](https://badge.fury.io/js/@convex-dev%2Fidempotent-workpool.svg)](https://badge.fury.io/js/@convex-dev%2Fidempotent-workpool)

<!-- START: Include on https://convex.dev/components -->

Actions can sometimes fail due to network errors, server restarts, or issues with a
3rd party API, so it's often useful to retry them. The Idempotent Workpool component
provides a simple way to run idempotent actions until completion, with configurable
retry logic, while allowing you to limit how many of them run in parallel.

```ts
import { IdempotentWorkpool } from "@convex-dev/idempotent-workpool";
import { components } from "./convex/_generated/server";

// Create a new idempotent workpool with a max parallelism of 10.
const idempotentWorkpool = new IdempotentWorkpool(
  components.idempotentWorkpool,
  {
    maxParallelism: 10,
  }
);

// `idempotentWorkpool.run` will automatically retry your action upon temporary failure.
await idempotentWorkpool.run(ctx, internal.module.myAction, { arg: 123 });
```

### Idempotency?

Idempotent actions are actions that can be run multiple times safely. This typically
means they don't cause any side effects that would be a problem if executed twice or more.

As an example of an unsafe, non-idempotent action, consider an action that charges
a user's credit card without providing a unique transaction id to the payment
processor. The first time the action is run, imagine that the API call succeeds to the
payment provider, but then the action throws an exception before the transaction is marked
finished in our Convex database. If the action is run twice, the user may be
double charged for the transaction!

If we alter this action to provide a consistent transaction id to the payment provider, they
can simply NOOP the second payment attempt. The this makes the action idempotent, and
it can safely be retried.

### Workpool?

Imagine that the payment processor is a 3rd party API, and they temporarily have an
outage. Now imagine you implement your own action retrying logic for your busy app.
You'll find very quickly that your entire backend is overwhelmed with retrying actions.
This could bog down live traffic with background work, and/or cause you to exceed
rate limits with the payment provider.

Creating an upper bound on how much work will be done in parallel is a good way to
mitigate this risk. Actions that are currently backing off awaiting retry will not tie
up a thread in the workpool.

### In summary

If you're creating complex workflows with many steps involving 3rd party APIs:

1.  You should ensure that each step is an idempotent Convex action.
2.  You should use this component to manage these actions so it all just works!

## Pre-requisite: Convex

You'll need an existing Convex project to use the component.
Convex is a hosted backend platform, including a database, serverless functions,
and a ton more you can learn about [here](https://docs.convex.dev/get-started).

Run `npm create convex` or follow any of the [quickstarts](https://docs.convex.dev/home) to set one up.

## Installation

First, add `@convex-dev/idempotent-workpool` as an NPM dependency:

```sh
npm install @convex-dev/idempotent-workpool
```

Then, install the component into your Convex project within the `convex/convex.config.ts` configuration file:

```ts
// convex/convex.config.ts
import { defineApp } from "convex/server";
import idempotentWorkpool from "@convex-dev/idempotent-workpool/convex.config";

const app = defineApp();
app.use(idempotentWorkpool);
export default app;
```

Finally, create a new `IdempotentWorkpool` within your Convex project, and point it to the installed component:

```ts
// convex/index.ts
import { IdempotentWorkpool } from "@convex-dev/idempotent-workpool";
import { components } from "./_generated/api";

// Create a new idempotent workpool with a max parallelism of 10.
// This argument is required.
export const idempotentWorkpool = new IdempotentWorkpool(
  components.idempotentWorkpool,
  {
    maxParallelism: 10,
  }
);
```

You can optionally configure the idempotent workpool's backoff behavior in the `IdempotentWorkpool` constructor.

```ts
const idempotentWorkpool = new IdempotentWorkpool(
  components.idempotentWorkpool,
  {
    maxParallelism: 10,
    initialBackoffMs: 10000,
    base: 10,
    maxRetries: 4,
  }
);
```

- `initialBackoffMs` is the initial delay after a failure before retrying (default: 250).
- `base` is the base for the exponential backoff (default: 2).
- `maxRetries` is the maximum number of times to retry the action (default: 4).

## API

### Starting a run

After installing the component, use the `run` method from either a mutation or action to kick off an action.

```ts
export const kickoffExampleAction = mutation({
  handler: async (ctx) => {
    const runId = await idempotentWorkpool.run(
      ctx,
      internal.index.exampleAction,
      {
        foo: "bar",
      }
    );
    // ... optionally persist or pass along the runId
  },
});

export const exampleAction = internalAction({
  args: { foo: v.string() },
  handler: async (ctx, args) => {
    return operationThatMightFail(args);
  },
});
```

The return value of `idempotentWorkpool.run` is not the result of the action, but rather an ID that you can use to query its status or cancel it. The action's return value is saved along with the status, when it succeeds.

You can optionally specify overrides to the backoff parameters in an options argument.

```ts
export const kickoffExampleAction = action(async (ctx) => {
  const runId = await idempotentWorkpool.run(
    ctx,
    internal.index.exampleAction,
    { foo: "bar" },
    {
      initialBackoffMs: 125,
      base: 2.71,
      maxRetries: 3,
    }
  );
});
```

You can specify an `onComplete` mutation callback in the options argument as well. This mutation is guaranteed to
eventually run exactly once.

```ts
// convex/index.ts

import { runResultValidator } from "@convex-dev/idempotent-workpool";

export const kickoffExampleAction = action(async (ctx) => {
  const runId = await idempotentWorkpool.run(
    ctx,
    internal.index.exampleAction,
    { foo: "bar" },
    {
      onComplete: internal.index.exampleCallback,
    }
  );
});

export const exampleCallback = internalMutation({
  args: { result: runResultValidator },
  handler: async (ctx, args) => {
    if (args.result.type === "success") {
      console.log(
        "Action succeeded with return value:",
        args.result.returnValue
      );
    } else if (args.result.type === "failed") {
      console.log("Action failed with error:", args.result.error);
    } else if (args.result.type === "canceled") {
      console.log("Action was canceled.");
    }
  },
});
```

### Run status

The `run` method returns a `RunId`, which can then be used for querying a run's status.

```ts
export const kickoffExampleAction = action(async (ctx) => {
  const runId = await idempotentWorkpool.run(
    ctx,
    internal.index.exampleAction,
    { foo: "bar" }
  );
  while (true) {
    const status = await idempotentWorkpool.status(ctx, runId);
    if (status.type === "inProgress") {
      await new Promise((resolve) => setTimeout(resolve, 1000));
      continue;
    } else {
      console.log("Run completed with result:", status.result);
      break;
    }
  }
});
```

The `status` method returns a `JobStatus` object, which contains information about the run's status:

- `{ kind: "enqueued" }` - The run is currently enqueued and waiting to be initially scheduled.
- `{ kind: "scheduled", retries: number, nextRun: number }` - The run is scheduled to run in the future.
- `{ kind: "running", retries: number }` - The run is currently executing.
- `{ kind: "canceled" }` - The run was canceled, and will be flushed soon.
- `{ kind: "unknown" }` - The pool does not know about this run. Either it was never enqueued,
  or it has been completed and cleaned up. The pool does not retain any information about completed
  runs. Use the `onComplete` callback to handle (and potentially store) the run's result.

### Canceling a run

You can cancel a run using the `cancel` method.

```ts
export const kickoffExampleAction = action(async (ctx) => {
  const runId = await idempotentWorkpool.run(
    ctx,
    internal.index.exampleAction,
    { foo: "bar" }
  );
  await new Promise((resolve) => setTimeout(resolve, 1000));
  await idempotentWorkpool.cancel(ctx, runId);
});
```

Runs that are currently executing will be canceled best effort, so they
may still continue to execute. A cancellation request may take a few seconds to
propagate to the pool manager if the pool is busy.

## Logging

You can set the `IDEMPOTENT_WORKPOOL_LOG_LEVEL` environment variable to `DEBUG`
to have the idempotent workpool log out more of its internal information,
which you can then view on the Convex dashboard.

```sh
npx convex env set IDEMPOTENT_WORKPOOL_LOG_LEVEL DEBUG
```

The default log level is `INFO`, but you can also set it to `ERROR` for even fewer logs.

## Performance and overhead

The idempotent workpool is designed to be lightweight and to minimize overhead.
However, Convex does not yet support a table triggering mechanism, so the chattiness
of communication between jobs and the pool manager is non-zero.

As Convex ships more optimized system primitives, this implementation will be updated
to reduce the overhead of the management of the pool accordingly.

## Telemetry

The `stats()` method returns a `Stats` object containing information about the
idempotent workpool's current state.

```ts
const stats = await idempotentWorkpool.stats(ctx);
```

Fields:

- `pending`: Number of runs currently in the pending state
- `oldestPending`: Time in milliseconds since the oldest pending run was created
- `recentExecutions`: Number of recent action executions
- `recentErrorRate`: Ratio (0-1) of transient errors and retries in recent executions
- `recentPermanentFailureRate`: Ratio (0-1) of permanent failures in recent executions

You can also log stats to the console in a structured JSON format
using the `logStats` method:

```ts
await idempotentWorkpool.logStats(ctx);
```

If you generate these log events regularly from an application cron, they can be
ingested by datadog, axiom, or some other service for monitoring and alerting.

Recency is determined by the `statsWindowMs` option in the `IdempotentWorkpool` constructor.
It defaults to 5 minutes.

## History

The Idempotent Workpool component is heavily based on ideas from
[the Convex Action Retrier component](https://github.com/get-convex/action-retrier)
and [the Convex Workpool component](https://github.com/get-convex/workpool).

<!-- END: Include on https://convex.dev/components -->
