import {
  createFunctionHandle,
  Expand,
  FunctionArgs,
  FunctionReference,
  GenericDataModel,
  GenericMutationCtx,
  GenericQueryCtx,
  getFunctionName,
} from "convex/server";
import { api } from "../component/_generated/api.js";
import { Infer, v, VString } from "convex/values";
import { GenericId } from "convex/values";
import { LogLevel, runResult } from "../component/schema.js";
import { JobStatus, Stats } from "../component/public.js";
import { getDefaultLogLevel } from "../component/logger.js";
import { ActionCtx } from "../component/_generated/server.js";

export type RunId = string & { __isRunId: true };
export const runIdValidator = v.string() as VString<RunId>;
export const onCompleteValidator = v.object({
  runId: runIdValidator,
  context: v.any(),
  result: runResult,
});

export type GlobalOptions = {
  /**
   * The maximum number of actions to run in parallel.
   */
  maxParallelism: number;
  /**
   * The number of milliseconds to look back for statistics. Defaults to 5 minutes.
   */
  statsWindowMs?: number;
  /**
   * The log level for the workpool. Defaults to `INFO`.
   */
  logLevel?: LogLevel;
};

export type RetryOptions = {
  /**
   * Initial delay before retrying a failure, in milliseconds. Defaults to 250ms.
   */
  initialBackoffMs?: number;
  /**
   * Base for the exponential backoff. Defaults to 2.
   */
  base?: number;
  /**
   * The maximum number of times to retry failures before giving up. Defaults to 4.
   */
  maxRetries?: number;
};

export type Options = GlobalOptions & RetryOptions;

export type CallbackOptions = {
  onComplete?: FunctionReference<
    "mutation",
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    any,
    Infer<typeof onCompleteValidator>,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    any
  > | null;

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  context?: any | null;
};

export type RunOptions = RetryOptions &
  CallbackOptions & {
    /**
     * The initial delay before the first run. Defaults to 0.
     */
    initialDelayMs?: number;

    /**
     * An annotation for the run. This will be logged for telemetry.
     */
    annotation?: string | null;
  };

export type RunOnceOptions = CallbackOptions;

const DEFAULT_INITIAL_BACKOFF_MS = 250;
const DEFAULT_BASE = 2;
const DEFAULT_MAX_RETRIES = 4;

function defaultRunOptions(
  baseline: Required<Options>,
  options?: RunOptions
): Required<RunOptions> {
  return {
    initialBackoffMs: options?.initialBackoffMs ?? baseline.initialBackoffMs,
    base: options?.base ?? baseline.base,
    maxRetries: options?.maxRetries ?? baseline.maxRetries,
    onComplete: options?.onComplete ?? null,
    annotation: options?.annotation ?? null,
    initialDelayMs: options?.initialDelayMs ?? 0,
    context: options?.context,
  };
}

function runOnceToRunOptions(
  baseline: Required<Options>,
  options?: RunOnceOptions
): Required<RunOptions> {
  const runOptions = defaultRunOptions(baseline);
  runOptions.onComplete = options?.onComplete ?? null;
  runOptions.context = options?.context;
  // No retries!
  runOptions.maxRetries = 0;
  return runOptions;
}

export class IdempotentWorkpool {
  options: Required<Options>;

  /**
   * Create a new IdempotentWorkpool, which retries failed actions with exponential backoff.
   * ```ts
   * import { components } from "./_generated/server"
   * const idempotentWorkpool = new IdempotentWorkpool(components.idempotentWorkpool)
   *
   * // In a mutation or action...
   * await idempotentWorkpool.run(ctx, internal.module.myAction, { arg: 123 });
   * ```
   *
   * @param component - The registered idempotent workpool from `components`.
   * @param options - Optional overrides for the default backoff and retry behavior.
   */
  constructor(
    private component: UseApi<typeof api>,
    options: Options
  ) {
    const DEFAULT_LOG_LEVEL = getDefaultLogLevel();

    this.options = {
      initialBackoffMs: options.initialBackoffMs ?? DEFAULT_INITIAL_BACKOFF_MS,
      base: options.base ?? DEFAULT_BASE,
      maxRetries: options.maxRetries ?? DEFAULT_MAX_RETRIES,
      logLevel: options.logLevel ?? DEFAULT_LOG_LEVEL,
      maxParallelism: options.maxParallelism,
      statsWindowMs: options.statsWindowMs ?? 0,
    };
  }

  /**
   * Run an action with retries, optionally with an `onComplete` mutation callback.
   *
   * @param ctx - The context object from your mutation or action.
   * @param reference - The function reference to run, e.g., `internal.module.myAction`.
   * @param args - Arguments for the action, e.g., `{ arg: 123 }`.
   * @param options.initialBackoffMs - Optional override for the default initial backoff on failure.
   * @param options.base - Optional override for the default base for the exponential backoff.
   * @param options.maxRetries - Optional override for the default maximum number of retries.
   * @param options.onComplete - Optional mutation to run after the function succeeds, fails,
   * @param options.annotation - Optional annotation for the run.
   * @param options.initialDelayMs - Optional initial delay before the first run.
   * @param options.context - Optional context object to pass to the function.
   * or is canceled. This function must take in a single `result` argument of type `RunResult`: use
   * `runResultValidator` to validate this argument.
   * @returns - A `RunId` for the run that can be used to query its status below.
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  async run<F extends FunctionReference<"action", any, any, any>>(
    ctx: RunMutationCtx,
    reference: F,
    args?: FunctionArgs<F>,
    options?: RunOptions
  ): Promise<RunId> {
    const handle = await createFunctionHandle(reference);
    const functionName = getFunctionName(reference);
    let onComplete: string | undefined;
    const finalOptions = defaultRunOptions(this.options, options);
    if (finalOptions.onComplete) {
      onComplete = await createFunctionHandle(finalOptions.onComplete);
    }
    const runId = await ctx.runMutation(this.component.public.start, {
      functionHandle: handle,
      functionName,
      functionArgs: args ?? {},
      options: {
        ...finalOptions,
        onComplete,
        logLevel: this.options.logLevel,
        maxParallelism: this.options.maxParallelism,
      },
    });
    return runId as RunId;
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  async runAfter<F extends FunctionReference<"action", any, any, any>>(
    ctx: RunMutationCtx,
    delayMs: number,
    reference: F,
    args?: FunctionArgs<F>,
    options?: RunOptions
  ): Promise<RunId> {
    const finalOptions = defaultRunOptions(this.options, options);
    finalOptions.initialDelayMs = delayMs;
    return (await this.run(ctx, reference, args, finalOptions)) as RunId;
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  async runAt<F extends FunctionReference<"action", any, any, any>>(
    ctx: RunMutationCtx,
    atMs: number,
    reference: F,
    args?: FunctionArgs<F>,
    options?: RunOptions
  ): Promise<RunId> {
    const finalOptions = defaultRunOptions(this.options, options);
    const now = Date.now();
    const delayMs = Math.max(0, atMs - now);
    finalOptions.initialDelayMs = delayMs;
    return (await this.run(ctx, reference, args, finalOptions)) as RunId;
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  async runOnce<F extends FunctionReference<"action", any, any, any>>(
    ctx: RunMutationCtx,
    reference: F,
    args?: FunctionArgs<F>,
    options?: RunOnceOptions
  ): Promise<RunId> {
    const runOptions = runOnceToRunOptions(this.options, options);
    return (await this.run(ctx, reference, args, runOptions)) as RunId;
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  async runOnceAfter<F extends FunctionReference<"action", any, any, any>>(
    ctx: RunMutationCtx,
    delayMs: number,
    reference: F,
    args?: FunctionArgs<F>,
    options?: RunOnceOptions
  ): Promise<RunId> {
    const runOptions = runOnceToRunOptions(this.options, options);
    runOptions.initialDelayMs = delayMs;
    return (await this.run(ctx, reference, args, runOptions)) as RunId;
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  async runOnceAt<F extends FunctionReference<"action", any, any, any>>(
    ctx: RunMutationCtx,
    atMs: number,
    reference: F,
    args?: FunctionArgs<F>,
    options?: RunOnceOptions
  ): Promise<RunId> {
    const runOptions = runOnceToRunOptions(this.options, options);
    const now = Date.now();
    const delayMs = Math.max(0, atMs - now);
    runOptions.initialDelayMs = delayMs;
    return (await this.run(ctx, reference, args, runOptions)) as RunId;
  }

  /**
   * Attempt to cancel a run. This method throws if the run isn't currently executing.
   * If the run is currently executing (and not waiting for retry), action execution may
   * continue after this method successfully returns.
   *
   * @param ctx - The context object from your mutation or action.
   * @param runId - The `RunId` returned from `run`.
   */
  async cancel(ctx: RunMutationCtx, runId: RunId) {
    await ctx.runMutation(this.component.public.cancel, { runId });
  }

  /**
   * Attempt to cancel all runs.
   *
   * @param ctx - The context object from your mutation or action.
   */
  async cancelAll(ctx: ActionCtx) {
    await ctx.runAction(this.component.public.cancelAll, {});
  }

  /**
   * Get statistics about the number of runs and their statuses.
   *
   * @param ctx - The context object from your mutation or action.
   */
  async stats(ctx: RunQueryCtx): Promise<Stats> {
    return ctx.runQuery(this.component.public.stats, {
      statsWindowMs: this.options.statsWindowMs,
    });
  }

  /**
   * Log stats to the console.
   *
   * @param ctx - The context object from your mutation or action.
   */
  async logStats(ctx: RunMutationCtx) {
    await ctx.runMutation(this.component.public.logStats, {
      statsWindowMs: this.options.statsWindowMs,
    });
  }

  /**
   * Get the status of a run.
   *
   * @param ctx - The context object from your mutation or action.
   * @param runId - The `RunId` returned from `run`.
   */
  async status(ctx: RunQueryCtx, runId: RunId): Promise<JobStatus> {
    return ctx.runQuery(this.component.public.status, { runId });
  }
}

/**
 * Validator for the `result` argument of the `onComplete` callback.
 */
export const runResultValidator = runResult;

type UseApi<API> = Expand<{
  [mod in keyof API]: API[mod] extends FunctionReference<
    infer FType,
    "public",
    infer FArgs,
    infer FReturnType,
    infer FComponentPath
  >
    ? FunctionReference<
        FType,
        "internal",
        OpaqueIds<FArgs>,
        OpaqueIds<FReturnType>,
        FComponentPath
      >
    : UseApi<API[mod]>;
}>;

type OpaqueIds<T> =
  T extends GenericId<infer _T>
    ? string
    : T extends (infer U)[]
      ? OpaqueIds<U>[]
      : T extends object
        ? { [K in keyof T]: OpaqueIds<T[K]> }
        : T;

type RunQueryCtx = {
  runQuery: GenericQueryCtx<GenericDataModel>["runQuery"];
};

type RunMutationCtx = {
  runMutation: GenericMutationCtx<GenericDataModel>["runMutation"];
};
