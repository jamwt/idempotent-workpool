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
import { v } from "convex/values";
import { GenericId } from "convex/values";
import { LogLevel, RunResult, runResult } from "../component/schema.js";
import { JobStatus, Stats } from "../component/public.js";
import { getDefaultLogLevel } from "../component/logger.js";
import { ActionCtx } from "../component/_generated/server.js";

export type RunId = string & { __isRunId: true };
export const RunIdValidator = v.string();

export type BaseOptions = {
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

export type Options = BaseOptions & {
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

export type RunOptions = BaseOptions & {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  onComplete?: FunctionReference<"mutation", any, { result: RunResult }, any>;
  /**
   * An annotation for the run. This will be logged for telemetry.
   */
  annotation?: string;
};

const DEFAULT_INITIAL_BACKOFF_MS = 250;
const DEFAULT_BASE = 2;
const DEFAULT_MAX_FAILURES = 4;
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
      initialBackoffMs: options?.initialBackoffMs ?? DEFAULT_INITIAL_BACKOFF_MS,
      base: options?.base ?? DEFAULT_BASE,
      maxRetries: options?.maxRetries ?? DEFAULT_MAX_FAILURES,
      logLevel: options?.logLevel ?? DEFAULT_LOG_LEVEL,
      maxParallelism: options?.maxParallelism,
      statsWindowMs: options?.statsWindowMs ?? 0,
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
    if (options?.onComplete) {
      onComplete = await createFunctionHandle(options.onComplete);
    }
    const runId = await ctx.runMutation(this.component.public.start, {
      functionHandle: handle,
      functionName,
      functionArgs: args ?? {},
      options: {
        initialBackoffMs:
          options?.initialBackoffMs ?? this.options.initialBackoffMs,
        base: options?.base ?? this.options.base,
        maxRetries: options?.maxRetries ?? this.options.maxRetries,
        logLevel: this.options.logLevel,
        maxParallelism: this.options.maxParallelism,
        onComplete,
        annotation: options?.annotation,
      },
    });
    return runId as RunId;
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
