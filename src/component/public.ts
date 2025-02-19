import {
  action,
  internalQuery,
  mutation,
  query,
  QueryCtx,
} from "./_generated/server.js";
import { Infer, v } from "convex/values";
import { Options, options } from "./schema.js";
import { createLogger, getDefaultLogLevel } from "./logger.js";
import {
  enqueueCancellation,
  enqueueIncoming,
  fromWheelSegment,
} from "./lib.js";
import { getErrorStats } from "./stats.js";
import { api, internal } from "./_generated/api.js";

const MAX_MAX_PARALLELISM = 100;

function validateOptions(options: Options) {
  if (
    options.maxParallelism <= 0 ||
    options.maxParallelism > MAX_MAX_PARALLELISM
  ) {
    throw new Error(
      `maxParallelism must be between 1 and ${MAX_MAX_PARALLELISM}`
    );
  }
  if (options.initialBackoffMs < 25) {
    throw new Error("initialBackoffMs must be >= 25");
  }
  if (options.base < 1) {
    throw new Error("base must be >= 1");
  }
  if (options.maxRetries < 0) {
    throw new Error("maxRetries must be >= 0");
  }
  if (
    options.logLevel !== "DEBUG" &&
    options.logLevel !== "INFO" &&
    options.logLevel !== "WARN" &&
    options.logLevel !== "ERROR"
  ) {
    throw new Error("logLevel must be one of: DEBUG, INFO, WARN, ERROR");
  }
}

export const start = mutation({
  args: {
    functionHandle: v.string(),
    functionName: v.string(),
    functionArgs: v.any(),
    options: v.object(options),
  },
  returns: v.id("incoming"),
  handler: async (ctx, args) => {
    validateOptions(args.options);
    const logger = createLogger(args.options.logLevel);
    const id = await enqueueIncoming(logger, ctx, {
      options: args.options,
      handle: args.functionHandle,
      arguments: args.functionArgs,
    });
    logger.event("job-start", {
      functionHandle: args.functionHandle,
      functionName: args.functionName,
      annotation: args.options.annotation,
      jobId: id,
      options: {
        base: args.options.base,
        initialBackoffMs: args.options.initialBackoffMs,
        maxRetries: args.options.maxRetries,
      },
    });
    return id;
  },
});

export const cancel = mutation({
  args: {
    runId: v.id("incoming"),
  },
  handler: async (ctx, args) => {
    const logger = createLogger(getDefaultLogLevel());
    await enqueueCancellation(logger, ctx, {
      job: args.runId,
    });
    return true;
  },
});

export const StatsValidator = v.object({
  /**
   * Number of runs currently in the pending state
   */
  pending: v.number(),

  /**
   * Time in milliseconds since the oldest pending run was created
   */
  oldestPendingMs: v.number(),

  /**
   *
   */
  recentExecutions: v.number(),

  /**
   * Ratio of runs with errors in the recent sample (0-1)
   * Calculated from the last ${SAMPLE_SIZE} runs
   */
  recentErrorRate: v.number(),

  /**
   * Ratio of runs that ended in permanent failure in the recent sample (0-1)
   * Calculated from the last ${SAMPLE_SIZE} completed runs
   */
  recentPermanentFailureRate: v.number(),

  /**
   * Average time taken to complete jobs in the recent sample.
   */
  recentJobAverageMs: v.number(),
});
export type Stats = Infer<typeof StatsValidator>;

const DEFAULT_STATS_WINDOW_MS = 5 * 60 * 1000; // 5 minutes

export const stats = query({
  args: { statsWindowMs: v.optional(v.number()) },
  returns: StatsValidator,
  handler: async (ctx, args) => {
    return getStats(ctx, args.statsWindowMs);
  },
});

async function getStats(ctx: QueryCtx, statsWindowMs: number | undefined) {
  const { runErrors, runTotal, jobErrors, jobTotal, jobTotalSum } =
    await getErrorStats(ctx, statsWindowMs ?? DEFAULT_STATS_WINDOW_MS);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const numIncoming = await (ctx.db.query("incoming") as any).count();
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const numCommitted = await (ctx.db.query("committed") as any).count();
  let oldestCreated = 0;
  if (numIncoming) {
    oldestCreated = (await ctx.db.query("incoming").first())!._creationTime;
  } else if (numCommitted) {
    oldestCreated = (await ctx.db.query("committed").first())!._creationTime;
  }
  const pending = numIncoming + numCommitted;
  const now = Date.now();
  return {
    pending,
    recentExecutions: runTotal,
    recentErrorRate: runTotal === 0 ? 0 : runErrors / runTotal,
    recentPermanentFailureRate: jobTotal === 0 ? 0 : jobErrors / jobTotal,
    recentJobAverageMs: jobTotalSum === 0 ? 0 : jobTotalSum / jobTotal,
    oldestPendingMs: oldestCreated ? now - oldestCreated : 0,
  };
}

export const cancelAll = action({
  args: {},
  handler: async (ctx) => {
    const logger = createLogger(getDefaultLogLevel());
    let runs;
    const firstPage = await ctx.runQuery(internal.public.activePage, {
      reverse: true,
    });
    if (firstPage.length === 0) {
      logger.info("No runs to cancel");
      return;
    }
    const end = firstPage[0];
    let cursor;
    logger.info(`Cancelling runs up to ${end}`);
    let total = 0;
    do {
      runs = await ctx.runQuery(internal.public.activePage, {
        reverse: false,
        start: cursor,
      });
      for (const run of runs) {
        await ctx.runMutation(api.public.cancel, { runId: run });
      }
      logger.info(` ... Cancelled ${runs.length} runs`);
      total += runs.length;
      cursor = runs[runs.length - 1];
    } while (runs.length > 0 && cursor < end);
    logger.info(`Cancelled ${total} total runs`);
  },
});

const CANCEL_PAGE_SIZE = 50;
export const activePage = internalQuery({
  args: { reverse: v.boolean(), start: v.optional(v.id("incoming")) },
  returns: v.array(v.id("incoming")),
  handler: async (ctx, args) => {
    const tableQuery = ctx.db.query("committed");

    const indexedQuery =
      args.start !== undefined
        ? tableQuery.withIndex("by_incoming", (q) =>
            q.eq("canceled", false).gt("incomingId", args.start!)
          )
        : tableQuery.withIndex("by_incoming", (q) => q.eq("canceled", false));
    const page = await indexedQuery
      .order(args.reverse ? "desc" : "asc")
      .take(CANCEL_PAGE_SIZE);
    return page.map((doc) => doc.incomingId);
  },
});

export const logStats = mutation({
  args: { statsWindowMs: v.optional(v.number()) },
  handler: async (ctx, args) => {
    const stats = await getStats(ctx, args.statsWindowMs);
    createLogger("INFO").event("stats", stats);
  },
});

const statusValidator = v.object({
  kind: v.union(
    v.literal("enqueued"),
    v.literal("scheduled"),
    v.literal("running"),
    v.literal("canceled"),
    v.literal("unknown")
  ),
  retries: v.optional(v.number()),
  nextRun: v.optional(v.number()),
});
export type JobStatus = {
  kind: "enqueued" | "scheduled" | "running" | "canceled" | "unknown";
  retries?: number;
  nextRun?: number;
};

export const status = query({
  args: { runId: v.id("incoming") },
  returns: statusValidator,
  handler: async (ctx, args): Promise<JobStatus> => {
    // First, check incoming.
    const incoming = await ctx.db.get(args.runId);
    if (incoming) {
      return { kind: "enqueued" };
    }
    // Next, check committed.
    const committed = await ctx.db
      .query("committed")
      .withIndex("by_incoming", (q) =>
        q.eq("canceled", false).eq("incomingId", args.runId)
      )
      .first();
    if (committed) {
      const running = await ctx.db
        .query("running")
        .withIndex("by_job", (q) => q.eq("job", committed._id))
        .first();
      if (running) {
        return { kind: "running", retries: committed.retries };
      } else {
        const wheel = await ctx.db
          .query("wheel")
          .withIndex("by_job", (q) => q.eq("job", committed._id))
          .first();
        if (!wheel) {
          throw new Error("Invariant: Wheel not found for scheduled job?");
        }
        const nextRun = fromWheelSegment(wheel.segment);
        return { kind: "scheduled", retries: committed.retries, nextRun };
      }
    } else {
      const committedCanceled = await ctx.db
        .query("committed")
        .withIndex("by_incoming", (q) =>
          q.eq("canceled", true).eq("incomingId", args.runId)
        )
        .first();
      if (committedCanceled) {
        return { kind: "canceled" };
      }
      return { kind: "unknown" };
    }
  },
});
