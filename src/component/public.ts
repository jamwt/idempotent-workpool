import { action, internalQuery, mutation, query } from "./_generated/server.js";
import { Infer, v } from "convex/values";
import { options } from "./schema.js";
import { createLogger, getDefaultLogLevel } from "./logger.js";
import { enqueueCancellation, enqueueIncoming } from "./lib.js";
import { getErrorStats } from "./stats.js";
import { api, internal } from "./_generated/api.js";

export const start = mutation({
  args: {
    functionHandle: v.string(),
    functionArgs: v.any(),
    options: v.object(options),
  },
  returns: v.string(),
  handler: async (ctx, args) => {
    const logger = createLogger(args.options.logLevel);
    const id = await enqueueIncoming(logger, ctx, {
      options: args.options,
      handle: args.functionHandle,
      arguments: args.functionArgs,
    });
    return id as string;
  },
});

export const cancel = mutation({
  args: {
    runId: v.string(),
  },
  handler: async (ctx, args) => {
    const logger = createLogger(getDefaultLogLevel());
    const id = ctx.db.normalizeId("incoming", args.runId);
    if (!id) {
      return false;
    }
    await enqueueCancellation(logger, ctx, {
      job: id,
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
  oldestPending: v.number(),

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
});
export type Stats = Infer<typeof StatsValidator>;

const DEFAULT_STATS_WINDOW_MS = 5 * 60 * 1000; // 5 minutes

export const stats = query({
  args: {
    statsWindowMs: v.optional(v.number()),
  },
  returns: StatsValidator,
  handler: async (ctx, args) => {
    const { runErrors, runTotal, jobErrors, jobTotal } = await getErrorStats(
      ctx,
      args.statsWindowMs ?? DEFAULT_STATS_WINDOW_MS
    );
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
      oldestPending: oldestCreated ? now - oldestCreated : 0,
    };
  },
});

// export const reset = internalAction({
//   args: {},
//   handler: async (ctx) => {
//     let deleted;
//     do {
//       deleted = await ctx.runMutation(internal.public.deletePage, {});
//       console.log(`Deleted ${deleted} runs`);
//     } while (deleted > 0);

//     // Reset all components.
//     await ctx.runMutation(internal.utils.clearErrorStats);
//     await ctx.runMutation(internal.heartbeat.resetRegistry);
//   },
// });

// export const deletePage = internalMutation({
//   args: {},
//   handler: async (ctx, args) => {
//     const runs = await ctx.db.query("runs").take(100);
//     for (const run of runs) {
//       await ctx.db.delete(run._id);
//     }
//     return runs.length;
//   },
// });

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
    const stats = await ctx.runQuery(api.public.stats, {
      statsWindowMs: args.statsWindowMs,
    });
    createLogger("INFO").event("stats", stats);
  },
});
