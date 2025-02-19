import { v } from "convex/values";
import { components, internal } from "./_generated/api";
import { Id } from "./_generated/dataModel";
import {
  internalMutation,
  internalQuery,
  MutationCtx,
  QueryCtx,
} from "./_generated/server";
import { MainLoop } from "./mainLoop";
import { Bound, DirectAggregate } from "@convex-dev/aggregate";

const mainLoop = new MainLoop({
  handle: internal.stats.loop,
});

const STATS_WINDOW = 50;

export const loop = internalMutation({
  args: {},
  handler: async (ctx) => {
    const stats = await ctx.db.query("stats").take(STATS_WINDOW);
    for (const s of stats) {
      await updateErrorStats(ctx, s.job, s.retry, s.error, s.finalRunTime);
      await ctx.db.delete(s._id);
    }
  },
});

export async function emitStats(
  ctx: MutationCtx,
  records: Array<{
    job: Id<"committed">;
    retry: number;
    when: number;
    finalRunTime?: number;
    error: boolean;
  }>
) {
  for (const r of records) {
    await ctx.db.insert("stats", {
      job: r.job,
      retry: r.retry,
      when: r.when,
      finalRunTime: r.finalRunTime,
      error: r.error,
    });
  }
  await mainLoop.trigger(ctx);
}
const statsAggregate = new DirectAggregate<{
  Namespace: "runTotal" | "runError" | "jobTotal" | "jobFailure";
  Key: number;
  Id: string;
}>(components.aggregate);

export async function updateErrorStats(
  ctx: MutationCtx,
  run: Id<"committed">,
  retry: number,
  error: boolean,
  finalRunTime?: number
) {
  const key = Date.now();
  const id = `${run}-${retry}`;
  await statsAggregate.insert(ctx, {
    namespace: "runTotal",
    key,
    id,
  });
  if (error) {
    await statsAggregate.insert(ctx, {
      namespace: "runError",
      key,
      id,
    });
  }
  if (finalRunTime) {
    await statsAggregate.insert(ctx, {
      namespace: "jobTotal",
      key,
      id,
      sumValue: finalRunTime,
    });
    if (error) {
      await statsAggregate.insert(ctx, {
        namespace: "jobFailure",
        key,
        id,
      });
    }
  }
}

export async function getErrorStats(
  ctx: QueryCtx,
  windowMs: number
): Promise<{
  runErrors: number;
  runTotal: number;
  jobErrors: number;
  jobTotal: number;
  jobTotalSum: number;
}> {
  const now = Date.now();
  const cutoff = now - windowMs;
  const bound: Bound<number, string> = {
    key: cutoff,
    inclusive: true,
  };
  const bounds = {
    lower: bound,
  };
  const runErrors = await statsAggregate.count(ctx, {
    namespace: "runError",
    bounds,
  });
  const runTotal = await statsAggregate.count(ctx, {
    namespace: "runTotal",
    bounds,
  });
  const jobTotal = await statsAggregate.count(ctx, {
    namespace: "jobTotal",
    bounds,
  });
  const jobErrors = await statsAggregate.count(ctx, {
    namespace: "jobFailure",
    bounds,
  });
  const jobTotalSum = await statsAggregate.sum(ctx, {
    namespace: "jobTotal",
    bounds,
  });
  return { runErrors, runTotal, jobErrors, jobTotal, jobTotalSum };
}

const STATS_BTREE_NODE_SIZE = 16;

export const clearErrorStats = internalMutation({
  args: {},
  handler: async (ctx) => {
    await statsAggregate.clear(ctx, {
      maxNodeSize: STATS_BTREE_NODE_SIZE,
      namespace: "runTotal",
    });
    await statsAggregate.clear(ctx, {
      maxNodeSize: STATS_BTREE_NODE_SIZE,
      namespace: "runError",
    });
    await statsAggregate.clear(ctx, {
      maxNodeSize: STATS_BTREE_NODE_SIZE,
      namespace: "jobTotal",
    });
    await statsAggregate.clear(ctx, {
      maxNodeSize: STATS_BTREE_NODE_SIZE,
      namespace: "jobFailure",
    });
  },
});

export const debugErrorStats = internalQuery({
  args: {
    windowMs: v.number(),
  },
  handler: async (ctx, args) => {
    return await getErrorStats(ctx, args.windowMs);
  },
});
