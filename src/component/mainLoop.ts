import { v } from "convex/values";
import { internalMutation, MutationCtx } from "./_generated/server";
import { components, internal } from "./_generated/api";
import { ShardedCounter } from "@convex-dev/sharded-counter";

const counter = new ShardedCounter(components.shardedCounter, {
  shards: {
    gen: 64,
  },
});

export const recover = internalMutation({
  args: {},
  returns: v.null(),
  handler: async (ctx) => {
    const currentGeneration = await ctx.db.query("currentGeneration").unique();
    if (currentGeneration === null) {
      return;
    }
    await ctx.scheduler.runAfter(0, internal.mainLoop.poll, {});
  },
});

export const poll = internalMutation({
  args: {
    threshold: v.optional(v.number()),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    const generation = await counter.count(ctx, "gen");
    const currentGeneration = await ctx.db.query("currentGeneration").unique();
    if (args.threshold && generation === args.threshold) {
      if (currentGeneration !== null) {
        await ctx.db.delete(currentGeneration._id);
      }
      return;
    }
    if (currentGeneration === null) {
      const job = await ctx.scheduler.runAfter(0, internal.mainLoop.runWrap, {
        generation,
      });
      await ctx.db.insert("currentGeneration", {
        generation,
        job,
      });
    } else {
      const job = currentGeneration.job;
      const status = await ctx.db.system.get(job);
      if (
        !status ||
        status.state.kind === "success" ||
        status.state.kind === "failed" ||
        status.state.kind === "canceled"
      ) {
        const newJob = await ctx.scheduler.runAfter(
          0,
          internal.mainLoop.runWrap,
          { generation }
        );
        await ctx.db.patch(currentGeneration._id, {
          generation,
          job: newJob,
        });
      }
    }
  },
});

export const runWrap = internalMutation({
  args: {
    generation: v.number(),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    await ctx.runMutation(internal.lib.loop, {});
    await ctx.scheduler.runAfter(0, internal.mainLoop.poll, {
      threshold: args.generation,
    });
  },
});

export async function trigger(ctx: MutationCtx) {
  await counter.add(ctx, "gen", 1);
  await ctx.scheduler.runAfter(0, internal.mainLoop.poll, {});
}
