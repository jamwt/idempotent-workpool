import { v } from "convex/values";
import { internalMutation } from "./_generated/server";
import { components, internal } from "./_generated/api";
import { ShardedCounter } from "@convex-dev/sharded-counter";
import { FunctionHandle } from "convex/server";

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
    await ctx.scheduler.runAfter(0, internal.mainLoop.poll, {
      handle: currentGeneration.handle,
    });
  },
});

export const poll = internalMutation({
  args: {
    handle: v.string(),
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
        handle: args.handle,
        generation,
      });
      await ctx.db.insert("currentGeneration", {
        generation,
        job,
        handle: args.handle,
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
          {
            handle: args.handle,
            generation,
          }
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
    handle: v.string(),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    const mut = args.handle as FunctionHandle<
      "mutation",
      Record<never, never>,
      void
    >;
    await ctx.runMutation(mut, {});
    await ctx.scheduler.runAfter(0, internal.mainLoop.poll, {
      handle: args.handle,
      threshold: args.generation,
    });
  },
});

import { createFunctionHandle } from "convex/server";
import { FunctionReference } from "convex/server";
import { MutationCtx } from "./_generated/server";

export type Options = { handle: FunctionReference<"mutation", "internal"> };

export class MainLoop {
  constructor(public options: Options) {}

  public async trigger(ctx: MutationCtx) {
    const handle = await createFunctionHandle(this.options.handle);
    await counter.add(ctx, "gen", 1);
    await ctx.scheduler.runAfter(0, internal.mainLoop.poll, { handle });
  }
}
