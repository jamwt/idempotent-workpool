import { v } from "convex/values";
import {
  internalAction,
  internalMutation,
  mutation,
} from "./_generated/server";
import { internal, components, api } from "./_generated/api";
import {
  IdempotentWorkpool,
  RunId,
  RunIdValidator,
  runResultValidator,
} from "@convex-dev/idempotent-workpool";

const idempotentWorkpool = new IdempotentWorkpool(
  components.idempotentWorkpool,
  {
    maxParallelism: 40,
    logLevel: "DEBUG",
  }
);

const action = v.union(
  v.literal("succeed"),
  v.literal("fail randomly"),
  v.literal("fail always")
);

// You can fetch data from and send data to third-party APIs via an action:
export const myAction = internalAction({
  args: { action },
  handler: async (_ctx, { action }) => {
    switch (action) {
      case "succeed":
        console.log("success");
        break;
      case "fail randomly":
        if (Math.random() < 0.8) {
          throw new Error("action failed.");
        }
        if (Math.random() < 0.01) {
          // Incur a timeout.
          console.log("I'm a baaaad timeout job.");
          await new Promise((resolve) => setTimeout(resolve, 15 * 60 * 1000));
        }
        console.log("action succeded.");
        break;
      case "fail always":
        throw new Error("action failed.");
      default:
        throw new Error("invalid action");
    }
  },
});

export const completion = internalMutation({
  args: {
    result: runResultValidator,
  },
  handler: async (ctx, args) => {
    console.log(args.result);
  },
});

export const kickoffMyAction = mutation({
  args: { action, initialBackoffMs: v.optional(v.number()) },
  handler: async (ctx, args) => {
    const id: any = await idempotentWorkpool.run(
      ctx,
      internal.example.myAction,
      {
        action: args.action,
      },
      {
        initialBackoffMs: args.initialBackoffMs ?? 1000,
        base: 2,
        maxRetries: 2,
        onComplete: internal.example.completion,
        //        logLevel: "DEBUG",
      }
    );
    return id;
  },
});

const EXAMPLE_RUNS = 1500;
export const runMany = internalAction({
  args: {},
  handler: async (ctx) => {
    const cancelations = [];
    for (let i = 0; i < EXAMPLE_RUNS; i++) {
      const id: string = await ctx.runMutation(api.example.kickoffMyAction, {
        action: "fail randomly",
      });
      if (Math.random() < 0.05) {
        cancelations.push(id);
      }
    }
    for (const id of cancelations) {
      await ctx.scheduler.runAfter(
        15 * Math.random() * 1000,
        internal.example.cancel,
        {
          id,
        }
      );
    }
  },
});

export const cancel = internalMutation({
  args: {
    id: RunIdValidator,
  },
  handler: async (ctx, args) => {
    console.log("Cancelling", args.id);
    await idempotentWorkpool.cancel(ctx, args.id as RunId);
  },
});

// Really test the wheel + delay by splaying out retries a bunch.
export const runSlowFailBatch = internalAction({
  args: {},
  handler: async (ctx) => {
    for (let i = 0; i < 10; i++) {
      await ctx.runMutation(api.example.kickoffMyAction, {
        action: "fail always",
        initialBackoffMs: 90000,
      });
    }
  },
});
