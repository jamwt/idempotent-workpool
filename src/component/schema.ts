import { defineSchema, defineTable } from "convex/server";
import { Infer, v } from "convex/values";

const logLevel = v.union(
  v.literal("DEBUG"),
  v.literal("INFO"),
  v.literal("WARN"),
  v.literal("ERROR")
);
export type LogLevel = Infer<typeof logLevel>;
export const frozenOptions = {
  logLevel,
  maxParallelism: v.number(),
};
const frozenOptionsObj = v.object(frozenOptions);
export type FrozenOptions = Infer<typeof frozenOptionsObj>;

export const options = {
  ...frozenOptions,
  initialBackoffMs: v.number(),
  base: v.number(),
  maxRetries: v.number(),
  onComplete: v.optional(v.string()),
};
const optionsObj = v.object(options);
export type Options = Infer<typeof optionsObj>;

export const runResult = v.union(
  v.object({
    type: v.literal("success"),
    returnValue: v.any(),
  }),
  v.object({
    type: v.literal("failed"),
    error: v.string(),
  }),
  v.object({
    type: v.literal("canceled"),
  })
);
export type RunResult = Infer<typeof runResult>;

export default defineSchema({
  incoming: defineTable({
    handle: v.string(),
    arguments: v.any(),
    options: optionsObj,
  }),
  cancellations: defineTable({
    job: v.id("incoming"),
  }),
  committed: defineTable({
    handle: v.string(),
    arguments: v.any(),
    retries: v.number(),
    incomingId: v.id("incoming"),
    enqueuedAt: v.number(),
    canceled: v.boolean(),
    options: optionsObj,
  }).index("by_incoming", ["canceled", "incomingId"]),
  wheel: defineTable({
    job: v.id("committed"),
    segment: v.number(),
  })
    .index("by_job", ["job"])
    .index("by_segment", ["segment"]),
  running: defineTable({
    job: v.id("committed"),
    schedulerId: v.id("_scheduled_functions"),
  }).index("by_job", ["job"]),
  ended: defineTable({
    job: v.id("committed"),
    result: runResult,
    endedAt: v.number(),
  }),
  loopState: defineTable({
    wake: v.optional(v.id("_scheduled_functions")),
    lastLivePoll: v.optional(v.number()),
  }),
  currentGeneration: defineTable({
    generation: v.number(),
    job: v.id("_scheduled_functions"),
    handle: v.string(),
  }),
  stats: defineTable({
    job: v.id("committed"),
    retry: v.number(),
    when: v.number(),
    finalRunTime: v.optional(v.number()),
    error: v.boolean(),
  }),
  frozenConfig: defineTable({
    config: frozenOptionsObj,
    frozenAt: v.number(),
  }),
});
