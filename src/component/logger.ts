import { v } from "convex/values";
import { LogLevel } from "./schema";
import { internalMutation } from "./_generated/server";

/* eslint-disable @typescript-eslint/no-explicit-any */
export type Logger = {
  debug: (...args: unknown[]) => void;
  info: (...args: unknown[]) => void;
  warn: (...args: unknown[]) => void;
  error: (...args: unknown[]) => void;
  event: (event: string, payload: Record<string, any>) => void;
};

export function createLogger(level: LogLevel): Logger {
  const levelIndex = ["DEBUG", "INFO", "WARN", "ERROR"].indexOf(level);
  if (levelIndex === -1) {
    throw new Error(`Invalid log level: ${level}`);
  }
  return {
    debug: (...args: unknown[]) => {
      if (levelIndex <= 0) {
        console.debug(...args);
      }
    },
    info: (...args: unknown[]) => {
      if (levelIndex <= 1) {
        console.info(...args);
      }
    },
    warn: (...args: unknown[]) => {
      if (levelIndex <= 2) {
        console.warn(...args);
      }
    },
    error: (...args: unknown[]) => {
      if (levelIndex <= 3) {
        console.error(...args);
      }
    },
    event: (event: string, payload: Record<string, any>) => {
      if (levelIndex <= 1) {
        const fullPayload = {
          system: "idempotent-workpool-component",
          event,
          payload,
        };
        console.info(fullPayload);
      }
    },
  };
}

export function getDefaultLogLevel(): LogLevel {
  let DEFAULT_LOG_LEVEL: LogLevel = "INFO";
  if (process.env.IDEMPOTENT_WORKPOOL_LOG_LEVEL) {
    if (
      !["DEBUG", "INFO", "WARN", "ERROR"].includes(
        process.env.IDEMPOTENT_WORKPOOL_LOG_LEVEL
      )
    ) {
      console.warn(
        `Invalid log level (${process.env.IDEMPOTENT_WORKPOOL_LOG_LEVEL}), defaulting to "INFO"`
      );
    } else {
      DEFAULT_LOG_LEVEL = process.env.IDEMPOTENT_WORKPOOL_LOG_LEVEL as LogLevel;
    }
  }
  return DEFAULT_LOG_LEVEL;
}

export const debugOverrideLogLevel = internalMutation({
  args: {
    level: v.union(
      v.literal("DEBUG"),
      v.literal("INFO"),
      v.literal("WARN"),
      v.literal("ERROR")
    ),
  },
  handler: async (ctx, args) => {
    const frozen = await ctx.db.query("frozenConfig").first();
    if (frozen) {
      await ctx.db.patch(frozen._id, {
        config: {
          maxParallelism: frozen.config.maxParallelism,
          logLevel: args.level,
        },
      });
    } else {
      throw Error("No existing config to patch.");
    }
  },
});
