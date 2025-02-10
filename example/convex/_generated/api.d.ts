/* eslint-disable */
/**
 * Generated `api` utility.
 *
 * THIS CODE IS AUTOMATICALLY GENERATED.
 *
 * To regenerate, run `npx convex dev`.
 * @module
 */

import type * as example from "../example.js";

import type {
  ApiFromModules,
  FilterApi,
  FunctionReference,
} from "convex/server";
/**
 * A utility for referencing Convex functions in your app's API.
 *
 * Usage:
 * ```js
 * const myFunctionReference = api.myModule.myFunction;
 * ```
 */
declare const fullApi: ApiFromModules<{
  example: typeof example;
}>;
declare const fullApiWithMounts: typeof fullApi;

export declare const api: FilterApi<
  typeof fullApiWithMounts,
  FunctionReference<any, "public">
>;
export declare const internal: FilterApi<
  typeof fullApiWithMounts,
  FunctionReference<any, "internal">
>;

export declare const components: {
  idempotentWorkpool: {
    public: {
      cancel: FunctionReference<"mutation", "internal", { runId: string }, any>;
      cancelAll: FunctionReference<"action", "internal", {}, any>;
      logStats: FunctionReference<
        "mutation",
        "internal",
        { statsWindowMs?: number },
        any
      >;
      start: FunctionReference<
        "mutation",
        "internal",
        {
          functionArgs: any;
          functionHandle: string;
          options: {
            base: number;
            initialBackoffMs: number;
            logLevel: "DEBUG" | "INFO" | "WARN" | "ERROR";
            maxParallelism: number;
            maxRetries: number;
            onComplete?: string;
          };
        },
        string
      >;
      stats: FunctionReference<
        "query",
        "internal",
        { statsWindowMs?: number },
        {
          oldestPending: number;
          pending: number;
          recentErrorRate: number;
          recentExecutions: number;
          recentPermanentFailureRate: number;
        }
      >;
      status: FunctionReference<
        "query",
        "internal",
        { runId: string },
        {
          kind: "enqueued" | "scheduled" | "running" | "canceled" | "unknown";
          nextRun?: number;
          retries?: number;
        }
      >;
    };
  };
};
