import { defineComponent } from "convex/server";
import shardedCounter from "@convex-dev/sharded-counter/convex.config";
import aggregate from "@convex-dev/aggregate/convex.config";

const component = defineComponent("idempotentWorkpool");
component.use(shardedCounter);
component.use(aggregate);
export default component;
