import { defineApp } from "convex/server";
import idempotentWorkpool from "@convex-dev/idempotent-workpool/convex.config";

const app = defineApp();
app.use(idempotentWorkpool);
export default app;
