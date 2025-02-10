import { cronJobs } from "convex/server";
import { internal } from "./_generated/api.js";

const crons = cronJobs();
crons.interval("Recovery loop", { seconds: 10 }, internal.mainLoop.recover, {});
export default crons;
