import { defineApp } from "convex/server";
import kpiSnapshot from "@okrlinkhub/kpi-snapshot/convex.config.js";

const app = defineApp();
app.use(kpiSnapshot, { name: "kpiSnapshot" });
export default app;
