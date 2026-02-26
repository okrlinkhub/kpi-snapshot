import { defineApp } from "convex/server";
import kpiSnapshot from "@kpi-snapshot/component/convex.config.js";

const app = defineApp();
app.use(kpiSnapshot, { name: "kpiSnapshot" });
export default app;
