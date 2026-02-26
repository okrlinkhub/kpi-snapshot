import { defineSchema, defineTable } from "convex/server";
import { v } from "convex/values";

export default defineSchema({
  /** Mock writer: values "received" by the example app when testing syncToLinkHub (simulates LinkHub). */
  indicatorValueLog: defineTable({
    companyId: v.string(),
    indicatorSlug: v.string(),
    value: v.number(),
    date: v.number(),
  }).index("by_company", ["companyId"]),
});
