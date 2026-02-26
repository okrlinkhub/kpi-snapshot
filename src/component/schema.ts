import { defineSchema, defineTable } from "convex/server";
import { v } from "convex/values";

export default defineSchema({
  externalSources: defineTable({
    name: v.string(),
    /** URL of the external Convex deployment or API (passed at runtime, not stored as secret). */
    deploymentUrl: v.optional(v.string()),
    /** LinkHub company identifier (string across component boundary). */
    linkHubCompanyId: v.string(),
    authType: v.optional(v.string()),
    createdAt: v.number(),
    updatedAt: v.optional(v.number()),
  }),
  syncRuns: defineTable({
    externalSourceId: v.id("externalSources"),
    startedAt: v.number(),
    finishedAt: v.optional(v.number()),
    status: v.string(),
    errorMessage: v.optional(v.string()),
    valuesSynced: v.optional(v.number()),
  }).index("by_external_source", ["externalSourceId"]),
  indicatorSnapshots: defineTable({
    externalSourceId: v.id("externalSources"),
    indicatorKey: v.string(),
    value: v.number(),
    date: v.number(),
    syncedAt: v.optional(v.number()),
    rawPayload: v.optional(v.any()),
  })
    .index("by_external_source", ["externalSourceId"])
    .index("by_external_source_and_key", ["externalSourceId", "indicatorKey"]),
});
