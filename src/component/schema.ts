import { defineSchema, defineTable } from "convex/server";
import { v } from "convex/values";

export default defineSchema({
  externalSources: defineTable({
    name: v.string(),
    /** URL of the external Convex deployment or API (passed at runtime, not stored as secret). */
    deploymentUrl: v.optional(v.string()),
    /** Generic destination entity identifier (string across component boundary). */
    targetEntityId: v.string(),
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

  snapshotProfiles: defineTable({
    slug: v.string(),
    name: v.string(),
    description: v.optional(v.string()),
    isActive: v.boolean(),
    version: v.number(),
    createdAt: v.number(),
    updatedAt: v.optional(v.number()),
  }).index("by_slug", ["slug"]),

  dataSources: defineTable({
    profileId: v.id("snapshotProfiles"),
    sourceKey: v.string(),
    label: v.string(),
    sourceKind: v.union(
      v.literal("component_table"),
      v.literal("external_reader"),
      v.literal("materialized_rows")
    ),
    metadata: v.optional(v.any()),
    enabled: v.boolean(),
    createdAt: v.number(),
    updatedAt: v.optional(v.number()),
  })
    .index("by_profile", ["profileId"])
    .index("by_profile_and_source_key", ["profileId", "sourceKey"]),

  indicators: defineTable({
    profileId: v.id("snapshotProfiles"),
    slug: v.string(),
    label: v.string(),
    unit: v.optional(v.string()),
    category: v.optional(v.string()),
    description: v.optional(v.string()),
    externalId: v.optional(v.string()),
    enabled: v.boolean(),
    createdAt: v.number(),
    updatedAt: v.optional(v.number()),
  })
    .index("by_profile", ["profileId"])
    .index("by_profile_and_slug", ["profileId", "slug"])
    .index("by_external_id", ["externalId"]),

  calculationDefinitions: defineTable({
    profileId: v.id("snapshotProfiles"),
    indicatorId: v.id("indicators"),
    dataSourceId: v.id("dataSources"),
    operation: v.union(
      v.literal("sum"),
      v.literal("count"),
      v.literal("avg"),
      v.literal("min"),
      v.literal("max"),
      v.literal("distinct_count")
    ),
    fieldPath: v.optional(v.string()),
    filters: v.optional(v.any()),
    groupBy: v.optional(v.array(v.string())),
    normalization: v.optional(v.any()),
    priority: v.number(),
    enabled: v.boolean(),
    ruleVersion: v.number(),
    createdAt: v.number(),
    updatedAt: v.optional(v.number()),
  })
    .index("by_profile", ["profileId"])
    .index("by_profile_and_priority", ["profileId", "priority"])
    .index("by_profile_and_indicator", ["profileId", "indicatorId"]),

  sourceRows: defineTable({
    profileId: v.id("snapshotProfiles"),
    dataSourceId: v.id("dataSources"),
    occurredAt: v.number(),
    rowData: v.any(),
    ingestedAt: v.number(),
  })
    .index("by_profile", ["profileId"])
    .index("by_data_source", ["dataSourceId"])
    .index("by_data_source_and_occurred_at", ["dataSourceId", "occurredAt"]),

  values: defineTable({
    indicatorId: v.id("indicators"),
    externalId: v.optional(v.string()),
    value: v.number(),
    measuredAt: v.number(),
    sourceRowId: v.optional(v.id("sourceRows")),
    rawPayload: v.optional(v.any()),
    createdAt: v.number(),
  })
    .index("by_indicator", ["indicatorId"])
    .index("by_indicator_and_measured_at", ["indicatorId", "measuredAt"])
    .index("by_external_id", ["externalId"]),

  snapshots: defineTable({
    profileId: v.id("snapshotProfiles"),
    snapshotAt: v.number(),
    status: v.union(
      v.literal("pending"),
      v.literal("running"),
      v.literal("success"),
      v.literal("error")
    ),
    note: v.optional(v.string()),
    triggeredBy: v.optional(v.string()),
    createdAt: v.number(),
    finishedAt: v.optional(v.number()),
    errorMessage: v.optional(v.string()),
  })
    .index("by_profile", ["profileId"])
    .index("by_profile_and_snapshot_at", ["profileId", "snapshotAt"]),

  snapshotRuns: defineTable({
    snapshotId: v.id("snapshots"),
    profileId: v.id("snapshotProfiles"),
    startedAt: v.number(),
    finishedAt: v.optional(v.number()),
    status: v.union(v.literal("running"), v.literal("success"), v.literal("error")),
    errorMessage: v.optional(v.string()),
    triggeredBy: v.optional(v.string()),
    definitionsCount: v.number(),
    processedCount: v.number(),
  })
    .index("by_snapshot", ["snapshotId"])
    .index("by_profile", ["profileId"]),

  snapshotRunItems: defineTable({
    snapshotRunId: v.id("snapshotRuns"),
    snapshotId: v.id("snapshots"),
    profileId: v.id("snapshotProfiles"),
    definitionId: v.id("calculationDefinitions"),
    indicatorId: v.id("indicators"),
    dataSourceId: v.id("dataSources"),
    status: v.union(v.literal("success"), v.literal("skipped"), v.literal("error")),
    inputCount: v.number(),
    rawResult: v.optional(v.number()),
    normalizedResult: v.optional(v.number()),
    durationMs: v.number(),
    errorMessage: v.optional(v.string()),
    warningMessage: v.optional(v.string()),
    ruleHash: v.string(),
    createdAt: v.number(),
  })
    .index("by_snapshot_run", ["snapshotRunId"])
    .index("by_snapshot", ["snapshotId"])
    .index("by_definition", ["definitionId"]),

  snapshotValues: defineTable({
    snapshotId: v.id("snapshots"),
    snapshotRunId: v.id("snapshotRuns"),
    snapshotRunItemId: v.id("snapshotRunItems"),
    profileId: v.id("snapshotProfiles"),
    indicatorId: v.id("indicators"),
    indicatorLabelSnapshot: v.string(),
    value: v.number(),
    computedAt: v.number(),
    ruleHash: v.string(),
    explainRef: v.optional(v.string()),
  })
    .index("by_snapshot", ["snapshotId"])
    .index("by_snapshot_and_indicator", ["snapshotId", "indicatorId"])
    .index("by_indicator", ["indicatorId"]),

  calculationTraces: defineTable({
    snapshotRunId: v.id("snapshotRuns"),
    snapshotRunItemId: v.id("snapshotRunItems"),
    profileId: v.id("snapshotProfiles"),
    definitionId: v.id("calculationDefinitions"),
    queryParams: v.optional(v.any()),
    resolvedFilters: v.optional(v.any()),
    sampleRowIds: v.optional(v.array(v.id("sourceRows"))),
    warnings: v.optional(v.array(v.string())),
    notes: v.optional(v.string()),
    createdAt: v.number(),
  })
    .index("by_snapshot_run", ["snapshotRunId"])
    .index("by_snapshot_run_item", ["snapshotRunItemId"]),
});
