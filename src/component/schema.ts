import { defineSchema, defineTable } from "convex/server";
import { v } from "convex/values";

export default defineSchema({
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
    adapterKey: v.optional(v.string()),
    sourceKind: v.union(
      v.literal("component_table"),
      v.literal("external_reader"),
      v.literal("materialized_rows")
    ),
    entityType: v.optional(v.string()),
    scopeDefinition: v.optional(v.any()),
    selectedFieldKeys: v.array(v.string()),
    dateFieldKey: v.optional(v.string()),
    rowKeyStrategy: v.optional(v.string()),
    schedulePreset: v.optional(
      v.union(
        v.literal("manual"),
        v.literal("daily"),
        v.literal("weekly_monday"),
        v.literal("monthly_first_day")
      )
    ),
    fieldCatalog: v.optional(
      v.array(
        v.object({
          key: v.string(),
          label: v.string(),
          valueType: v.string(),
          filterable: v.optional(v.boolean()),
        })
      )
    ),
    metadata: v.optional(v.any()),
    enabled: v.boolean(),
    status: v.union(
      v.literal("idle"),
      v.literal("refreshing"),
      v.literal("ready"),
      v.literal("error")
    ),
    materializedCount: v.number(),
    lastRefreshedAt: v.optional(v.number()),
    lastError: v.optional(v.string()),
    activeMaterializationJobId: v.optional(v.id("materializationJobs")),
    lastFrozenExportId: v.optional(v.id("analyticsExports")),
    lastSnapshotId: v.optional(v.id("snapshots")),
    lastSnapshotRunId: v.optional(v.id("snapshotRuns")),
    archivedAt: v.optional(v.number()),
    createdAt: v.number(),
    updatedAt: v.optional(v.number()),
  })
    .index("by_profile", ["profileId"])
    .index("by_profile_and_source_key", ["profileId", "sourceKey"])
    .index("by_profile_and_archived_at", ["profileId", "archivedAt"])
    .index("by_source_key", ["sourceKey"]),

  analyticsMaterializedRows: defineTable({
    dataSourceId: v.id("dataSources"),
    sourceKey: v.string(),
    rowKey: v.string(),
    sourceRecordId: v.optional(v.string()),
    sourceEntityType: v.string(),
    occurredAt: v.number(),
    rowData: v.any(),
    updatedAt: v.number(),
  })
    .index("by_data_source", ["dataSourceId"])
    .index("by_source_key", ["sourceKey"])
    .index("by_data_source_and_row_key", ["dataSourceId", "rowKey"])
    .index("by_data_source_and_occurred_at", ["dataSourceId", "occurredAt"])
    .index("by_source_key_and_occurred_at", ["sourceKey", "occurredAt"]),

  materializationJobs: defineTable({
    profileId: v.id("snapshotProfiles"),
    dataSourceId: v.id("dataSources"),
    sourceKey: v.string(),
    status: v.union(
      v.literal("staging"),
      v.literal("purging"),
      v.literal("inserting"),
      v.literal("freezing"),
      v.literal("completed"),
      v.literal("error")
    ),
    triggerKind: v.union(
      v.literal("manual"),
      v.literal("schedule"),
      v.literal("upsert"),
      v.literal("snapshot")
    ),
    requestedBy: v.optional(v.string()),
    stagedCount: v.number(),
    insertedCount: v.number(),
    deletedCount: v.number(),
    finalRowCount: v.optional(v.number()),
    errorMessage: v.optional(v.string()),
    createdAt: v.number(),
    startedAt: v.optional(v.number()),
    finishedAt: v.optional(v.number()),
  })
    .index("by_data_source", ["dataSourceId"])
    .index("by_data_source_and_created_at", ["dataSourceId", "createdAt"])
    .index("by_status", ["status"]),

  materializationJobRows: defineTable({
    jobId: v.id("materializationJobs"),
    rowIndex: v.number(),
    rowKey: v.string(),
    sourceRecordId: v.optional(v.string()),
    sourceEntityType: v.string(),
    occurredAt: v.number(),
    rowData: v.any(),
    createdAt: v.number(),
  })
    .index("by_job", ["jobId"])
    .index("by_job_and_row_index", ["jobId", "rowIndex"]),

  analyticsExports: defineTable({
    requestedBy: v.optional(v.string()),
    name: v.optional(v.string()),
    status: v.union(
      v.literal("completed"),
      v.literal("expired"),
      v.literal("error")
    ),
    dataSourceId: v.id("dataSources"),
    dataSourceKey: v.string(),
    configSnapshot: v.any(),
    filters: v.optional(v.any()),
    storageId: v.optional(v.id("_storage")),
    fileName: v.optional(v.string()),
    rowCount: v.optional(v.number()),
    usageKind: v.union(v.literal("manual"), v.literal("kpi_snapshot_source")),
    pinnedByAudit: v.boolean(),
    auditProfileSlug: v.optional(v.string()),
    auditSnapshotId: v.optional(v.id("snapshots")),
    auditSnapshotRunId: v.optional(v.id("snapshotRuns")),
    materializationJobId: v.optional(v.id("materializationJobs")),
    regeneratedFromExportId: v.optional(v.id("analyticsExports")),
    clonedFromExportId: v.optional(v.id("analyticsExports")),
    createdAt: v.number(),
    startedAt: v.optional(v.number()),
    completedAt: v.optional(v.number()),
    expiresAt: v.optional(v.number()),
    error: v.optional(v.string()),
  })
    .index("by_data_source", ["dataSourceId"])
    .index("by_data_source_key", ["dataSourceKey"])
    .index("by_requested_by_and_created", ["requestedBy", "createdAt"])
    .index("by_audit_snapshot", ["auditSnapshotId"])
    .index("by_expires", ["expiresAt"])
    .index("by_pinned_by_audit", ["pinnedByAudit"]),

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
    .index("by_profile_and_indicator", ["profileId", "indicatorId"])
    .index("by_profile_and_data_source", ["profileId", "dataSourceId"]),

  integrationValues: defineTable({
    snapshotValueId: v.id("snapshotValues"),
    snapshotId: v.id("snapshots"),
    snapshotRunId: v.id("snapshotRuns"),
    profileId: v.id("snapshotProfiles"),
    indicatorId: v.id("indicators"),
    externalId: v.optional(v.string()),
    value: v.number(),
    measuredAt: v.number(),
    syncStatus: v.union(v.literal("pending"), v.literal("synced")),
    lastSyncedAt: v.optional(v.number()),
    createdAt: v.number(),
  })
    .index("by_indicator_and_measured_at", ["indicatorId", "measuredAt"])
    .index("by_external_id", ["externalId"])
    .index("by_snapshot_value", ["snapshotValueId"])
    .index("by_sync_status_and_measured_at", ["syncStatus", "measuredAt"])
    .index("by_profile_and_sync_status_and_measured_at", [
      "profileId",
      "syncStatus",
      "measuredAt",
    ]),

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
    triggerKind: v.optional(
      v.union(
        v.literal("manual"),
        v.literal("source_materialization"),
        v.literal("scheduled_materialization")
      )
    ),
    triggerSourceKey: v.optional(v.string()),
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
    triggerKind: v.optional(
      v.union(
        v.literal("manual"),
        v.literal("source_materialization"),
        v.literal("scheduled_materialization")
      )
    ),
    triggerSourceKey: v.optional(v.string()),
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
    sourceExportIds: v.array(v.id("analyticsExports")),
    errorMessage: v.optional(v.string()),
    warningMessage: v.optional(v.string()),
    ruleHash: v.string(),
    evidenceRef: v.optional(v.string()),
    evidenceFileName: v.optional(v.string()),
    evidenceRowCount: v.optional(v.number()),
    evidenceGeneratedAt: v.optional(v.number()),
    evidenceMimeType: v.optional(v.string()),
    evidenceSha256: v.optional(v.string()),
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
    sourceExportIds: v.array(v.id("analyticsExports")),
    explainRef: v.optional(v.string()),
    evidenceRef: v.optional(v.string()),
    evidenceFileName: v.optional(v.string()),
    evidenceRowCount: v.optional(v.number()),
    evidenceGeneratedAt: v.optional(v.number()),
    evidenceMimeType: v.optional(v.string()),
    evidenceSha256: v.optional(v.string()),
  })
    .index("by_snapshot", ["snapshotId"])
    .index("by_snapshot_and_indicator", ["snapshotId", "indicatorId"])
    .index("by_indicator", ["indicatorId"])
    .index("by_snapshot_run_item", ["snapshotRunItemId"]),

  calculationTraces: defineTable({
    snapshotRunId: v.id("snapshotRuns"),
    snapshotRunItemId: v.id("snapshotRunItems"),
    profileId: v.id("snapshotProfiles"),
    definitionId: v.id("calculationDefinitions"),
    queryParams: v.optional(v.any()),
    resolvedFilters: v.optional(v.any()),
    sampleRowsPreview: v.optional(v.array(v.any())),
    warnings: v.optional(v.array(v.string())),
    notes: v.optional(v.string()),
    createdAt: v.number(),
  })
    .index("by_snapshot_run", ["snapshotRunId"])
    .index("by_snapshot_run_item", ["snapshotRunItemId"]),

  derivedIndicators: defineTable({
    profileId: v.id("snapshotProfiles"),
    slug: v.string(),
    label: v.string(),
    unit: v.optional(v.string()),
    description: v.optional(v.string()),
    formula: v.object({
      kind: v.union(
        v.literal("ratio"),
        v.literal("difference"),
        v.literal("sum")
      ),
      operands: v.array(
        v.object({
          indicatorSlug: v.string(),
          role: v.optional(
            v.union(
              v.literal("numerator"),
              v.literal("denominator"),
              v.literal("term")
            )
          ),
          weight: v.optional(v.number()),
        })
      ),
    }),
    enabled: v.boolean(),
    createdAt: v.number(),
    updatedAt: v.optional(v.number()),
  })
    .index("by_profile", ["profileId"])
    .index("by_profile_and_slug", ["profileId", "slug"]),

  derivedSnapshotValues: defineTable({
    snapshotId: v.id("snapshots"),
    snapshotRunId: v.id("snapshotRuns"),
    profileId: v.id("snapshotProfiles"),
    derivedIndicatorId: v.id("derivedIndicators"),
    derivedIndicatorSlug: v.string(),
    derivedIndicatorLabelSnapshot: v.string(),
    derivedIndicatorUnit: v.optional(v.string()),
    formulaKind: v.union(
      v.literal("ratio"),
      v.literal("difference"),
      v.literal("sum")
    ),
    value: v.number(),
    computedAt: v.number(),
    baseSnapshotValueIds: v.array(v.id("snapshotValues")),
    baseIndicatorSlugs: v.array(v.string()),
    sourceExportIds: v.array(v.id("analyticsExports")),
    formulaSnapshot: v.any(),
    createdAt: v.number(),
  })
    .index("by_snapshot", ["snapshotId"])
    .index("by_profile", ["profileId"])
    .index("by_snapshot_and_slug", ["snapshotId", "derivedIndicatorSlug"]),
});
