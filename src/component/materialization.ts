import { v } from "convex/values";
import { internal } from "./_generated/api.js";
import { internalMutation, mutation, query } from "./_generated/server.js";
import type { Id } from "./_generated/dataModel.js";

const triggerKindValidator = v.union(
  v.literal("manual"),
  v.literal("schedule"),
  v.literal("upsert"),
  v.literal("snapshot")
);

const materializedRowInputValidator = v.object({
  rowKey: v.string(),
  occurredAt: v.number(),
  rowData: v.any(),
  sourceRecordId: v.optional(v.string()),
  sourceEntityType: v.optional(v.string()),
});

function normalizeMaterializedRows(
  rows: Array<{
    rowKey: string;
    occurredAt: number;
    rowData: Record<string, unknown>;
    sourceRecordId?: string;
    sourceEntityType?: string;
  }>
) {
  return rows.map((row) => ({
    rowKey: row.rowKey,
    occurredAt: row.occurredAt,
    sourceRecordId: row.sourceRecordId,
    sourceEntityType: row.sourceEntityType ?? "record",
    rowData:
      row.rowData && typeof row.rowData === "object"
        ? row.rowData
        : {},
  }));
}

async function lookupDataSourceByKey(ctx: any, sourceKey: string) {
  const dataSource = await ctx.db
    .query("dataSources")
    .withIndex("by_source_key", (q: any) => q.eq("sourceKey", sourceKey))
    .unique();
  if (!dataSource || dataSource.archivedAt) {
    return null;
  }
  return dataSource;
}

export const getDataSourceByKey = query({
  args: {
    sourceKey: v.string(),
  },
  returns: v.union(v.any(), v.null()),
  handler: async (ctx, args) => {
    const dataSource = await lookupDataSourceByKey(ctx, args.sourceKey);
    if (!dataSource) {
      return null;
    }
    return dataSource;
  },
});

export const startMaterializationJob = mutation({
  args: {
    sourceKey: v.string(),
    triggerKind: triggerKindValidator,
    requestedBy: v.optional(v.string()),
  },
  returns: v.object({
    jobId: v.string(),
    sourceKey: v.string(),
  }),
  handler: async (ctx, args) => {
    const dataSource = await lookupDataSourceByKey(ctx, args.sourceKey);
    if (!dataSource || !dataSource.enabled) {
      throw new Error("Data source non trovata o disabilitata");
    }
    const now = Date.now();
    const jobId = await ctx.db.insert("materializationJobs", {
      dataSourceId: dataSource._id,
      sourceKey: dataSource.sourceKey,
      status: "staging",
      triggerKind: args.triggerKind,
      requestedBy: args.requestedBy,
      stagedCount: 0,
      insertedCount: 0,
      deletedCount: 0,
      createdAt: now,
      startedAt: now,
    });
    await ctx.db.patch(dataSource._id, {
      status: "refreshing",
      lastError: undefined,
      activeMaterializationJobId: jobId,
      updatedAt: now,
    });
    return {
      jobId: String(jobId),
      sourceKey: dataSource.sourceKey,
    };
  },
});

export const appendMaterializationRowsBatch = mutation({
  args: {
    jobId: v.string(),
    offset: v.number(),
    rows: v.array(materializedRowInputValidator),
  },
  returns: v.object({
    stagedCount: v.number(),
  }),
  handler: async (ctx, args) => {
    const job = await ctx.db.get(args.jobId as Id<"materializationJobs">);
    if (!job) {
      throw new Error("Job di materializzazione non trovato");
    }
    if (job.status !== "staging") {
      throw new Error("Il job non accetta piu' nuove righe");
    }
    const now = Date.now();
    const normalizedRows = normalizeMaterializedRows(args.rows as Array<any>);
    for (const [index, row] of normalizedRows.entries()) {
      await ctx.db.insert("materializationJobRows", {
        jobId: job._id,
        rowIndex: args.offset + index,
        rowKey: row.rowKey,
        sourceRecordId: row.sourceRecordId,
        sourceEntityType: row.sourceEntityType,
        occurredAt: row.occurredAt,
        rowData: row.rowData,
        createdAt: now,
      });
    }
    const stagedCount = job.stagedCount + normalizedRows.length;
    await ctx.db.patch(job._id, {
      stagedCount,
    });
    return { stagedCount };
  },
});

export const purgeExistingMaterializedRowsBatch = mutation({
  args: {
    jobId: v.string(),
    batchSize: v.optional(v.number()),
  },
  returns: v.object({
    deletedCount: v.number(),
    hasMore: v.boolean(),
  }),
  handler: async (ctx, args) => {
    const job = await ctx.db.get(args.jobId as Id<"materializationJobs">);
    if (!job) {
      throw new Error("Job di materializzazione non trovato");
    }
    const batchSize = Math.max(1, Math.min(args.batchSize ?? 200, 500));
    const existingRows = await ctx.db
      .query("analyticsMaterializedRows")
      .withIndex("by_data_source", (q) => q.eq("dataSourceId", job.dataSourceId))
      .take(batchSize);
    for (const existingRow of existingRows) {
      await ctx.db.delete(existingRow._id);
    }
    const deletedCount = job.deletedCount + existingRows.length;
    await ctx.db.patch(job._id, {
      status: "purging",
      deletedCount,
    });
    return {
      deletedCount,
      hasMore: existingRows.length === batchSize,
    };
  },
});

export const insertMaterializationRowsBatch = mutation({
  args: {
    jobId: v.string(),
    batchSize: v.optional(v.number()),
  },
  returns: v.object({
    insertedCount: v.number(),
    hasMore: v.boolean(),
  }),
  handler: async (ctx, args) => {
    const job = await ctx.db.get(args.jobId as Id<"materializationJobs">);
    if (!job) {
      throw new Error("Job di materializzazione non trovato");
    }
    const batchSize = Math.max(1, Math.min(args.batchSize ?? 200, 500));
    const stagedRows = await ctx.db
      .query("materializationJobRows")
      .withIndex("by_job_and_row_index", (q) =>
        q.eq("jobId", job._id).gt("rowIndex", job.insertedCount - 1)
      )
      .take(batchSize);
    const now = Date.now();
    for (const row of stagedRows) {
      await ctx.db.insert("analyticsMaterializedRows", {
        dataSourceId: job.dataSourceId,
        sourceKey: job.sourceKey,
        rowKey: row.rowKey,
        sourceRecordId: row.sourceRecordId,
        sourceEntityType: row.sourceEntityType,
        occurredAt: row.occurredAt,
        rowData: row.rowData,
        updatedAt: now,
      });
    }
    const insertedCount = job.insertedCount + stagedRows.length;
    await ctx.db.patch(job._id, {
      status: "inserting",
      insertedCount,
    });
    return {
      insertedCount,
      hasMore: insertedCount < job.stagedCount,
    };
  },
});

export const completeMaterializationJob = mutation({
  args: {
    jobId: v.string(),
    frozenExportId: v.optional(v.string()),
    snapshotId: v.optional(v.string()),
    snapshotRunId: v.optional(v.string()),
  },
  returns: v.object({
    rowCount: v.number(),
  }),
  handler: async (ctx, args) => {
    const job = await ctx.db.get(args.jobId as Id<"materializationJobs">);
    if (!job) {
      throw new Error("Job di materializzazione non trovato");
    }
    const dataSource = await ctx.db.get(job.dataSourceId);
    if (!dataSource) {
      throw new Error("Data source del job non trovata");
    }
    const now = Date.now();
    await ctx.db.patch(job._id, {
      status: "completed",
      finalRowCount: job.stagedCount,
      finishedAt: now,
    });
    await ctx.db.patch(dataSource._id, {
      status: "ready",
      materializedCount: job.stagedCount,
      lastRefreshedAt: now,
      lastError: undefined,
      activeMaterializationJobId: undefined,
      lastFrozenExportId: args.frozenExportId
        ? (args.frozenExportId as Id<"analyticsExports">)
        : dataSource.lastFrozenExportId,
      lastSnapshotId: args.snapshotId
        ? (args.snapshotId as Id<"snapshots">)
        : dataSource.lastSnapshotId,
      lastSnapshotRunId: args.snapshotRunId
        ? (args.snapshotRunId as Id<"snapshotRuns">)
        : dataSource.lastSnapshotRunId,
      updatedAt: now,
    });
    await ctx.scheduler.runAfter(0, internal.materialization.cleanupMaterializationJobRows, {
      jobId: String(job._id),
    });
    return {
      rowCount: job.stagedCount,
    };
  },
});

export const failMaterializationJob = mutation({
  args: {
    jobId: v.string(),
    errorMessage: v.string(),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    const job = await ctx.db.get(args.jobId as Id<"materializationJobs">);
    if (!job) {
      return null;
    }
    const dataSource = await ctx.db.get(job.dataSourceId);
    const now = Date.now();
    await ctx.db.patch(job._id, {
      status: "error",
      errorMessage: args.errorMessage,
      finishedAt: now,
    });
    if (dataSource) {
      await ctx.db.patch(dataSource._id, {
        status: "error",
        lastError: args.errorMessage,
        activeMaterializationJobId: undefined,
        updatedAt: now,
      });
    }
    await ctx.scheduler.runAfter(0, internal.materialization.cleanupMaterializationJobRows, {
      jobId: String(job._id),
    });
    return null;
  },
});

export const cleanupMaterializationJobRows = internalMutation({
  args: {
    jobId: v.string(),
    batchSize: v.optional(v.number()),
  },
  returns: v.object({
    deletedCount: v.number(),
    hasMore: v.boolean(),
  }),
  handler: async (ctx, args) => {
    const jobId = args.jobId as Id<"materializationJobs">;
    const batchSize = Math.max(1, Math.min(args.batchSize ?? 200, 500));
    const rows = await ctx.db
      .query("materializationJobRows")
      .withIndex("by_job", (q) => q.eq("jobId", jobId))
      .take(batchSize);
    for (const row of rows) {
      await ctx.db.delete(row._id);
    }
    const hasMore = rows.length === batchSize;
    if (hasMore) {
      await ctx.scheduler.runAfter(0, internal.materialization.cleanupMaterializationJobRows, {
        jobId: args.jobId,
        batchSize,
      });
    }
    return {
      deletedCount: rows.length,
      hasMore,
    };
  },
});

export const purgeArchivedDataSourceRows = internalMutation({
  args: {
    dataSourceId: v.string(),
    batchSize: v.optional(v.number()),
  },
  returns: v.object({
    deletedCount: v.number(),
    hasMore: v.boolean(),
  }),
  handler: async (ctx, args) => {
    const dataSourceId = args.dataSourceId as Id<"dataSources">;
    const batchSize = Math.max(1, Math.min(args.batchSize ?? 200, 500));
    const rows = await ctx.db
      .query("analyticsMaterializedRows")
      .withIndex("by_data_source", (q) => q.eq("dataSourceId", dataSourceId))
      .take(batchSize);
    for (const row of rows) {
      await ctx.db.delete(row._id);
    }
    const hasMore = rows.length === batchSize;
    if (hasMore) {
      await ctx.scheduler.runAfter(0, internal.materialization.purgeArchivedDataSourceRows, {
        dataSourceId: args.dataSourceId,
        batchSize,
      });
    }
    return {
      deletedCount: rows.length,
      hasMore,
    };
  },
});

export const deleteDataSource = mutation({
  args: {
    profileSlug: v.optional(v.string()),
    sourceKey: v.string(),
  },
  returns: v.object({
    deleted: v.boolean(),
  }),
  handler: async (ctx, args) => {
    const dataSource = await lookupDataSourceByKey(ctx, args.sourceKey);
    if (!dataSource) {
      throw new Error("Data source non trovata");
    }
    const linkedDefinitions = await ctx.db
      .query("calculationDefinitions")
      .withIndex("by_data_source", (q) => q.eq("dataSourceId", dataSource._id))
      .take(1);
    if (linkedDefinitions.length > 0) {
      throw new Error("La data source e' ancora usata da regole KPI e non puo' essere eliminata");
    }
    const now = Date.now();
    await ctx.db.patch(dataSource._id, {
      archivedAt: now,
      enabled: false,
      status: "idle",
      materializedCount: 0,
      activeMaterializationJobId: undefined,
      updatedAt: now,
    });
    await ctx.scheduler.runAfter(0, internal.materialization.purgeArchivedDataSourceRows, {
      dataSourceId: String(dataSource._id),
    });
    return {
      deleted: true,
    };
  },
});
