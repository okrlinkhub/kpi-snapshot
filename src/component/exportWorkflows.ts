import { v } from "convex/values";
import { api, internal } from "./_generated/api.js";
import {
  action,
  internalAction,
  internalMutation,
  internalQuery,
} from "./_generated/server.js";
import type { Doc, Id } from "./_generated/dataModel.js";

type ExportFilters = {
  startDate?: number;
  endDate?: number;
  fieldFilters?: Array<{ fieldKey: string; values: string[] }>;
};

type MaterializedRowsBatchResult = {
  page: Array<{
    _id: Id<"analyticsMaterializedRows">;
    rowKey: string;
    occurredAt: number;
    rowData: Record<string, unknown>;
  }>;
  hasMore: boolean;
  nextRowKey: string | null;
};

const exportFieldFilterValidator = v.object({
  fieldKey: v.string(),
  values: v.array(v.string()),
});

const exportFiltersValidator = v.object({
  startDate: v.optional(v.number()),
  endDate: v.optional(v.number()),
  dateFieldKey: v.optional(v.string()),
  fieldFilters: v.optional(v.array(exportFieldFilterValidator)),
});

function escapeCsvValue(value: unknown) {
  if (value == null) {
    return "";
  }
  if (typeof value === "object") {
    return `"${JSON.stringify(value).replace(/"/g, '""')}"`;
  }
  const raw = String(value).replace(/\r?\n/g, " ");
  if (raw.includes('"') || raw.includes(",") || raw.includes(";")) {
    return `"${raw.replace(/"/g, '""')}"`;
  }
  return raw;
}

function matchesExportFilters(
  row: {
    occurredAt: number;
    rowData: Record<string, unknown>;
  },
  filters?: ExportFilters
) {
  if (typeof filters?.startDate === "number" && row.occurredAt < filters.startDate) {
    return false;
  }
  if (typeof filters?.endDate === "number" && row.occurredAt > filters.endDate) {
    return false;
  }
  if (!filters?.fieldFilters || filters.fieldFilters.length === 0) {
    return true;
  }
  return filters.fieldFilters.every((fieldFilter) => {
    if (fieldFilter.values.length === 0) {
      return true;
    }
    const value = row.rowData[fieldFilter.fieldKey];
    return fieldFilter.values.includes(value == null ? "" : String(value));
  });
}

function slugifyName(value?: string, fallback?: string) {
  return (value ?? fallback ?? "export")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

async function getDataSourceByKey(ctx: any, sourceKey: string) {
  return await ctx.db
    .query("dataSources")
    .withIndex("by_source_key", (q: any) => q.eq("sourceKey", sourceKey))
    .unique();
}

async function getDataSourceById(ctx: any, dataSourceId: string) {
  return await ctx.db.get(dataSourceId as Id<"dataSources">);
}

function buildConfigSnapshot(dataSource: Doc<"dataSources">) {
  return {
    adapterKey: dataSource.adapterKey,
    label: dataSource.label,
    sourceKey: dataSource.sourceKey,
    entityType: dataSource.entityType,
    selectedFieldKeys: dataSource.selectedFieldKeys,
    dateFieldKey: dataSource.dateFieldKey,
    rowKeyStrategy: dataSource.rowKeyStrategy,
    scopeDefinition: dataSource.scopeDefinition,
    fieldCatalog: dataSource.fieldCatalog,
    metadata: dataSource.metadata,
  };
}

export const listMaterializedRowsBatch = internalQuery({
  args: {
    dataSourceId: v.string(),
    snapshotAt: v.optional(v.number()),
    cursorRowKey: v.optional(v.string()),
    batchSize: v.optional(v.number()),
  },
  returns: v.object({
    page: v.array(
      v.object({
        _id: v.id("analyticsMaterializedRows"),
        rowKey: v.string(),
        occurredAt: v.number(),
        rowData: v.any(),
      })
    ),
    hasMore: v.boolean(),
    nextRowKey: v.union(v.string(), v.null()),
  }),
  handler: async (ctx, args) => {
    const batchSize = Math.max(1, Math.min(args.batchSize ?? 500, 500));
    const result = await ctx.db
      .query("analyticsMaterializedRows")
      .withIndex("by_data_source_and_row_key", (q) => {
        const scopedQuery = q.eq("dataSourceId", args.dataSourceId as Id<"dataSources">);
        return args.cursorRowKey
          ? scopedQuery.gt("rowKey", args.cursorRowKey)
          : scopedQuery;
      })
      .take(batchSize);

    const page = result
      .filter((row) =>
        typeof args.snapshotAt === "number"
          ? row.occurredAt <= args.snapshotAt
          : true
      )
      .map((row) => ({
        _id: row._id,
        rowKey: row.rowKey,
        occurredAt: row.occurredAt,
        rowData: row.rowData,
      }));

    return {
      page,
      hasMore: result.length === batchSize,
      nextRowKey: result.length > 0 ? result[result.length - 1].rowKey : null,
    };
  },
});

export const persistCompletedExport = internalMutation({
  args: {
    profileId: v.optional(v.string()),
    requestedBy: v.optional(v.string()),
    name: v.optional(v.string()),
    dataSourceId: v.string(),
    filters: v.optional(exportFiltersValidator),
    usageKind: v.union(v.literal("manual"), v.literal("kpi_snapshot_source")),
    pinnedByAudit: v.boolean(),
    auditProfileSlug: v.optional(v.string()),
    auditSnapshotId: v.optional(v.string()),
    auditSnapshotRunId: v.optional(v.string()),
    clonedFromExportId: v.optional(v.string()),
    regeneratedFromExportId: v.optional(v.string()),
    materializationJobId: v.optional(v.string()),
    storageId: v.id("_storage"),
    fileName: v.string(),
    rowCount: v.number(),
  },
  returns: v.string(),
  handler: async (ctx, args) => {
    const dataSource = await getDataSourceById(ctx, args.dataSourceId);
    if (!dataSource) {
      throw new Error("Data source non trovata");
    }
    const now = Date.now();
    const exportId = await ctx.db.insert("analyticsExports", {
      profileId: args.profileId
        ? (args.profileId as Id<"snapshotProfiles">)
        : undefined,
      exportScope: args.profileId ? "profile" : "global",
      requestedBy: args.requestedBy,
      name: args.name,
      status: "completed",
      dataSourceId: dataSource._id,
      dataSourceKey: dataSource.sourceKey,
      configSnapshot: buildConfigSnapshot(dataSource),
      filters: args.filters,
      storageId: args.storageId,
      fileName: args.fileName,
      rowCount: args.rowCount,
      usageKind: args.usageKind,
      pinnedByAudit: args.pinnedByAudit,
      auditProfileSlug: args.auditProfileSlug,
      auditSnapshotId: args.auditSnapshotId
        ? (args.auditSnapshotId as Id<"snapshots">)
        : undefined,
      auditSnapshotRunId: args.auditSnapshotRunId
        ? (args.auditSnapshotRunId as Id<"snapshotRuns">)
        : undefined,
      materializationJobId: args.materializationJobId
        ? (args.materializationJobId as Id<"materializationJobs">)
        : undefined,
      clonedFromExportId: args.clonedFromExportId
        ? (args.clonedFromExportId as Id<"analyticsExports">)
        : undefined,
      regeneratedFromExportId: args.regeneratedFromExportId
        ? (args.regeneratedFromExportId as Id<"analyticsExports">)
        : undefined,
      createdAt: now,
      startedAt: now,
      completedAt: now,
      expiresAt: args.pinnedByAudit ? undefined : now + (7 * 24 * 60 * 60 * 1000),
    });
    return String(exportId);
  },
});

async function createExportFromPages(
  ctx: any,
  args: {
    requestedBy?: string;
    name?: string;
    dataSource: Doc<"dataSources">;
    profileId?: string;
    filters?: ExportFilters;
    usageKind: "manual" | "kpi_snapshot_source";
    pinnedByAudit: boolean;
    auditProfileSlug?: string;
    auditSnapshotId?: string;
    auditSnapshotRunId?: string;
    clonedFromExportId?: string;
    regeneratedFromExportId?: string;
    materializationJobId?: string;
    snapshotAt?: number;
  }
): Promise<string> {
  let cursorRowKey: string | undefined;
  let hasMore = true;
  let rowCount = 0;
  const headers =
    args.dataSource.selectedFieldKeys.length > 0
      ? ["occurredAt", ...args.dataSource.selectedFieldKeys]
      : ["occurredAt"];
  const lines: string[] = [];

  while (hasMore) {
    const result: MaterializedRowsBatchResult = await ctx.runQuery(
      internal.exportWorkflows.listMaterializedRowsBatch,
      {
        dataSourceId: String(args.dataSource._id),
        snapshotAt: args.snapshotAt,
        cursorRowKey,
        batchSize: 500,
      }
    );
    for (const row of result.page as Array<{
      rowKey: string;
      occurredAt: number;
      rowData: Record<string, unknown>;
    }>) {
      if (!matchesExportFilters(row, args.filters)) {
        continue;
      }
      if (headers.length === 1) {
        for (const key of Object.keys(row.rowData)) {
          headers.push(key);
        }
      }
      const values = headers.map((header) => {
        if (header === "occurredAt") {
          return escapeCsvValue(row.occurredAt);
        }
        return escapeCsvValue(row.rowData[header]);
      });
      lines.push(values.join(";"));
      rowCount++;
    }
    hasMore = result.hasMore;
    cursorRowKey = result.nextRowKey ?? undefined;
  }

  const csvContent = `\uFEFF${headers.join(";")}\n${lines.join("\n")}`;
  const storageId = await ctx.storage.store(
    new Blob([csvContent], { type: "text/csv;charset=utf-8" })
  );
  const now = Date.now();
  const fileName = `analytics-export-${slugifyName(
    args.name,
    args.dataSource.label ?? args.dataSource.sourceKey
  )}-${now}.csv`;
  return await ctx.runMutation(internal.exportWorkflows.persistCompletedExport, {
    requestedBy: args.requestedBy,
    profileId: args.profileId,
    name: args.name,
    dataSourceId: String(args.dataSource._id),
    filters: args.filters,
    usageKind: args.usageKind,
    pinnedByAudit: args.pinnedByAudit,
    auditProfileSlug: args.auditProfileSlug,
    auditSnapshotId: args.auditSnapshotId,
    auditSnapshotRunId: args.auditSnapshotRunId,
    clonedFromExportId: args.clonedFromExportId,
    regeneratedFromExportId: args.regeneratedFromExportId,
    materializationJobId: args.materializationJobId,
    storageId,
    fileName,
    rowCount,
  });
}

export const requestExport = action({
  args: {
    profileSlug: v.optional(v.string()),
    requestedBy: v.optional(v.string()),
    name: v.optional(v.string()),
    dataSourceKey: v.string(),
    filters: exportFiltersValidator,
    clonedFromExportId: v.optional(v.string()),
  },
  returns: v.string(),
  handler: async (ctx, args): Promise<string> => {
    const profile = args.profileSlug
      ? await ctx.runQuery(api.snapshotEngine.listSnapshotProfiles, { includeArchived: true })
      : null;
    const profileId = args.profileSlug
      ? profile?.find((row: { slug: string }) => row.slug === args.profileSlug)?._id
      : undefined;
    const dataSource: Doc<"dataSources"> | null = await ctx.runQuery(
      api.materialization.getDataSourceByKey,
      {
        sourceKey: args.dataSourceKey,
      }
    );
    if (!dataSource || !dataSource.enabled) {
      throw new Error("Data source non disponibile");
    }
    if (dataSource.status !== "ready") {
      throw new Error("Rigenera prima la materializzazione della data source");
    }
    return await createExportFromPages(ctx, {
      profileId: profileId ? String(profileId) : undefined,
      requestedBy: args.requestedBy,
      name: args.name,
      dataSource,
      filters: args.filters,
      usageKind: "manual",
      pinnedByAudit: false,
      clonedFromExportId: args.clonedFromExportId,
    });
  },
});

export const regenerateExport = action({
  args: {
    exportId: v.string(),
    requestedBy: v.optional(v.string()),
    name: v.optional(v.string()),
  },
  returns: v.string(),
  handler: async (ctx, args): Promise<string> => {
    const exportRow: Doc<"analyticsExports"> | null = await ctx.runQuery(
      internal.exportWorkflows.getExportForRegeneration,
      {
        exportId: args.exportId,
      }
    );
    if (!exportRow) {
      throw new Error("Export non trovato");
    }
    const dataSource: Doc<"dataSources"> | null = await ctx.runQuery(
      api.materialization.getDataSourceByKey,
      {
        sourceKey: exportRow.dataSourceKey,
      }
    );
    if (!dataSource) {
      throw new Error("Data source non trovata");
    }
    return await createExportFromPages(ctx, {
      profileId: exportRow.profileId ? String(exportRow.profileId) : undefined,
      requestedBy: args.requestedBy ?? exportRow.requestedBy,
      name: args.name ?? exportRow.name,
      dataSource,
      filters: exportRow.filters,
      usageKind: "manual",
      pinnedByAudit: false,
      regeneratedFromExportId: args.exportId,
    });
  },
});

export const getExportForRegeneration = internalQuery({
  args: {
    exportId: v.string(),
  },
  returns: v.union(v.any(), v.null()),
  handler: async (ctx, args) => {
    return await ctx.db.get(args.exportId as Id<"analyticsExports">);
  },
});

export const freezeSnapshotExports = internalAction({
  args: {
    profileSlug: v.string(),
    snapshotId: v.string(),
    snapshotRunId: v.string(),
    triggeredBy: v.optional(v.string()),
    materializationJobId: v.optional(v.string()),
    sourceKeys: v.array(v.string()),
    snapshotAt: v.optional(v.number()),
  },
  returns: v.array(v.string()),
  handler: async (ctx, args) => {
    const exportIds: string[] = [];
    for (const sourceKey of args.sourceKeys) {
      const dataSource: Doc<"dataSources"> | null = await ctx.runQuery(api.materialization.getDataSourceByKey, {
        sourceKey,
      });
      if (!dataSource) {
        continue;
      }
      const exportId = await createExportFromPages(ctx, {
        requestedBy: args.triggeredBy,
        name: `KPI source ${args.profileSlug} - ${dataSource.label}`,
        dataSource,
        usageKind: "kpi_snapshot_source",
        pinnedByAudit: true,
        auditProfileSlug: args.profileSlug,
        auditSnapshotId: args.snapshotId,
        auditSnapshotRunId: args.snapshotRunId,
        materializationJobId: args.materializationJobId,
        snapshotAt: args.snapshotAt,
      });
      exportIds.push(exportId);
      await ctx.runMutation(internal.exportWorkflows.attachExportToSnapshot, {
        snapshotId: args.snapshotId,
        snapshotRunId: args.snapshotRunId,
        sourceKey,
        exportId,
      });
    }
    return exportIds;
  },
});

export const attachExportToSnapshot = internalMutation({
  args: {
    snapshotId: v.string(),
    snapshotRunId: v.string(),
    sourceKey: v.string(),
    exportId: v.string(),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    const snapshotId = args.snapshotId as Id<"snapshots">;
    const snapshotRunId = args.snapshotRunId as Id<"snapshotRuns">;
    const exportId = args.exportId as Id<"analyticsExports">;
    const dataSource = await getDataSourceByKey(ctx, args.sourceKey);
    if (!dataSource) {
      return null;
    }
    const runItems = await ctx.db
      .query("snapshotRunItems")
      .withIndex("by_snapshot_run", (q) => q.eq("snapshotRunId", snapshotRunId))
      .collect();
    const matchingItems = runItems.filter((item) => item.dataSourceId === dataSource._id);
    for (const item of matchingItems) {
      await ctx.db.patch(item._id, {
        sourceExportIds: [...new Set([...(item.sourceExportIds ?? []), exportId])],
      });
      const snapshotValues = await ctx.db
        .query("snapshotValues")
        .withIndex("by_snapshot_run_item", (q) => q.eq("snapshotRunItemId", item._id))
        .collect();
      for (const snapshotValue of snapshotValues) {
        await ctx.db.patch(snapshotValue._id, {
          sourceExportIds: [...new Set([...(snapshotValue.sourceExportIds ?? []), exportId])],
        });
      }
    }
    const derivedValues = await ctx.db
      .query("derivedSnapshotValues")
      .withIndex("by_snapshot", (q) => q.eq("snapshotId", snapshotId))
      .collect();
    for (const derivedValue of derivedValues) {
      const baseValues = await Promise.all(
        derivedValue.baseSnapshotValueIds.map((valueId) => ctx.db.get(valueId))
      );
      const sourceExportIds = [
        ...new Set(
          baseValues.flatMap((value) => (value?.sourceExportIds ?? []).map((valueId) => String(valueId)))
        ),
      ];
      await ctx.db.patch(derivedValue._id, {
        sourceExportIds: sourceExportIds.map((valueId) => valueId as Id<"analyticsExports">),
      });
    }
    await ctx.db.patch(dataSource._id, {
      lastFrozenExportId: exportId,
      lastSnapshotId: snapshotId,
      lastSnapshotRunId: snapshotRunId,
      updatedAt: Date.now(),
    });
    return null;
  },
});
