import { v } from "convex/values";
import { mutation, query } from "./_generated/server.js";
import type { Doc, Id } from "./_generated/dataModel.js";

const operationValidator = v.union(
  v.literal("sum"),
  v.literal("count"),
  v.literal("avg"),
  v.literal("min"),
  v.literal("max"),
  v.literal("distinct_count")
);

const sourceKindValidator = v.union(
  v.literal("component_table"),
  v.literal("external_reader"),
  v.literal("materialized_rows")
);

const sourceRowInputValidator = v.object({
  occurredAt: v.number(),
  rowData: v.any(),
});

const sourcePayloadValidator = v.object({
  sourceKey: v.string(),
  rows: v.array(sourceRowInputValidator),
});

const evidenceUploadValidator = v.object({
  snapshotRunItemId: v.id("snapshotRunItems"),
  snapshotValueId: v.id("snapshotValues"),
  fileName: v.string(),
  rowCount: v.number(),
  csvContent: v.string(),
});

type Operation = "sum" | "count" | "avg" | "min" | "max" | "distinct_count";

type FilterRule = {
  field: string;
  op: "eq" | "neq" | "gt" | "gte" | "lt" | "lte" | "in";
  value: unknown;
};

type SourceRowInput = {
  occurredAt: number;
  rowData: Record<string, unknown>;
};

type CalculationResult = {
  inputCount: number;
  rawResult: number | null;
  normalizedResult: number | null;
  warningMessage?: string;
  filteredRows: Array<SourceRowInput>;
};

async function getProfileBySlug(
  ctx: any,
  slug: string
) {
  const profile = await ctx.db
    .query("snapshotProfiles")
    .withIndex("by_slug", (q: any) => q.eq("slug", slug))
    .unique();
  if (!profile) {
    throw new Error(`Snapshot profile '${slug}' non trovato`);
  }
  return profile;
}

async function getIndicatorByProfileAndSlug(
  ctx: any,
  profileId: Id<"snapshotProfiles">,
  slug: string
) {
  return await ctx.db
    .query("indicators")
    .withIndex("by_profile_and_slug", (q: any) =>
      q.eq("profileId", profileId).eq("slug", slug)
    )
    .unique();
}

function getNestedValue(rowData: unknown, fieldPath?: string): unknown {
  if (!fieldPath) return rowData;
  if (rowData == null || typeof rowData !== "object") return undefined;
  const chunks = fieldPath.split(".");
  let cursor: unknown = rowData;
  for (const chunk of chunks) {
    if (cursor == null || typeof cursor !== "object") return undefined;
    cursor = (cursor as Record<string, unknown>)[chunk];
  }
  return cursor;
}

function toFiniteNumber(value: unknown): number | null {
  if (typeof value !== "number" || Number.isNaN(value) || !Number.isFinite(value)) {
    return null;
  }
  return value;
}

function applyFilterRule(rowData: unknown, rule: FilterRule): boolean {
  const left = getNestedValue(rowData, rule.field);
  const right = rule.value;

  switch (rule.op) {
    case "eq":
      return left === right;
    case "neq":
      return left !== right;
    case "gt":
      return typeof left === "number" && typeof right === "number" && left > right;
    case "gte":
      return typeof left === "number" && typeof right === "number" && left >= right;
    case "lt":
      return typeof left === "number" && typeof right === "number" && left < right;
    case "lte":
      return typeof left === "number" && typeof right === "number" && left <= right;
    case "in":
      return Array.isArray(right) ? right.includes(left) : false;
    default:
      return false;
  }
}

function matchesFilters(rowData: unknown, filters: unknown): boolean {
  if (!Array.isArray(filters) || filters.length === 0) return true;
  return (filters as Array<FilterRule>).every((rule) => applyFilterRule(rowData, rule));
}

function computeOperation(
  rows: Array<SourceRowInput>,
  operation: Operation,
  fieldPath?: string
): number | null {
  if (operation === "count") return rows.length;

  const values = rows
    .map((row) => toFiniteNumber(getNestedValue(row.rowData, fieldPath)))
    .filter((v): v is number => v != null);

  if (values.length === 0) return null;

  switch (operation) {
    case "sum":
      return values.reduce((acc, value) => acc + value, 0);
    case "avg":
      return values.reduce((acc, value) => acc + value, 0) / values.length;
    case "min":
      return Math.min(...values);
    case "max":
      return Math.max(...values);
    case "distinct_count": {
      const distinct = new Set(values.map((v) => `${v}`));
      return distinct.size;
    }
    default:
      return null;
  }
}

function applyNormalization(value: number | null, normalization: unknown): number | null {
  if (value == null) {
    if (
      normalization &&
      typeof normalization === "object" &&
      "coalesce" in normalization &&
      typeof (normalization as { coalesce?: unknown }).coalesce === "number"
    ) {
      return (normalization as { coalesce: number }).coalesce;
    }
    return null;
  }

  let result = value;
  if (normalization && typeof normalization === "object") {
    const objectNormalization = normalization as {
      scale?: unknown;
      round?: unknown;
      clamp?: { min?: number; max?: number };
    };
    if (typeof objectNormalization.scale === "number") {
      result *= objectNormalization.scale;
    }
    if (typeof objectNormalization.round === "number") {
      const digits = Math.max(0, Math.trunc(objectNormalization.round));
      result = Number(result.toFixed(digits));
    }
    if (objectNormalization.clamp && typeof objectNormalization.clamp === "object") {
      const min = objectNormalization.clamp.min;
      const max = objectNormalization.clamp.max;
      if (typeof min === "number") {
        result = Math.max(min, result);
      }
      if (typeof max === "number") {
        result = Math.min(max, result);
      }
    }
  }

  return result;
}

function buildRuleHash(definition: Doc<"calculationDefinitions">): string {
  return [
    definition._id,
    definition.ruleVersion,
    definition.operation,
    definition.fieldPath ?? "",
    JSON.stringify(definition.filters ?? null),
    JSON.stringify(definition.normalization ?? null),
  ].join("|");
}

function normalizeIndicatorLabelSnapshot(label: string): string {
  return label.replace(/\s+/g, " ").trim();
}

function normalizeSourceRows(rows: Array<SourceRowInput>, snapshotAt: number): Array<SourceRowInput> {
  return rows
    .filter((row) => row.occurredAt <= snapshotAt)
    .map((row) => ({
      occurredAt: row.occurredAt,
      rowData:
        row.rowData && typeof row.rowData === "object"
          ? (row.rowData as Record<string, unknown>)
          : {},
    }));
}

function escapeCsvValue(value: unknown): string {
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

function buildEvidenceCsv(rows: Array<SourceRowInput>): string {
  if (rows.length === 0) {
    return "occurredAt,rowData\n";
  }

  const fieldSet = new Set<string>(["occurredAt"]);
  for (const row of rows) {
    Object.keys(row.rowData || {}).forEach((key) => fieldSet.add(key));
  }

  const headers = Array.from(fieldSet);
  const body = rows.map((row) =>
    headers
      .map((header) => {
        if (header === "occurredAt") {
          return escapeCsvValue(row.occurredAt);
        }
        return escapeCsvValue(row.rowData?.[header]);
      })
      .join(",")
  );

  return `${headers.join(",")}\n${body.join("\n")}`;
}

function runSingleDefinition(
  definition: Doc<"calculationDefinitions">,
  rows: Array<SourceRowInput>
): CalculationResult {
  const filteredRows = rows.filter((row) => matchesFilters(row.rowData, definition.filters));
  const rawResult = computeOperation(
    filteredRows,
    definition.operation as Operation,
    definition.fieldPath
  );
  const normalizedResult = applyNormalization(rawResult, definition.normalization);
  const warningMessage =
    definition.groupBy && definition.groupBy.length > 0
      ? "groupBy presente ma non ancora applicato in questa versione"
      : undefined;

  return {
    inputCount: filteredRows.length,
    rawResult,
    normalizedResult,
    warningMessage,
    filteredRows,
  };
}

export const createSnapshotProfile = mutation({
  args: {
    slug: v.string(),
    name: v.string(),
    description: v.optional(v.string()),
    isActive: v.optional(v.boolean()),
  },
  returns: v.id("snapshotProfiles"),
  handler: async (ctx, args) => {
    const now = Date.now();
    const existing = await ctx.db
      .query("snapshotProfiles")
      .withIndex("by_slug", (q) => q.eq("slug", args.slug))
      .unique();
    if (existing) {
      await ctx.db.patch(existing._id, {
        name: args.name,
        description: args.description,
        isActive: args.isActive ?? existing.isActive,
        version: existing.version + 1,
        updatedAt: now,
      });
      return existing._id;
    }
    return await ctx.db.insert("snapshotProfiles", {
      slug: args.slug,
      name: args.name,
      description: args.description,
      isActive: args.isActive ?? true,
      version: 1,
      createdAt: now,
    });
  },
});

export const listSnapshotProfiles = query({
  args: {},
  returns: v.array(
    v.object({
      _id: v.id("snapshotProfiles"),
      _creationTime: v.number(),
      slug: v.string(),
      name: v.string(),
      description: v.optional(v.string()),
      isActive: v.boolean(),
      version: v.number(),
      createdAt: v.number(),
      updatedAt: v.optional(v.number()),
    })
  ),
  handler: async (ctx) => {
    return await ctx.db.query("snapshotProfiles").collect();
  },
});

export const upsertDataSource = mutation({
  args: {
    profileSlug: v.string(),
    sourceKey: v.string(),
    label: v.string(),
    sourceKind: sourceKindValidator,
    metadata: v.optional(v.any()),
    enabled: v.optional(v.boolean()),
  },
  returns: v.id("dataSources"),
  handler: async (ctx, args) => {
    const profile = await getProfileBySlug(ctx, args.profileSlug);
    const existing = await ctx.db
      .query("dataSources")
      .withIndex("by_profile_and_source_key", (q) =>
        q.eq("profileId", profile._id).eq("sourceKey", args.sourceKey)
      )
      .unique();
    const now = Date.now();
    if (existing) {
      await ctx.db.patch(existing._id, {
        label: args.label,
        sourceKind: args.sourceKind,
        metadata: args.metadata,
        enabled: args.enabled ?? existing.enabled,
        updatedAt: now,
      });
      return existing._id;
    }
    return await ctx.db.insert("dataSources", {
      profileId: profile._id,
      sourceKey: args.sourceKey,
      label: args.label,
      sourceKind: args.sourceKind,
      metadata: args.metadata,
      enabled: args.enabled ?? true,
      createdAt: now,
    });
  },
});

export const upsertIndicator = mutation({
  args: {
    profileSlug: v.string(),
    slug: v.string(),
    label: v.string(),
    unit: v.optional(v.string()),
    category: v.optional(v.string()),
    description: v.optional(v.string()),
    externalId: v.optional(v.string()),
    enabled: v.optional(v.boolean()),
  },
  returns: v.id("indicators"),
  handler: async (ctx, args) => {
    const profile = await getProfileBySlug(ctx, args.profileSlug);
    const existing = await getIndicatorByProfileAndSlug(ctx, profile._id, args.slug);
    if (args.externalId) {
      const existingExternal = await ctx.db
        .query("indicators")
        .withIndex("by_external_id", (q) => q.eq("externalId", args.externalId))
        .unique();
      if (existingExternal && existingExternal._id !== existing?._id) {
        throw new Error(`externalId '${args.externalId}' già associato a un altro indicatore`);
      }
    }
    const now = Date.now();
    if (existing) {
      await ctx.db.patch(existing._id, {
        label: args.label,
        unit: args.unit,
        category: args.category,
        description: args.description,
        externalId: args.externalId ?? existing.externalId,
        enabled: args.enabled ?? existing.enabled,
        updatedAt: now,
      });
      return existing._id;
    }
    return await ctx.db.insert("indicators", {
      profileId: profile._id,
      slug: args.slug,
      label: args.label,
      unit: args.unit,
      category: args.category,
      description: args.description,
      externalId: args.externalId,
      enabled: args.enabled ?? true,
      createdAt: now,
    });
  },
});

export const getIndicatorBySlug = query({
  args: {
    profileSlug: v.string(),
    slug: v.string(),
  },
  handler: async (ctx, args) => {
    const profile = await getProfileBySlug(ctx, args.profileSlug);
    return await getIndicatorByProfileAndSlug(ctx, profile._id, args.slug);
  },
});

export const getIndicatorByExternalId = query({
  args: {
    externalId: v.string(),
  },
  handler: async (ctx, args) => {
    return await ctx.db
      .query("indicators")
      .withIndex("by_external_id", (q) => q.eq("externalId", args.externalId))
      .unique();
  },
});

export const setIndicatorExternalId = mutation({
  args: {
    indicatorId: v.id("indicators"),
    externalId: v.string(),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    const indicator = await ctx.db.get(args.indicatorId);
    if (!indicator) {
      throw new Error("Indicatore non trovato");
    }
    const existing = await ctx.db
      .query("indicators")
      .withIndex("by_external_id", (q) => q.eq("externalId", args.externalId))
      .unique();
    if (existing && existing._id !== indicator._id) {
      throw new Error(`externalId '${args.externalId}' già associato a un altro indicatore`);
    }
    await ctx.db.patch(args.indicatorId, {
      externalId: args.externalId,
      updatedAt: Date.now(),
    });
    return null;
  },
});

export const upsertCalculationDefinition = mutation({
  args: {
    profileSlug: v.string(),
    indicatorSlug: v.string(),
    sourceKey: v.string(),
    operation: operationValidator,
    fieldPath: v.optional(v.string()),
    filters: v.optional(v.any()),
    groupBy: v.optional(v.array(v.string())),
    normalization: v.optional(v.any()),
    priority: v.optional(v.number()),
    enabled: v.optional(v.boolean()),
    ruleVersion: v.optional(v.number()),
  },
  returns: v.id("calculationDefinitions"),
  handler: async (ctx, args) => {
    const profile = await getProfileBySlug(ctx, args.profileSlug);
    const indicator = await ctx.db
      .query("indicators")
      .withIndex("by_profile_and_slug", (q) =>
        q.eq("profileId", profile._id).eq("slug", args.indicatorSlug)
      )
      .unique();
    if (!indicator) {
      throw new Error(`Indicatore '${args.indicatorSlug}' non trovato`);
    }
    const dataSource = await ctx.db
      .query("dataSources")
      .withIndex("by_profile_and_source_key", (q) =>
        q.eq("profileId", profile._id).eq("sourceKey", args.sourceKey)
      )
      .unique();
    if (!dataSource) {
      throw new Error(`Data source '${args.sourceKey}' non trovata`);
    }

    const existingForIndicator = await ctx.db
      .query("calculationDefinitions")
      .withIndex("by_profile_and_indicator", (q) =>
        q.eq("profileId", profile._id).eq("indicatorId", indicator._id)
      )
      .collect();

    const existing = existingForIndicator.find((definition) => {
      return (
        definition.dataSourceId === dataSource._id &&
        definition.operation === args.operation &&
        definition.fieldPath === args.fieldPath
      );
    });

    const now = Date.now();
    if (existing) {
      await ctx.db.patch(existing._id, {
        filters: args.filters,
        groupBy: args.groupBy,
        normalization: args.normalization,
        priority: args.priority ?? existing.priority,
        enabled: args.enabled ?? existing.enabled,
        ruleVersion: args.ruleVersion ?? existing.ruleVersion + 1,
        updatedAt: now,
      });
      return existing._id;
    }

    return await ctx.db.insert("calculationDefinitions", {
      profileId: profile._id,
      indicatorId: indicator._id,
      dataSourceId: dataSource._id,
      operation: args.operation,
      fieldPath: args.fieldPath,
      filters: args.filters,
      groupBy: args.groupBy,
      normalization: args.normalization,
      priority: args.priority ?? 100,
      enabled: args.enabled ?? true,
      ruleVersion: args.ruleVersion ?? 1,
      createdAt: now,
    });
  },
});

export const toggleCalculation = mutation({
  args: {
    definitionId: v.id("calculationDefinitions"),
    enabled: v.boolean(),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    await ctx.db.patch(args.definitionId, {
      enabled: args.enabled,
      updatedAt: Date.now(),
    });
    return null;
  },
});

export const replaceProfileDefinitions = mutation({
  args: {
    profileSlug: v.string(),
    definitions: v.array(
      v.object({
        indicatorSlug: v.string(),
        sourceKey: v.string(),
        operation: operationValidator,
        fieldPath: v.optional(v.string()),
        filters: v.optional(v.any()),
        groupBy: v.optional(v.array(v.string())),
        normalization: v.optional(v.any()),
        priority: v.optional(v.number()),
        enabled: v.optional(v.boolean()),
        ruleVersion: v.optional(v.number()),
      })
    ),
  },
  returns: v.object({
    deleted: v.number(),
    created: v.number(),
  }),
  handler: async (ctx, args) => {
    const profile = await getProfileBySlug(ctx, args.profileSlug);
    const existing = await ctx.db
      .query("calculationDefinitions")
      .withIndex("by_profile", (q) => q.eq("profileId", profile._id))
      .collect();
    for (const definition of existing) {
      await ctx.db.delete(definition._id);
    }

    let created = 0;
    for (const definition of args.definitions) {
      const indicator = await ctx.db
        .query("indicators")
        .withIndex("by_profile_and_slug", (q) =>
          q.eq("profileId", profile._id).eq("slug", definition.indicatorSlug)
        )
        .unique();
      if (!indicator) {
        throw new Error(`Indicatore '${definition.indicatorSlug}' non trovato`);
      }
      const dataSource = await ctx.db
        .query("dataSources")
        .withIndex("by_profile_and_source_key", (q) =>
          q.eq("profileId", profile._id).eq("sourceKey", definition.sourceKey)
        )
        .unique();
      if (!dataSource) {
        throw new Error(`Data source '${definition.sourceKey}' non trovata`);
      }
      await ctx.db.insert("calculationDefinitions", {
        profileId: profile._id,
        indicatorId: indicator._id,
        dataSourceId: dataSource._id,
        operation: definition.operation,
        fieldPath: definition.fieldPath,
        filters: definition.filters,
        groupBy: definition.groupBy,
        normalization: definition.normalization,
        priority: definition.priority ?? 100,
        enabled: definition.enabled ?? true,
        ruleVersion: definition.ruleVersion ?? 1,
        createdAt: Date.now(),
      });
      created++;
    }
    return { deleted: existing.length, created };
  },
});

export const listProfileDefinitions = query({
  args: {
    profileSlug: v.string(),
  },
  handler: async (ctx, args) => {
    const profile = await getProfileBySlug(ctx, args.profileSlug);
    const [indicators, dataSources, definitions] = await Promise.all([
      ctx.db
        .query("indicators")
        .withIndex("by_profile", (q) => q.eq("profileId", profile._id))
        .collect(),
      ctx.db
        .query("dataSources")
        .withIndex("by_profile", (q) => q.eq("profileId", profile._id))
        .collect(),
      ctx.db
        .query("calculationDefinitions")
        .withIndex("by_profile_and_priority", (q) => q.eq("profileId", profile._id))
        .collect(),
    ]);

    const indicatorsById = new Map(indicators.map((indicator) => [indicator._id, indicator]));
    const dataSourcesById = new Map(dataSources.map((source) => [source._id, source]));

    return {
      profile,
      indicators,
      dataSources,
      definitions: definitions.map((definition) => ({
        ...definition,
        indicatorSlug: indicatorsById.get(definition.indicatorId)?.slug ?? null,
        sourceKey: dataSourcesById.get(definition.dataSourceId)?.sourceKey ?? null,
      })),
    };
  },
});

export const listProfileDataSources = query({
  args: {
    profileSlug: v.string(),
  },
  handler: async (ctx, args) => {
    const profile = await getProfileBySlug(ctx, args.profileSlug);
    return await ctx.db
      .query("dataSources")
      .withIndex("by_profile", (q) => q.eq("profileId", profile._id))
      .collect();
  },
});

export const ingestSourceRows = mutation({
  args: {
    profileSlug: v.string(),
    sourceKey: v.string(),
    rows: v.array(sourceRowInputValidator),
  },
  returns: v.object({
    inserted: v.number(),
  }),
  handler: async (_ctx, _args) => {
    throw new Error(
      "ingestSourceRows è stato rimosso in kpi-snapshot@1.0.0. " +
        "Passa i payload sorgente direttamente a createSnapshot/simulateSnapshot."
    );
  },
});

export const simulateSnapshot = query({
  args: {
    profileSlug: v.string(),
    snapshotAt: v.optional(v.number()),
    sourcePayloads: v.optional(v.array(sourcePayloadValidator)),
  },
  handler: async (ctx, args) => {
    const profile = await getProfileBySlug(ctx, args.profileSlug);
    const snapshotAt = args.snapshotAt ?? Date.now();
    const [definitions, indicators, dataSources] = await Promise.all([
      ctx.db
        .query("calculationDefinitions")
        .withIndex("by_profile_and_priority", (q) => q.eq("profileId", profile._id))
        .collect(),
      ctx.db
        .query("indicators")
        .withIndex("by_profile", (q) => q.eq("profileId", profile._id))
        .collect(),
      ctx.db
        .query("dataSources")
        .withIndex("by_profile", (q) => q.eq("profileId", profile._id))
        .collect(),
    ]);

    const enabledDefinitions = definitions.filter((definition) => definition.enabled);
    const indicatorsById = new Map(indicators.map((indicator) => [indicator._id, indicator]));
    const dataSourcesById = new Map(dataSources.map((source) => [source._id, source]));
    const payloadBySourceKey = new Map(
      (args.sourcePayloads ?? []).map((payload) => [payload.sourceKey, payload.rows])
    );
    const rowsByDataSource = new Map<Id<"dataSources">, Array<SourceRowInput>>();
    for (const source of dataSources) {
      const payloadRows = payloadBySourceKey.get(source.sourceKey) ?? [];
      rowsByDataSource.set(source._id, normalizeSourceRows(payloadRows, snapshotAt));
    }

    return enabledDefinitions.map((definition) => {
      const rows = rowsByDataSource.get(definition.dataSourceId) ?? [];
      const result = runSingleDefinition(definition, rows);
      return {
        definitionId: definition._id,
        indicatorId: definition.indicatorId,
        indicatorSlug: indicatorsById.get(definition.indicatorId)?.slug ?? null,
        sourceKey: dataSourcesById.get(definition.dataSourceId)?.sourceKey ?? null,
        operation: definition.operation,
        fieldPath: definition.fieldPath,
        inputCount: result.inputCount,
        rawResult: result.rawResult,
        normalizedResult: result.normalizedResult,
        warningMessage: result.warningMessage,
      };
    });
  },
});

export const createSnapshotRun = mutation({
  args: {
    profileSlug: v.string(),
    snapshotAt: v.optional(v.number()),
    triggeredBy: v.optional(v.string()),
    note: v.optional(v.string()),
    sourcePayloads: v.array(sourcePayloadValidator),
  },
  returns: v.object({
    snapshotId: v.id("snapshots"),
    snapshotRunId: v.id("snapshotRuns"),
    status: v.union(v.literal("success"), v.literal("error")),
    processedCount: v.number(),
    errorsCount: v.number(),
    evidencePayloads: v.array(evidenceUploadValidator),
  }),
  handler: async (ctx, args) => {
    const profile = await getProfileBySlug(ctx, args.profileSlug);
    const snapshotAt = args.snapshotAt ?? Date.now();
    const now = Date.now();
    const dataSources = await ctx.db
      .query("dataSources")
      .withIndex("by_profile", (q) => q.eq("profileId", profile._id))
      .collect();
    const dataSourcesById = new Map(dataSources.map((source) => [source._id, source]));
    const payloadBySourceKey = new Map(
      args.sourcePayloads.map((payload) => [payload.sourceKey, normalizeSourceRows(payload.rows, snapshotAt)])
    );
    const snapshotId = await ctx.db.insert("snapshots", {
      profileId: profile._id,
      snapshotAt,
      status: "running",
      note: args.note,
      triggeredBy: args.triggeredBy,
      createdAt: now,
    });

    const definitions = await ctx.db
      .query("calculationDefinitions")
      .withIndex("by_profile_and_priority", (q) => q.eq("profileId", profile._id))
      .collect();
    const indicators = await ctx.db
      .query("indicators")
      .withIndex("by_profile", (q) => q.eq("profileId", profile._id))
      .collect();
    const indicatorsById = new Map(indicators.map((indicator) => [indicator._id, indicator]));
    const enabledDefinitions = definitions.filter((definition) => definition.enabled);

    const snapshotRunId = await ctx.db.insert("snapshotRuns", {
      snapshotId,
      profileId: profile._id,
      startedAt: now,
      status: "running",
      triggeredBy: args.triggeredBy,
      definitionsCount: enabledDefinitions.length,
      processedCount: 0,
    });

    let processedCount = 0;
    let errorsCount = 0;
    const evidencePayloads: Array<{
      snapshotRunItemId: Id<"snapshotRunItems">;
      snapshotValueId: Id<"snapshotValues">;
      fileName: string;
      rowCount: number;
      csvContent: string;
    }> = [];
    for (const definition of enabledDefinitions) {
      const startedAt = Date.now();
      const dataSource = dataSourcesById.get(definition.dataSourceId);
      const dataSourceRows = dataSource
        ? payloadBySourceKey.get(dataSource.sourceKey) ?? []
        : [];

      try {
        const result = runSingleDefinition(definition, dataSourceRows);
        const durationMs = Date.now() - startedAt;
        const ruleHash = buildRuleHash(definition);
        const indicator = indicatorsById.get(definition.indicatorId);
        if (!indicator) {
          throw new Error(`Indicatore non trovato per definitionId '${definition._id}'`);
        }
        const indicatorLabelSnapshot = normalizeIndicatorLabelSnapshot(indicator.label);
        const itemId = await ctx.db.insert("snapshotRunItems", {
          snapshotRunId,
          snapshotId,
          profileId: profile._id,
          definitionId: definition._id,
          indicatorId: definition.indicatorId,
          dataSourceId: definition.dataSourceId,
          status: result.normalizedResult == null ? "skipped" : "success",
          inputCount: result.inputCount,
          rawResult: result.rawResult ?? undefined,
          normalizedResult: result.normalizedResult ?? undefined,
          durationMs,
          warningMessage: result.warningMessage,
          ruleHash,
          createdAt: Date.now(),
        });
        if (result.normalizedResult != null) {
          const evidenceFileName = [
            profile.slug,
            indicator.slug,
            definition._id,
            snapshotAt,
          ].join("__") + ".csv";
          const snapshotValueId = await ctx.db.insert("snapshotValues", {
            snapshotId,
            snapshotRunId,
            snapshotRunItemId: itemId,
            profileId: profile._id,
            indicatorId: definition.indicatorId,
            indicatorLabelSnapshot,
            value: result.normalizedResult,
            computedAt: Date.now(),
            ruleHash,
            explainRef: `runItem:${itemId}`,
          });
          evidencePayloads.push({
            snapshotRunItemId: itemId,
            snapshotValueId,
            fileName: evidenceFileName,
            rowCount: result.filteredRows.length,
            csvContent: buildEvidenceCsv(result.filteredRows),
          });
          await ctx.db.insert("values", {
            indicatorId: definition.indicatorId,
            value: result.normalizedResult,
            measuredAt: snapshotAt,
            createdAt: Date.now(),
          });
        }
        await ctx.db.insert("calculationTraces", {
          snapshotRunId,
          snapshotRunItemId: itemId,
          profileId: profile._id,
          definitionId: definition._id,
          queryParams: {
            snapshotAt,
            dataSourceId: definition.dataSourceId,
            sourceKey: dataSource?.sourceKey ?? null,
          },
          resolvedFilters: definition.filters,
          sampleRowsPreview: result.filteredRows.slice(0, 10),
          warnings: result.warningMessage ? [result.warningMessage] : undefined,
          createdAt: Date.now(),
        });
      } catch (error) {
        errorsCount++;
        const durationMs = Date.now() - startedAt;
        const itemId = await ctx.db.insert("snapshotRunItems", {
          snapshotRunId,
          snapshotId,
          profileId: profile._id,
          definitionId: definition._id,
          indicatorId: definition.indicatorId,
          dataSourceId: definition.dataSourceId,
          status: "error",
          inputCount: dataSourceRows.length,
          durationMs,
          errorMessage: error instanceof Error ? error.message : String(error),
          ruleHash: buildRuleHash(definition),
          createdAt: Date.now(),
        });
        await ctx.db.insert("calculationTraces", {
          snapshotRunId,
          snapshotRunItemId: itemId,
          profileId: profile._id,
          definitionId: definition._id,
          queryParams: {
            snapshotAt,
            dataSourceId: definition.dataSourceId,
            sourceKey: dataSource?.sourceKey ?? null,
          },
          resolvedFilters: definition.filters,
          sampleRowsPreview: dataSourceRows.slice(0, 10),
          warnings: ["calcolo terminato con errore"],
          notes: error instanceof Error ? error.message : String(error),
          createdAt: Date.now(),
        });
      }
      processedCount++;
    }

    const endStatus: "success" | "error" = errorsCount > 0 ? "error" : "success";
    await ctx.db.patch(snapshotRunId, {
      finishedAt: Date.now(),
      status: endStatus,
      errorMessage: errorsCount > 0 ? `${errorsCount} regole in errore` : undefined,
      processedCount,
    });
    await ctx.db.patch(snapshotId, {
      finishedAt: Date.now(),
      status: endStatus,
      errorMessage: errorsCount > 0 ? `${errorsCount} regole in errore` : undefined,
    });

    return {
      snapshotId,
      snapshotRunId,
      status: endStatus,
      processedCount,
      errorsCount,
      evidencePayloads,
    };
  },
});

export const attachSnapshotValueEvidence = mutation({
  args: {
    uploads: v.array(
      v.object({
        snapshotRunItemId: v.id("snapshotRunItems"),
        snapshotValueId: v.id("snapshotValues"),
        storageId: v.id("_storage"),
        fileName: v.string(),
        rowCount: v.number(),
        mimeType: v.string(),
        sha256: v.optional(v.string()),
      })
    ),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    const generatedAt = Date.now();
    for (const upload of args.uploads) {
      const evidenceRef = String(upload.storageId);
      await ctx.db.patch(upload.snapshotRunItemId, {
        evidenceRef,
        evidenceFileName: upload.fileName,
        evidenceRowCount: upload.rowCount,
        evidenceGeneratedAt: generatedAt,
        evidenceMimeType: upload.mimeType,
        evidenceSha256: upload.sha256,
      });
      await ctx.db.patch(upload.snapshotValueId, {
        evidenceRef,
        evidenceFileName: upload.fileName,
        evidenceRowCount: upload.rowCount,
        evidenceGeneratedAt: generatedAt,
        evidenceMimeType: upload.mimeType,
        evidenceSha256: upload.sha256,
      });
    }
    return null;
  },
});

export const getValueByExternalId = query({
  args: {
    externalId: v.string(),
  },
  handler: async (ctx, args) => {
    return await ctx.db
      .query("values")
      .withIndex("by_external_id", (q) => q.eq("externalId", args.externalId))
      .unique();
  },
});

export const listValuesForSync = query({
  args: {
    profileSlug: v.optional(v.string()),
    limit: v.optional(v.number()),
  },
  handler: async (ctx, args) => {
    const limit = Math.max(1, Math.min(args.limit ?? 50, 200));
    const scanLimit = Math.min(limit * 5, 1000);
    const profile = args.profileSlug ? await getProfileBySlug(ctx, args.profileSlug) : null;
    const recentValues = await ctx.db.query("values").order("desc").take(scanLimit);

    const results: Array<
      Doc<"values"> & {
        indicatorExternalId?: string;
        indicatorSlug?: string;
        profileSlug?: string;
      }
    > = [];

    for (const valueRow of recentValues) {
      if (valueRow.externalId) {
        continue;
      }
      const indicator = await ctx.db.get(valueRow.indicatorId);
      if (!indicator) {
        continue;
      }
      if (profile && indicator.profileId !== profile._id) {
        continue;
      }
      results.push({
        ...valueRow,
        indicatorExternalId: indicator.externalId,
        indicatorSlug: indicator.slug,
        profileSlug: profile?.slug,
      });
      if (results.length >= limit) {
        break;
      }
    }

    return results;
  },
});

export const setValueExternalId = mutation({
  args: {
    valueId: v.id("values"),
    externalId: v.string(),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    const valueRow = await ctx.db.get(args.valueId);
    if (!valueRow) {
      throw new Error("Valore non trovato");
    }
    const existing = await ctx.db
      .query("values")
      .withIndex("by_external_id", (q) => q.eq("externalId", args.externalId))
      .unique();
    if (existing && existing._id !== valueRow._id) {
      throw new Error(`externalId '${args.externalId}' già associato a un altro valore`);
    }
    await ctx.db.patch(args.valueId, {
      externalId: args.externalId,
    });
    return null;
  },
});

export const listSnapshots = query({
  args: {
    profileSlug: v.optional(v.string()),
    limit: v.optional(v.number()),
  },
  handler: async (ctx, args) => {
    const limit = args.limit ?? 20;
    if (!args.profileSlug) {
      return await ctx.db.query("snapshots").order("desc").take(limit);
    }
    const profile = await getProfileBySlug(ctx, args.profileSlug);
    return await ctx.db
      .query("snapshots")
      .withIndex("by_profile_and_snapshot_at", (q) => q.eq("profileId", profile._id))
      .order("desc")
      .take(limit);
  },
});

export const listSnapshotValues = query({
  args: {
    snapshotId: v.id("snapshots"),
  },
  handler: async (ctx, args) => {
    const values = await ctx.db
      .query("snapshotValues")
      .withIndex("by_snapshot", (q) => q.eq("snapshotId", args.snapshotId))
      .collect();
    return await Promise.all(
      values.map(async (valueRow) => {
        const indicator = await ctx.db.get(valueRow.indicatorId);
        const evidenceDownloadUrl = valueRow.evidenceRef
          ? await ctx.storage.getUrl(valueRow.evidenceRef as any)
          : null;
        return {
          ...valueRow,
          indicatorSlug: indicator?.slug ?? null,
          indicatorUnit: indicator?.unit ?? null,
          evidenceDownloadUrl,
        };
      })
    );
  },
});

export const getSnapshotValueEvidenceDownloadUrl = query({
  args: {
    snapshotValueId: v.id("snapshotValues"),
  },
  returns: v.union(v.string(), v.null()),
  handler: async (ctx, args) => {
    const snapshotValue = await ctx.db.get(args.snapshotValueId);
    if (!snapshotValue?.evidenceRef) {
      return null;
    }
    return await ctx.storage.getUrl(snapshotValue.evidenceRef as any);
  },
});

export const getSnapshotExplain = query({
  args: {
    snapshotId: v.id("snapshots"),
  },
  handler: async (ctx, args) => {
    const snapshot = await ctx.db.get(args.snapshotId);
    if (!snapshot) return null;
    const run = await ctx.db
      .query("snapshotRuns")
      .withIndex("by_snapshot", (q) => q.eq("snapshotId", args.snapshotId))
      .unique();
    if (!run) {
      return {
        snapshot,
        run: null,
        runItems: [],
        traces: [],
        values: [],
      };
    }
    const [runItems, values] = await Promise.all([
      ctx.db
        .query("snapshotRunItems")
        .withIndex("by_snapshot_run", (q) => q.eq("snapshotRunId", run._id))
        .collect(),
      ctx.db
        .query("snapshotValues")
        .withIndex("by_snapshot", (q) => q.eq("snapshotId", args.snapshotId))
        .collect(),
    ]);
    const traces: Array<Doc<"calculationTraces">> = [];
    for (const item of runItems) {
      const trace = await ctx.db
        .query("calculationTraces")
        .withIndex("by_snapshot_run_item", (q) => q.eq("snapshotRunItemId", item._id))
        .unique();
      if (trace) traces.push(trace);
    }
    return {
      snapshot,
      run,
      runItems,
      traces,
      values,
    };
  },
});

export const listSnapshotRunErrors = query({
  args: {
    snapshotRunId: v.id("snapshotRuns"),
  },
  handler: async (ctx, args) => {
    const items = await ctx.db
      .query("snapshotRunItems")
      .withIndex("by_snapshot_run", (q) => q.eq("snapshotRunId", args.snapshotRunId))
      .collect();
    return items.filter((item) => item.status === "error");
  },
});

/**
 * Backfill migration for legacy rows where `snapshotValues.indicatorLabelSnapshot` is missing.
 * Run this on all deployments before making schema field required again.
 */
export const backfillIndicatorLabelSnapshot = mutation({
  args: {
    profileSlug: v.optional(v.string()),
    dryRun: v.optional(v.boolean()),
  },
  returns: v.object({
    scanned: v.number(),
    updated: v.number(),
    skippedAlreadySet: v.number(),
    skippedMissingIndicator: v.number(),
  }),
  handler: async (ctx, args) => {
    const profile = args.profileSlug ? await getProfileBySlug(ctx, args.profileSlug) : null;
    const dryRun = args.dryRun ?? false;

    const [snapshotValues, indicators] = await Promise.all([
      ctx.db.query("snapshotValues").collect(),
      ctx.db.query("indicators").collect(),
    ]);
    const indicatorsById = new Map(indicators.map((indicator) => [indicator._id, indicator]));

    let scanned = 0;
    let updated = 0;
    let skippedAlreadySet = 0;
    let skippedMissingIndicator = 0;

    for (const row of snapshotValues) {
      if (profile && row.profileId !== profile._id) {
        continue;
      }
      scanned++;

      if (row.indicatorLabelSnapshot && row.indicatorLabelSnapshot.trim().length > 0) {
        skippedAlreadySet++;
        continue;
      }

      const indicator = indicatorsById.get(row.indicatorId);
      if (!indicator) {
        skippedMissingIndicator++;
        continue;
      }

      const indicatorLabelSnapshot = normalizeIndicatorLabelSnapshot(indicator.label);
      if (!dryRun) {
        await ctx.db.patch(row._id, { indicatorLabelSnapshot });
      }
      updated++;
    }

    return {
      scanned,
      updated,
      skippedAlreadySet,
      skippedMissingIndicator,
    };
  },
});
