import { v } from "convex/values";
import { mutation, query } from "./_generated/server.js";
import { components } from "./_generated/api.js";
import type { Id } from "./_generated/dataModel.js";

const DEMO_PROFILE = "finance_demo";
const INVOICE_SOURCE = "invoices";

function isProfileMissingError(error: unknown): boolean {
  const message =
    error && typeof error === "object" && "message" in error
      ? String((error as { message?: unknown }).message ?? "")
      : String(error ?? "");
  return message.includes("Snapshot profile") && message.includes("non trovato");
}

export const setupDefaultSnapshotConfig = mutation({
  args: {
    profileSlug: v.optional(v.string()),
  },
  returns: v.object({
    profileSlug: v.string(),
  }),
  handler: async (ctx, args) => {
    const profileSlug = args.profileSlug ?? DEMO_PROFILE;
    const kpiComponent = (components as any).kpiSnapshot;

    await ctx.runMutation(kpiComponent.snapshotEngine.createSnapshotProfile, {
      slug: profileSlug,
      name: "Finance Demo",
      description: "Profilo demo per snapshot KPI da invoices",
      isActive: true,
    });

    await ctx.runMutation(kpiComponent.snapshotEngine.upsertDataSource, {
      profileSlug,
      sourceKey: INVOICE_SOURCE,
      label: "Invoices materialized rows",
      sourceKind: "materialized_rows",
      enabled: true,
    });

    const indicators = [
      { slug: "invoice_total", label: "Invoice Total", unit: "EUR", category: "finance" },
      { slug: "invoice_count", label: "Invoice Count", unit: "items", category: "finance" },
      { slug: "invoice_max", label: "Invoice Max", unit: "EUR", category: "finance" },
      { slug: "invoice_avg", label: "Invoice Avg", unit: "EUR", category: "finance" },
    ];

    for (const indicator of indicators) {
      await ctx.runMutation(kpiComponent.snapshotEngine.upsertIndicator, {
        profileSlug,
        ...indicator,
        enabled: true,
      });
    }

    await ctx.runMutation(kpiComponent.snapshotEngine.replaceProfileDefinitions, {
      profileSlug,
      definitions: [
        {
          indicatorSlug: "invoice_total",
          sourceKey: INVOICE_SOURCE,
          operation: "sum",
          fieldPath: "amount",
          normalization: { round: 2 },
          priority: 10,
          enabled: true,
          ruleVersion: 1,
        },
        {
          indicatorSlug: "invoice_count",
          sourceKey: INVOICE_SOURCE,
          operation: "count",
          normalization: { coalesce: 0 },
          priority: 20,
          enabled: true,
          ruleVersion: 1,
        },
        {
          indicatorSlug: "invoice_max",
          sourceKey: INVOICE_SOURCE,
          operation: "max",
          fieldPath: "amount",
          normalization: { round: 2 },
          priority: 30,
          enabled: true,
          ruleVersion: 1,
        },
        {
          indicatorSlug: "invoice_avg",
          sourceKey: INVOICE_SOURCE,
          operation: "avg",
          fieldPath: "amount",
          normalization: { round: 2, coalesce: 0 },
          priority: 40,
          enabled: true,
          ruleVersion: 1,
        },
      ],
    });

    return { profileSlug };
  },
});

export const seedInvoices = mutation({
  args: {
    profileSlug: v.optional(v.string()),
    count: v.optional(v.number()),
  },
  returns: v.object({
    inserted: v.number(),
    profileSlug: v.string(),
  }),
  handler: async (ctx, args) => {
    const profileSlug = args.profileSlug ?? DEMO_PROFILE;
    const count = Math.max(1, Math.trunc(args.count ?? 40));
    const categories = ["software", "services", "operations", "marketing"] as const;
    const now = Date.now();

    const rows: Array<{ occurredAt: number; rowData: Record<string, unknown> }> = [];
    for (let i = 0; i < count; i++) {
      const category = categories[i % categories.length];
      const issuedAt = now - (i % 30) * 24 * 60 * 60 * 1000;
      const amount = 150 + (i * 137) % 5000;
      await ctx.db.insert("invoices", {
        profileSlug,
        category,
        amount,
        issuedAt,
        description: `Invoice ${i + 1} - ${category}`,
      });
      rows.push({
        occurredAt: issuedAt,
        rowData: {
          category,
          amount,
          issuedAt,
        },
      });
    }

    const kpiComponent = (components as any).kpiSnapshot;
    await ctx.runMutation(kpiComponent.snapshotEngine.ingestSourceRows, {
      profileSlug,
      sourceKey: INVOICE_SOURCE,
      rows,
    });

    return {
      inserted: count,
      profileSlug,
    };
  },
});

export const listInvoices = query({
  args: {
    profileSlug: v.optional(v.string()),
    limit: v.optional(v.number()),
  },
  handler: async (ctx, args) => {
    const profileSlug = args.profileSlug ?? DEMO_PROFILE;
    const limit = args.limit ?? 100;
    return await ctx.db
      .query("invoices")
      .withIndex("by_profile_and_issued_at", (q) => q.eq("profileSlug", profileSlug))
      .order("desc")
      .take(limit);
  },
});

export const listInvoicesByCategory = query({
  args: {
    profileSlug: v.optional(v.string()),
  },
  handler: async (ctx, args) => {
    const profileSlug = args.profileSlug ?? DEMO_PROFILE;
    const rows = await ctx.db
      .query("invoices")
      .withIndex("by_profile", (q) => q.eq("profileSlug", profileSlug))
      .collect();

    const byCategory: Record<string, { count: number; total: number; max: number }> = {};
    for (const row of rows) {
      if (!byCategory[row.category]) {
        byCategory[row.category] = { count: 0, total: 0, max: 0 };
      }
      byCategory[row.category].count += 1;
      byCategory[row.category].total += row.amount;
      byCategory[row.category].max = Math.max(byCategory[row.category].max, row.amount);
    }
    return Object.entries(byCategory).map(([category, stats]) => ({
      category,
      count: stats.count,
      total: Number(stats.total.toFixed(2)),
      max: stats.max,
    }));
  },
});

export const simulateSnapshot = query({
  args: {
    profileSlug: v.optional(v.string()),
    snapshotAt: v.optional(v.number()),
  },
  handler: async (ctx, args) => {
    const profileSlug = args.profileSlug ?? DEMO_PROFILE;
    const kpiComponent = (components as any).kpiSnapshot;
    try {
      return await ctx.runQuery(kpiComponent.snapshotEngine.simulateSnapshot, {
        profileSlug,
        snapshotAt: args.snapshotAt,
      });
    } catch (error) {
      if (isProfileMissingError(error)) {
        return [];
      }
      throw error;
    }
  },
});

export const createManualSnapshot = mutation({
  args: {
    profileSlug: v.optional(v.string()),
    snapshotAt: v.optional(v.number()),
    note: v.optional(v.string()),
  },
  handler: async (ctx, args) => {
    const profileSlug = args.profileSlug ?? DEMO_PROFILE;
    const kpiComponent = (components as any).kpiSnapshot;
    return await ctx.runMutation(kpiComponent.snapshotEngine.createSnapshot, {
      profileSlug,
      snapshotAt: args.snapshotAt,
      note: args.note,
      triggeredBy: "example-ui",
    });
  },
});

export const listSnapshots = query({
  args: {
    profileSlug: v.optional(v.string()),
    limit: v.optional(v.number()),
  },
  handler: async (ctx, args) => {
    const profileSlug = args.profileSlug ?? DEMO_PROFILE;
    const kpiComponent = (components as any).kpiSnapshot;
    try {
      return await ctx.runQuery(kpiComponent.snapshotEngine.listSnapshots, {
        profileSlug,
        limit: args.limit,
      });
    } catch (error) {
      if (isProfileMissingError(error)) {
        return [];
      }
      throw error;
    }
  },
});

export const getSnapshotExplain = query({
  args: {
    snapshotId: v.id("snapshots"),
  },
  handler: async (ctx, args) => {
    const kpiComponent = (components as any).kpiSnapshot;
    return await ctx.runQuery(kpiComponent.snapshotEngine.getSnapshotExplain, args);
  },
});

export const getLatestSnapshotExplain = query({
  args: {
    profileSlug: v.optional(v.string()),
  },
  handler: async (ctx, args) => {
    const profileSlug = args.profileSlug ?? DEMO_PROFILE;
    const kpiComponent = (components as any).kpiSnapshot;
    let snapshots: Array<{ _id: Id<"snapshots"> }> = [];
    try {
      snapshots = await ctx.runQuery(kpiComponent.snapshotEngine.listSnapshots, {
        profileSlug,
        limit: 1,
      });
    } catch (error) {
      if (!isProfileMissingError(error)) {
        throw error;
      }
    }
    if (!snapshots[0]) return null;
    return await ctx.runQuery(kpiComponent.snapshotEngine.getSnapshotExplain, {
      snapshotId: snapshots[0]._id,
    });
  },
});
