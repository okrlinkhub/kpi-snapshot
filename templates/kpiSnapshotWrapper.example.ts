/**
 * Template wrapper per @okrlinkhub/kpi-snapshot.
 * Copia questo file in convex/ della tua app (es. come kpiSnapshot.ts), poi:
 * 1. Sostituisci requireAuth(ctx) con il tuo controllo accessi (es. requireAdmin che legge userSettings).
 * 2. Verifica che il nome del componente in convex.config.ts sia "kpiSnapshot" (o adatta components.kpiSnapshot sotto).
 *
 * Il client userà api.kpiSnapshot.* invece di accedere al componente direttamente.
 *
 * Nota: in questo repo _generated non esiste in templates/, quindi gli import sotto possono dare errore;
 * una volta copiato in convex/ della tua app, esegui "npx convex dev" e gli import si risolvono.
 */
// eslint-disable-next-line @typescript-eslint/ban-ts-comment -- template: _generated esiste in convex/ dell'app
// @ts-nocheck
import { query, mutation } from "./_generated/server";
import { v } from "convex/values";
import { components } from "./_generated/api";

async function requireAuth(ctx: { db: any }) {
  // TODO: sostituire con il tuo controllo (es. getAuthenticatedUserId + userSettings.isAdmin)
  // await getAuthenticatedUserId(ctx);
  // const settings = await ctx.db.query("userSettings").withIndex("by_user", (q) => q.eq("userId", userId)).first();
  // if (!settings?.isAdmin) throw new Error("Solo gli admin possono gestire i KPI");
}

export const listProfiles = query({
  args: {},
  handler: async (ctx) => {
    await requireAuth(ctx);
    return await ctx.runQuery(components.kpiSnapshot.snapshotEngine.listSnapshotProfiles, {});
  },
});

export const listProfileDefinitions = query({
  args: { profileSlug: v.string() },
  handler: async (ctx, args) => {
    await requireAuth(ctx);
    return await ctx.runQuery(components.kpiSnapshot.snapshotEngine.listProfileDefinitions, {
      profileSlug: args.profileSlug,
    });
  },
});

export const simulateSnapshot = query({
  args: { profileSlug: v.string(), snapshotAt: v.optional(v.number()) },
  handler: async (ctx, args) => {
    await requireAuth(ctx);
    return await ctx.runQuery(components.kpiSnapshot.snapshotEngine.simulateSnapshot, {
      profileSlug: args.profileSlug,
      snapshotAt: args.snapshotAt,
    });
  },
});

export const listSnapshots = query({
  args: { profileSlug: v.optional(v.string()), limit: v.optional(v.number()) },
  handler: async (ctx, args) => {
    await requireAuth(ctx);
    return await ctx.runQuery(components.kpiSnapshot.snapshotEngine.listSnapshots, {
      profileSlug: args.profileSlug,
      limit: args.limit,
    });
  },
});

export const getSnapshotExplain = query({
  args: { snapshotId: v.string() },
  handler: async (ctx, args) => {
    await requireAuth(ctx);
    return await ctx.runQuery(components.kpiSnapshot.snapshotEngine.getSnapshotExplain, {
      snapshotId: args.snapshotId,
    });
  },
});

export const createSnapshot = mutation({
  args: {
    profileSlug: v.string(),
    snapshotAt: v.optional(v.number()),
    note: v.optional(v.string()),
    triggeredBy: v.optional(v.string()),
  },
  handler: async (ctx, args) => {
    await requireAuth(ctx);
    return await ctx.runMutation(components.kpiSnapshot.snapshotEngine.createSnapshot, {
      profileSlug: args.profileSlug,
      snapshotAt: args.snapshotAt,
      note: args.note,
      triggeredBy: args.triggeredBy,
    });
  },
});

export const ingestSourceRows = mutation({
  args: {
    profileSlug: v.string(),
    sourceKey: v.string(),
    rows: v.array(v.object({ occurredAt: v.number(), rowData: v.any() })),
  },
  handler: async (ctx, args) => {
    await requireAuth(ctx);
    return await ctx.runMutation(components.kpiSnapshot.snapshotEngine.ingestSourceRows, {
      profileSlug: args.profileSlug,
      sourceKey: args.sourceKey,
      rows: args.rows,
    });
  },
});

// Aggiungi altri pass-through se ti servono: createSnapshotProfile, upsertDataSource, upsertIndicator, upsertCalculationDefinition, replaceProfileDefinitions, toggleCalculation, listSnapshotRunErrors.
