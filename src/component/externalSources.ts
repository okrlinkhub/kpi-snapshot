import { v } from "convex/values";
import {
  query,
  mutation,
  internalMutation,
} from "./_generated/server.js";

export const listExternalSources = query({
  args: {},
  returns: v.array(
    v.object({
      _id: v.id("externalSources"),
      _creationTime: v.number(),
      name: v.string(),
      deploymentUrl: v.optional(v.string()),
      linkHubCompanyId: v.string(),
      authType: v.optional(v.string()),
      createdAt: v.number(),
      updatedAt: v.optional(v.number()),
    })
  ),
  handler: async (ctx) => {
    return await ctx.db.query("externalSources").collect();
  },
});

export const addExternalSource = mutation({
  args: {
    name: v.string(),
    deploymentUrl: v.optional(v.string()),
    linkHubCompanyId: v.string(),
    authType: v.optional(v.string()),
  },
  returns: v.id("externalSources"),
  handler: async (ctx, args) => {
    const now = Date.now();
    return await ctx.db.insert("externalSources", {
      name: args.name,
      deploymentUrl: args.deploymentUrl,
      linkHubCompanyId: args.linkHubCompanyId,
      authType: args.authType,
      createdAt: now,
    });
  },
});

export const updateExternalSource = mutation({
  args: {
    id: v.id("externalSources"),
    name: v.optional(v.string()),
    deploymentUrl: v.optional(v.string()),
    linkHubCompanyId: v.optional(v.string()),
    authType: v.optional(v.string()),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    const { id, ...updates } = args;
    const existing = await ctx.db.get(id);
    if (!existing) return null;
    const filtered = Object.fromEntries(
      Object.entries(updates).filter(([, v]) => v !== undefined)
    ) as Record<string, unknown>;
    await ctx.db.patch(id, { ...filtered, updatedAt: Date.now() });
    return null;
  },
});

export const listSyncRuns = query({
  args: {
    externalSourceId: v.optional(v.id("externalSources")),
    limit: v.optional(v.number()),
  },
  returns: v.array(
    v.object({
      _id: v.id("syncRuns"),
      _creationTime: v.number(),
      externalSourceId: v.id("externalSources"),
      startedAt: v.number(),
      finishedAt: v.optional(v.number()),
      status: v.string(),
      errorMessage: v.optional(v.string()),
      valuesSynced: v.optional(v.number()),
    })
  ),
  handler: async (ctx, args) => {
    const limit = args.limit ?? 50;
    const q =
      args.externalSourceId != null
        ? ctx.db
            .query("syncRuns")
            .withIndex("by_external_source", (q) =>
              q.eq("externalSourceId", args.externalSourceId!)
            )
        : ctx.db.query("syncRuns");
    return await q.order("desc").take(limit);
  },
});

export const recordSyncRunStart = internalMutation({
  args: {
    externalSourceId: v.id("externalSources"),
  },
  returns: v.id("syncRuns"),
  handler: async (ctx, args) => {
    const now = Date.now();
    return await ctx.db.insert("syncRuns", {
      externalSourceId: args.externalSourceId,
      startedAt: now,
      status: "running",
    });
  },
});

export const recordSyncRunFinish = internalMutation({
  args: {
    syncRunId: v.id("syncRuns"),
    status: v.union(v.literal("success"), v.literal("error")),
    errorMessage: v.optional(v.string()),
    valuesSynced: v.optional(v.number()),
  },
  returns: v.null(),
  handler: async (ctx, args) => {
    await ctx.db.patch(args.syncRunId, {
      finishedAt: Date.now(),
      status: args.status,
      errorMessage: args.errorMessage,
      valuesSynced: args.valuesSynced,
    });
    return null;
  },
});
