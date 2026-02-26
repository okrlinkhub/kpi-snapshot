import { v } from "convex/values";
import { query, mutation } from "./_generated/server.js";
import { components } from "./_generated/api.js";

export const listExternalSources = query({
  args: {},
  handler: async (ctx) => {
    return await ctx.runQuery(
      components.kpiSnapshot.externalSources.listExternalSources,
      {}
    );
  },
});

export const addExternalSource = mutation({
  args: {
    name: v.string(),
    deploymentUrl: v.optional(v.string()),
    targetEntityId: v.string(),
    authType: v.optional(v.string()),
  },
  handler: async (ctx, args) => {
    return await ctx.runMutation(
      components.kpiSnapshot.externalSources.addExternalSource,
      args
    );
  },
});

export const updateExternalSource = mutation({
  args: {
    id: v.string(),
    name: v.optional(v.string()),
    deploymentUrl: v.optional(v.string()),
    targetEntityId: v.optional(v.string()),
    authType: v.optional(v.string()),
  },
  handler: async (ctx, args) => {
    await ctx.runMutation(
      components.kpiSnapshot.externalSources.updateExternalSource,
      { ...args, id: args.id as any }
    );
  },
});

export const listSyncRuns = query({
  args: {
    externalSourceId: v.optional(v.string()),
    limit: v.optional(v.number()),
  },
  handler: async (ctx, args) => {
    return await ctx.runQuery(
      components.kpiSnapshot.externalSources.listSyncRuns,
      {
        externalSourceId: args.externalSourceId as any,
        limit: args.limit,
      }
    );
  },
});
