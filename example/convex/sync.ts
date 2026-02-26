import { createFunctionHandle } from "convex/server";
import { v } from "convex/values";
import { action } from "./_generated/server.js";
import { api, components } from "./_generated/api.js";

/** Send a test batch using the example mock writer (no LinkHub needed). */
export const sendTestBatchToLinkHub = action({
  args: {
    linkHubCompanyId: v.string(),
    batch: v.array(
      v.object({
        indicatorSlug: v.string(),
        value: v.number(),
        date: v.number(),
      })
    ),
  },
  returns: v.object({
    synced: v.number(),
    errors: v.array(v.string()),
  }),
  handler: async (ctx, args) => {
    const writeHandle = await createFunctionHandle(api.mockWriter.writeIndicatorValue);
    return await ctx.runAction(components.kpiSnapshot.sync.syncToLinkHub, {
      writeHandle: writeHandle as unknown as string,
      linkHubCompanyId: args.linkHubCompanyId,
      batch: args.batch,
    });
  },
});

export const pullFromExternal = action({
  args: {
    externalSourceId: v.string(),
    since: v.optional(v.number()),
    deploymentUrl: v.optional(v.string()),
    authToken: v.optional(v.string()),
  },
  handler: async (ctx, args) => {
    return await ctx.runAction(components.kpiSnapshot.sync.pullFromExternal, {
      ...args,
      externalSourceId: args.externalSourceId as any,
    });
  },
});

export const syncToLinkHub = action({
  args: {
    writeHandle: v.string(),
    linkHubCompanyId: v.string(),
    batch: v.array(
      v.object({
        indicatorSlug: v.string(),
        value: v.number(),
        date: v.number(),
      })
    ),
  },
  handler: async (ctx, args) => {
    return await ctx.runAction(components.kpiSnapshot.sync.syncToLinkHub, args);
  },
});
