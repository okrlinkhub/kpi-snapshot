import { v } from "convex/values";
import { action } from "./_generated/server.js";
import { components } from "./_generated/api.js";

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
