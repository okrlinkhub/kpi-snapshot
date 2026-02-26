import { v } from "convex/values";
import { mutation, query } from "./_generated/server.js";

/**
 * Mock writer for testing syncToLinkHub without LinkHub.
 * In production LinkHub would expose a mutation with this contract;
 * the example app uses it so the UI can test the full flow.
 */
export const writeIndicatorValue = mutation({
  args: {
    companyId: v.string(),
    indicatorSlug: v.string(),
    value: v.number(),
    date: v.number(),
  },
  handler: async (ctx, args) => {
    await ctx.db.insert("indicatorValueLog", {
      companyId: args.companyId,
      indicatorSlug: args.indicatorSlug,
      value: args.value,
      date: args.date,
    });
    return null;
  },
});

/** List last received values (mock writer log) for the test UI. */
export const listReceivedValues = query({
  args: { limit: v.optional(v.number()) },
  handler: async (ctx, args) => {
    const limit = args.limit ?? 20;
    return await ctx.db
      .query("indicatorValueLog")
      .order("desc")
      .take(limit);
  },
});
