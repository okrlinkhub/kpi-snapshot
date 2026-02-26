import { v } from "convex/values";
import { query, mutation } from "./_generated/server.js";

/** Legacy stub for example UI compatibility. */
export const listNumbers = query({
  args: { count: v.number() },
  handler: async (ctx) => {
    const viewer = (await ctx.auth.getUserIdentity())?.name ?? null;
    return { viewer, numbers: [] };
  },
});

/** Legacy stub for example UI compatibility. */
export const addNumber = mutation({
  args: { value: v.number() },
  handler: async () => {},
});
