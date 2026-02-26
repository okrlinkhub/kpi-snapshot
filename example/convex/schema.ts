import { defineSchema, defineTable } from "convex/server";
import { v } from "convex/values";

export default defineSchema({
  invoices: defineTable({
    category: v.string(),
    amount: v.number(),
    issuedAt: v.number(),
    description: v.optional(v.string()),
    profileSlug: v.string(),
  })
    .index("by_profile", ["profileSlug"])
    .index("by_profile_and_category", ["profileSlug", "category"])
    .index("by_profile_and_issued_at", ["profileSlug", "issuedAt"]),
});
