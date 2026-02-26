import { v } from "convex/values";
import { action } from "./_generated/server.js";
import { api, internal } from "./_generated/api.js";
import type { Id } from "./_generated/dataModel.js";

export const pullFromExternal = action({
  args: {
    externalSourceId: v.id("externalSources"),
    /** Optional: fetch only values since this timestamp (ms). */
    since: v.optional(v.number()),
    /** Passed from app: deployment URL or API URL (env not available in component). */
    deploymentUrl: v.optional(v.string()),
    /** Passed from app: bearer token or API key if needed. */
    authToken: v.optional(v.string()),
  },
  returns: v.object({
    syncRunId: v.id("syncRuns"),
    status: v.string(),
    valuesStaged: v.number(),
    errorMessage: v.optional(v.string()),
  }),
  handler: async (ctx, args): Promise<{
    syncRunId: Id<"syncRuns">;
    status: string;
    valuesStaged: number;
    errorMessage?: string;
  }> => {
    const sources = await ctx.runQuery(api.externalSources.listExternalSources, {});
    const externalSource = sources.find(
      (s: { _id: Id<"externalSources"> }) => s._id === args.externalSourceId
    );
    if (!externalSource) {
      throw new Error("External source not found");
    }

    const syncRunId = (await ctx.runMutation(
      internal.externalSources.recordSyncRunStart,
      { externalSourceId: args.externalSourceId }
    )) as Id<"syncRuns">;

    const url = args.deploymentUrl ?? externalSource.deploymentUrl;
    let valuesStaged = 0;
    let errorMessage: string | undefined;

    try {
      // Stub: no HTTP/Convex call to external app yet. When implemented,
      // call external API here and insert rows/values into component tables.
      if (url) {
        // TODO: fetch from url (e.g. Convex HTTP action or runQuery via Convex client)
        // and insert results via internal mutation
      }
      await ctx.runMutation(internal.externalSources.recordSyncRunFinish, {
        syncRunId,
        status: "success",
        valuesSynced: valuesStaged,
      });
    } catch (err) {
      errorMessage = err instanceof Error ? err.message : String(err);
      await ctx.runMutation(internal.externalSources.recordSyncRunFinish, {
        syncRunId,
        status: "error",
        errorMessage,
      });
    }

    return {
      syncRunId,
      status: errorMessage ? "error" : "success",
      valuesStaged,
      errorMessage,
    };
  },
});
