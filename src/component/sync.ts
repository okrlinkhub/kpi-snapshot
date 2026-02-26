import { v } from "convex/values";
import { action } from "./_generated/server.js";
import { api, internal } from "./_generated/api.js";
import type { Id } from "./_generated/dataModel.js";
import type { FunctionReference } from "convex/server";

const valuePayloadValidator = v.object({
  indicatorSlug: v.string(),
  value: v.number(),
  date: v.number(),
});

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
      // call external API here and insert into indicatorSnapshots.
      if (url) {
        // TODO: fetch from url (e.g. Convex HTTP action or runQuery via Convex client)
        // and insert results via internal mutation into indicatorSnapshots
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

export const syncToLinkHub = action({
  args: {
    /** Function handle from LinkHub (createFunctionHandle(api.kpiSnapshot.writeIndicatorValue)). */
    writeHandle: v.string(),
    /** Company id in LinkHub for the write. */
    linkHubCompanyId: v.string(),
    batch: v.array(valuePayloadValidator),
  },
  returns: v.object({
    synced: v.number(),
    errors: v.array(v.string()),
  }),
  handler: async (ctx, args) => {
    const handle = args.writeHandle as unknown as FunctionReference<"mutation">;
    const errors: string[] = [];
    let synced = 0;
    for (const item of args.batch) {
      try {
        await ctx.runMutation(handle, {
          companyId: args.linkHubCompanyId,
          indicatorSlug: item.indicatorSlug,
          value: item.value,
          date: item.date,
        });
        synced++;
      } catch (e) {
        errors.push(
          `${item.indicatorSlug}: ${e instanceof Error ? e.message : String(e)}`
        );
      }
    }
    return { synced, errors };
  },
});
