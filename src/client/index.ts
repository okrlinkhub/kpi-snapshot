/**
 * Client entry point per @okrlinkhub/kpi-snapshot.
 * Re-export di tipi e helper per le app che usano il componente.
 */

import {
  actionGeneric,
  mutationGeneric,
  queryGeneric,
} from "convex/server";
import type {
  Auth,
  DefaultFunctionArgs,
  FunctionReference,
} from "convex/server";
import { v } from "convex/values";
import type { ComponentApi } from "../component/_generated/component.js";

export type { ComponentApi } from "../component/_generated/component.js";

const periodicityValidator = v.union(
  v.literal("weekly"),
  v.literal("monthly"),
  v.literal("quarterly"),
  v.literal("semesterly"),
  v.literal("yearly")
);

type MutationRef<
  Args extends DefaultFunctionArgs,
  Result,
  Name extends string | undefined = string | undefined,
> =
  FunctionReference<"mutation", "internal", Args, Result, Name>;

type ActionRef<
  Args extends DefaultFunctionArgs,
  Result,
  Name extends string | undefined = string | undefined,
> =
  FunctionReference<"action", "internal", Args, Result, Name>;

type OkrhubComponentApi<Name extends string | undefined = string | undefined> = {
  okrhub: {
    createIndicator: MutationRef<
      {
        companyExternalId: string;
        description: string;
        isReverse?: boolean;
        metadata?: any;
        periodicity: "weekly" | "monthly" | "quarterly" | "semesterly" | "yearly";
        sourceApp: string;
        sourceUrl: string;
        symbol: string;
      },
      {
        error?: string;
        existing?: boolean;
        externalId: string;
        success: boolean;
      },
      Name
    >;
    createIndicatorValue: MutationRef<
      {
        date: number;
        indicatorExternalId: string;
        sourceApp: string;
        sourceUrl: string;
        value: number;
      },
      {
        error?: string;
        existing?: boolean;
        externalId: string;
        success: boolean;
      },
      Name
    >;
  };
  sync?: {
    processor?: {
      processSyncQueue?: ActionRef<
        {
          batchSize?: number;
        },
        {
          failed: number;
          processed: number;
          succeeded: number;
        },
        Name
      >;
    };
  };
};

type AuthOperation =
  | { type: "read"; entityType: string }
  | { type: "insert" | "update"; entityType: string }
  | { type: "sync"; entityType: string };

export interface ExposeApiOptions<Name extends string | undefined = string | undefined> {
  auth?: (
    ctx: { auth: Auth },
    operation: AuthOperation
  ) => Promise<void>;
  okrhubComponent?: OkrhubComponentApi<Name>;
  okrhub?: {
    sourceApp: string;
    sourceUrl?: string;
    processSyncQueueByDefault?: boolean;
    processSyncQueueBatchSize?: number;
  };
}

function getOkrhubConfig(options?: ExposeApiOptions) {
  return {
    sourceApp: options?.okrhub?.sourceApp,
    sourceUrl: options?.okrhub?.sourceUrl ?? "",
    processSyncQueueByDefault: options?.okrhub?.processSyncQueueByDefault ?? false,
    processSyncQueueBatchSize: options?.okrhub?.processSyncQueueBatchSize ?? 10,
  };
}

function requireOkrhubComponent(options?: ExposeApiOptions) {
  if (!options?.okrhubComponent) {
    throw new Error(
      "Questa funzione richiede `okrhubComponent` nelle opzioni di exposeApi()."
    );
  }
  return options.okrhubComponent;
}

export function exposeApi<Name extends string | undefined = string | undefined>(
  component: ComponentApi<Name>,
  options?: ExposeApiOptions<Name>
) {
  return {
    listProfiles: queryGeneric({
      args: {},
      handler: async (ctx) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "read", entityType: "profile" });
        }
        return await ctx.runQuery(component.snapshotEngine.listSnapshotProfiles, {});
      },
    }),

    listProfileDefinitions: queryGeneric({
      args: { profileSlug: v.string() },
      handler: async (ctx, args) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "read", entityType: "definition" });
        }
        return await ctx.runQuery(component.snapshotEngine.listProfileDefinitions, args);
      },
    }),

    simulateSnapshot: queryGeneric({
      args: { profileSlug: v.string(), snapshotAt: v.optional(v.number()) },
      handler: async (ctx, args) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "read", entityType: "snapshot" });
        }
        return await ctx.runQuery(component.snapshotEngine.simulateSnapshot, args);
      },
    }),

    listSnapshots: queryGeneric({
      args: { profileSlug: v.optional(v.string()), limit: v.optional(v.number()) },
      handler: async (ctx, args) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "read", entityType: "snapshot" });
        }
        return await ctx.runQuery(component.snapshotEngine.listSnapshots, args);
      },
    }),

    getSnapshotExplain: queryGeneric({
      args: { snapshotId: v.string() },
      handler: async (ctx, args) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "read", entityType: "snapshotExplain" });
        }
        return await ctx.runQuery(component.snapshotEngine.getSnapshotExplain, args);
      },
    }),

    listSnapshotRunErrors: queryGeneric({
      args: { snapshotRunId: v.string() },
      handler: async (ctx, args) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "read", entityType: "snapshotRun" });
        }
        return await ctx.runQuery(component.snapshotEngine.listSnapshotRunErrors, args);
      },
    }),

    createSnapshotProfile: mutationGeneric({
      args: {
        slug: v.string(),
        name: v.string(),
        description: v.optional(v.string()),
        isActive: v.optional(v.boolean()),
      },
      handler: async (ctx, args) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "insert", entityType: "profile" });
        }
        return await ctx.runMutation(component.snapshotEngine.createSnapshotProfile, args);
      },
    }),

    upsertDataSource: mutationGeneric({
      args: {
        profileSlug: v.string(),
        sourceKey: v.string(),
        label: v.string(),
        sourceKind: v.union(
          v.literal("component_table"),
          v.literal("external_reader"),
          v.literal("materialized_rows")
        ),
        metadata: v.optional(v.any()),
        enabled: v.optional(v.boolean()),
      },
      handler: async (ctx, args) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "insert", entityType: "dataSource" });
        }
        return await ctx.runMutation(component.snapshotEngine.upsertDataSource, args);
      },
    }),

    upsertIndicator: mutationGeneric({
      args: {
        profileSlug: v.string(),
        slug: v.string(),
        label: v.string(),
        unit: v.optional(v.string()),
        category: v.optional(v.string()),
        description: v.optional(v.string()),
        externalId: v.optional(v.string()),
        enabled: v.optional(v.boolean()),
      },
      handler: async (ctx, args) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "insert", entityType: "indicator" });
        }
        return await ctx.runMutation(component.snapshotEngine.upsertIndicator, args);
      },
    }),

    upsertCalculationDefinition: mutationGeneric({
      args: {
        profileSlug: v.string(),
        indicatorSlug: v.string(),
        sourceKey: v.string(),
        operation: v.union(
          v.literal("sum"),
          v.literal("count"),
          v.literal("avg"),
          v.literal("min"),
          v.literal("max"),
          v.literal("distinct_count")
        ),
        fieldPath: v.optional(v.string()),
        filters: v.optional(v.any()),
        groupBy: v.optional(v.array(v.string())),
        normalization: v.optional(v.any()),
        priority: v.optional(v.number()),
        enabled: v.optional(v.boolean()),
        ruleVersion: v.optional(v.number()),
      },
      handler: async (ctx, args) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "insert", entityType: "definition" });
        }
        return await ctx.runMutation(
          component.snapshotEngine.upsertCalculationDefinition,
          args
        );
      },
    }),

    replaceProfileDefinitions: mutationGeneric({
      args: {
        profileSlug: v.string(),
        definitions: v.array(
          v.object({
            indicatorSlug: v.string(),
            sourceKey: v.string(),
            operation: v.union(
              v.literal("sum"),
              v.literal("count"),
              v.literal("avg"),
              v.literal("min"),
              v.literal("max"),
              v.literal("distinct_count")
            ),
            fieldPath: v.optional(v.string()),
            filters: v.optional(v.any()),
            groupBy: v.optional(v.array(v.string())),
            normalization: v.optional(v.any()),
            priority: v.optional(v.number()),
            enabled: v.optional(v.boolean()),
            ruleVersion: v.optional(v.number()),
          })
        ),
      },
      handler: async (ctx, args) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "update", entityType: "definition" });
        }
        return await ctx.runMutation(component.snapshotEngine.replaceProfileDefinitions, args);
      },
    }),

    toggleCalculation: mutationGeneric({
      args: {
        definitionId: v.string(),
        enabled: v.boolean(),
      },
      handler: async (ctx, args) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "update", entityType: "definition" });
        }
        return await ctx.runMutation(component.snapshotEngine.toggleCalculation, args);
      },
    }),

    createSnapshot: mutationGeneric({
      args: {
        profileSlug: v.string(),
        snapshotAt: v.optional(v.number()),
        triggeredBy: v.optional(v.string()),
        note: v.optional(v.string()),
      },
      handler: async (ctx, args) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "insert", entityType: "snapshot" });
        }
        return await ctx.runMutation(component.snapshotEngine.createSnapshot, args);
      },
    }),

    ingestSourceRows: mutationGeneric({
      args: {
        profileSlug: v.string(),
        sourceKey: v.string(),
        rows: v.array(
          v.object({
            occurredAt: v.number(),
            rowData: v.any(),
          })
        ),
      },
      handler: async (ctx, args) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "insert", entityType: "sourceRows" });
        }
        return await ctx.runMutation(component.snapshotEngine.ingestSourceRows, args);
      },
    }),

    getIndicatorBySlug: queryGeneric({
      args: {
        profileSlug: v.string(),
        slug: v.string(),
      },
      handler: async (ctx, args) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "read", entityType: "indicator" });
        }
        return await ctx.runQuery(component.snapshotEngine.getIndicatorBySlug, args);
      },
    }),

    getIndicatorByExternalId: queryGeneric({
      args: {
        externalId: v.string(),
      },
      handler: async (ctx, args) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "read", entityType: "indicator" });
        }
        return await ctx.runQuery(component.snapshotEngine.getIndicatorByExternalId, args);
      },
    }),

    setIndicatorExternalId: mutationGeneric({
      args: {
        indicatorId: v.string(),
        externalId: v.string(),
      },
      handler: async (ctx, args) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "update", entityType: "indicator" });
        }
        return await ctx.runMutation(component.snapshotEngine.setIndicatorExternalId, args);
      },
    }),

    getValueByExternalId: queryGeneric({
      args: {
        externalId: v.string(),
      },
      handler: async (ctx, args) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "read", entityType: "value" });
        }
        return await ctx.runQuery(component.snapshotEngine.getValueByExternalId, args);
      },
    }),

    listValuesForSync: queryGeneric({
      args: {
        profileSlug: v.optional(v.string()),
        limit: v.optional(v.number()),
      },
      handler: async (ctx, args) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "read", entityType: "value" });
        }
        return await ctx.runQuery(component.snapshotEngine.listValuesForSync, args);
      },
    }),

    setValueExternalId: mutationGeneric({
      args: {
        valueId: v.string(),
        externalId: v.string(),
      },
      handler: async (ctx, args) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "update", entityType: "value" });
        }
        return await ctx.runMutation(component.snapshotEngine.setValueExternalId, args);
      },
    }),

    ensureIndicatorOkrhubLink: mutationGeneric({
      args: {
        profileSlug: v.string(),
        indicatorSlug: v.string(),
        companyExternalId: v.string(),
        symbol: v.string(),
        periodicity: periodicityValidator,
        sourceApp: v.optional(v.string()),
        sourceUrl: v.optional(v.string()),
        isReverse: v.optional(v.boolean()),
        metadata: v.optional(v.any()),
      },
      handler: async (ctx, args) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "sync", entityType: "indicator" });
        }
        const okrhubComponent = requireOkrhubComponent(options);
        const okrhubConfig = getOkrhubConfig(options);
        const sourceApp = args.sourceApp ?? okrhubConfig.sourceApp;
        if (!sourceApp) {
          throw new Error("`sourceApp` è obbligatorio per collegare KPI Snapshot a OKRHub.");
        }

        const indicator = await ctx.runQuery(component.snapshotEngine.getIndicatorBySlug, {
          profileSlug: args.profileSlug,
          slug: args.indicatorSlug,
        });
        if (!indicator) {
          throw new Error("Indicatore non trovato nel componente kpi-snapshot.");
        }
        if (indicator.externalId) {
          return {
            indicatorId: indicator._id,
            externalId: indicator.externalId,
            existing: true,
          };
        }

        const result = await ctx.runMutation(okrhubComponent.okrhub.createIndicator, {
          sourceApp,
          sourceUrl: args.sourceUrl ?? okrhubConfig.sourceUrl,
          companyExternalId: args.companyExternalId,
          description: indicator.description ?? indicator.label,
          symbol: args.symbol,
          periodicity: args.periodicity,
          isReverse: args.isReverse,
          metadata: args.metadata,
        });
        if (!result.success) {
          throw new Error(result.error ?? "createIndicator fallito");
        }

        await ctx.runMutation(component.snapshotEngine.setIndicatorExternalId, {
          indicatorId: indicator._id,
          externalId: result.externalId,
        });

        return {
          indicatorId: indicator._id,
          externalId: result.externalId,
          existing: result.existing ?? false,
        };
      },
    }),

    syncValuesToOkrhub: actionGeneric({
      args: {
        profileSlug: v.optional(v.string()),
        limit: v.optional(v.number()),
        sourceApp: v.optional(v.string()),
        sourceUrl: v.optional(v.string()),
        processSyncQueue: v.optional(v.boolean()),
        processSyncQueueBatchSize: v.optional(v.number()),
      },
      handler: async (ctx, args) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "sync", entityType: "value" });
        }
        const okrhubComponent = requireOkrhubComponent(options);
        const okrhubConfig = getOkrhubConfig(options);
        const sourceApp = args.sourceApp ?? okrhubConfig.sourceApp;
        if (!sourceApp) {
          throw new Error("`sourceApp` è obbligatorio per sincronizzare i valori verso OKRHub.");
        }

        const values = await ctx.runQuery(component.snapshotEngine.listValuesForSync, {
          profileSlug: args.profileSlug,
          limit: args.limit,
        });

        let succeeded = 0;
        let failed = 0;
        let skippedMissingIndicatorExternalId = 0;

        for (const valueRow of values) {
          if (!valueRow.indicatorExternalId) {
            skippedMissingIndicatorExternalId++;
            continue;
          }
          const result = await ctx.runMutation(okrhubComponent.okrhub.createIndicatorValue, {
            sourceApp,
            sourceUrl: args.sourceUrl ?? okrhubConfig.sourceUrl,
            indicatorExternalId: valueRow.indicatorExternalId,
            value: valueRow.value,
            date: valueRow.measuredAt,
          });
          if (!result.success) {
            failed++;
            continue;
          }
          await ctx.runMutation(component.snapshotEngine.setValueExternalId, {
            valueId: valueRow._id,
            externalId: result.externalId,
          });
          succeeded++;
        }

        const shouldProcessQueue =
          args.processSyncQueue ?? okrhubConfig.processSyncQueueByDefault;
        let queueResult:
          | {
              failed: number;
              processed: number;
              succeeded: number;
            }
          | null = null;
        if (shouldProcessQueue && okrhubComponent.sync?.processor?.processSyncQueue) {
          queueResult = await ctx.runAction(
            okrhubComponent.sync.processor.processSyncQueue,
            {
              batchSize:
                args.processSyncQueueBatchSize ?? okrhubConfig.processSyncQueueBatchSize,
            }
          );
        }

        return {
          processed: values.length,
          succeeded,
          failed,
          skippedMissingIndicatorExternalId,
          queueResult,
        };
      },
    }),
  };
}
