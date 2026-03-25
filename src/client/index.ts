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
import { calculationFiltersValidator } from "../component/lib/calculationFilters.js";
import { derivedFormulaValidator } from "../shared/derivedFormula.js";
import {
  createReportWidgetArgsValidator,
  reportWidgetMemberInputValidator,
  transportReportWidgetValidator,
  type AnalyticsReportWidget,
  type ReportWidgetLayout,
} from "../shared/reportWidgets.js";

export type { ComponentApi } from "../component/_generated/component.js";

export type AnalyticsReportSummary = {
  _id: string
  profileId: string
  profileSlug: string
  slug: string
  name: string
  description?: string
  isArchived: boolean
  createdByKey?: string
  updatedByKey?: string
  createdAt: number
  updatedAt: number
}

export type {
  AnalyticsReportWidget,
  ChartReportWidget,
  CreateChartReportWidgetArgs,
  CreateReportWidgetArgs,
  CreateSingleValueReportWidgetArgs,
  ReportWidgetChartKind,
  ReportWidgetIndicatorKind,
  ReportWidgetLayout,
  ReportWidgetMember,
  ReportWidgetMemberInput,
  ReportWidgetTimeRange,
  ReportWidgetType,
  SingleValueReportWidget,
} from "../shared/reportWidgets.js";

export type AnalyticsReportDetail = {
  report: AnalyticsReportSummary
  widgets: AnalyticsReportWidget[]
}

export type IndicatorHistoryPoint = {
  snapshotId: string
  snapshotAt: number | null
  computedAt: number
  value: number | null
  recordedValue: number | null
  isStaleInactive: boolean
  staleReason: 'indicator_disabled' | 'operand_disabled' | null
}

export type IndicatorHistory = {
  profileSlug: string
  indicatorSlug: string
  indicatorKind: 'base' | 'derived'
  indicatorLabel: string
  indicatorUnit: string | null
  points: IndicatorHistoryPoint[]
}

export type SnapshotIndicatorSliceItem = {
  memberKey: string
  sourceProfileSlug: string
  indicatorSlug: string
  indicatorKind: 'base' | 'derived'
  indicatorLabel: string
  indicatorUnit: string | null
  value: number | null
  recordedValue: number | null
  snapshotId: string | null
  snapshotAt: number | null
  computedAt: number | null
  isStaleInactive: boolean
  staleReason: 'indicator_disabled' | 'operand_disabled' | null
}

export type SnapshotIndicatorSlice = {
  snapshotId: string | null
  snapshotAt: number | null
  items: SnapshotIndicatorSliceItem[]
}

export type AnalyticsReportWidgetData =
  | {
      widgetId: string
      widgetType: 'single_value'
      title: string
      description: string | null
      layout: ReportWidgetLayout | null
      member: SnapshotIndicatorSliceItem | null
    }
  | {
      widgetId: string
      widgetType: 'chart'
      chartKind: 'pie'
      title: string
      description: string | null
      layout: ReportWidgetLayout | null
      slice: SnapshotIndicatorSlice
    }
  | {
      widgetId: string
      widgetType: 'chart'
      chartKind: 'line' | 'area' | 'bar'
      title: string
      description: string | null
      layout: ReportWidgetLayout | null
      timeline: {
        series: Array<{
          memberKey: string
          label: string
          unit: string | null
        }>
        points: Array<{
          timestamp: number
          snapshotId: string
          values: Array<{
            memberKey: string
            label: string
            unit: string | null
            value: number | null
            recordedValue: number | null
          }>
        }>
      }
    }

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

    listReports: queryGeneric({
      args: {
        profileSlug: v.optional(v.string()),
        includeArchived: v.optional(v.boolean()),
      },
      handler: async (ctx, args) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "read", entityType: "report" });
        }
        return await ctx.runQuery(component.reportEngine.listReports, args);
      },
    }),

    getReport: queryGeneric({
      args: { reportId: v.string() },
      handler: async (ctx, args) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "read", entityType: "report" });
        }
        return await ctx.runQuery(component.reportEngine.getReport, args);
      },
    }),

    getReportBySlug: queryGeneric({
      args: { slug: v.string() },
      handler: async (ctx, args) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "read", entityType: "report" });
        }
        return await ctx.runQuery(component.reportEngine.getReportBySlug, args);
      },
    }),

    listProfileDataSources: queryGeneric({
      args: { profileSlug: v.string() },
      handler: async (ctx, args) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "read", entityType: "dataSource" });
        }
        return await ctx.runQuery(component.snapshotEngine.listProfileDataSources, args);
      },
    }),

    listDataSources: queryGeneric({
      args: { includeDisabled: v.optional(v.boolean()) },
      handler: async (ctx, args) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "read", entityType: "dataSource" });
        }
        return await ctx.runQuery(component.snapshotEngine.listDataSources, args);
      },
    }),

    getDataSourceFilterOptions: queryGeneric({
      args: { sourceKey: v.string() },
      handler: async (ctx, args) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "read", entityType: "dataSource" });
        }
        return await ctx.runQuery(component.snapshotEngine.getDataSourceFilterOptions, args);
      },
    }),

    simulateSnapshot: queryGeneric({
      args: {
        profileSlug: v.string(),
        snapshotAt: v.optional(v.number()),
        sourcePayloads: v.optional(
          v.array(
            v.object({
              sourceKey: v.string(),
              rows: v.array(
                v.object({
                  occurredAt: v.number(),
                  rowData: v.any(),
                })
              ),
            })
          )
        ),
      },
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

    createSnapshotRun: mutationGeneric({
      args: {
        profileSlug: v.string(),
        snapshotAt: v.optional(v.number()),
        triggeredBy: v.optional(v.string()),
        note: v.optional(v.string()),
        triggerKind: v.optional(v.union(
          v.literal("manual"),
          v.literal("source_materialization"),
          v.literal("scheduled_materialization")
        )),
        triggerSourceKey: v.optional(v.string()),
      },
      handler: async (ctx, args) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "insert", entityType: "snapshot" });
        }
        return await ctx.runMutation(component.snapshotEngine.createSnapshotRun, args);
      },
    }),

    getSnapshotRunStatus: queryGeneric({
      args: { snapshotRunId: v.string() },
      handler: async (ctx, args) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "read", entityType: "snapshotRun" });
        }
        return await ctx.runQuery(component.snapshotEngine.getSnapshotRunStatus, args);
      },
    }),

    listSnapshotValues: queryGeneric({
      args: { snapshotId: v.string() },
      handler: async (ctx, args) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "read", entityType: "snapshotValue" });
        }
        return await ctx.runQuery(component.snapshotEngine.listSnapshotValues, args);
      },
    }),

    getLatestSnapshotValuesForProfile: queryGeneric({
      args: { profileSlug: v.string() },
      handler: async (ctx, args) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "read", entityType: "snapshotValue" });
        }
        return await ctx.runQuery(component.snapshotEngine.getLatestSnapshotValuesForProfile, args);
      },
    }),

    getIndicatorHistory: queryGeneric({
      args: {
        profileSlug: v.string(),
        indicatorSlug: v.string(),
        indicatorKind: v.union(v.literal("base"), v.literal("derived")),
        limit: v.optional(v.number()),
      },
      handler: async (ctx, args) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "read", entityType: "snapshotValue" });
        }
        return await ctx.runQuery(component.snapshotEngine.getIndicatorHistory, args);
      },
    }),

    getSnapshotIndicatorSlice: queryGeneric({
      args: {
        profileSlug: v.optional(v.string()),
        snapshotId: v.optional(v.string()),
        members: v.array(reportWidgetMemberInputValidator),
      },
      handler: async (ctx, args) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "read", entityType: "snapshotValue" });
        }
        return await ctx.runQuery(component.snapshotEngine.getSnapshotIndicatorSlice, args);
      },
    }),

    getReportWidgetData: queryGeneric({
      args: {
        widget: transportReportWidgetValidator,
      },
      handler: async (ctx, args) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "read", entityType: "report" });
        }
        return await ctx.runQuery(component.snapshotEngine.getReportWidgetData, args);
      },
    }),

    getReportWidgetsData: queryGeneric({
      args: {
        widgets: v.array(transportReportWidgetValidator),
      },
      handler: async (ctx, args) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "read", entityType: "report" });
        }
        return await ctx.runQuery(component.snapshotEngine.getReportWidgetsData, args);
      },
    }),

    getSnapshotValueEvidenceDownloadUrl: queryGeneric({
      args: { snapshotValueId: v.string() },
      handler: async (ctx, args) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "read", entityType: "snapshotEvidence" });
        }
        return await ctx.runQuery(
          component.snapshotEngine.getSnapshotValueEvidenceDownloadUrl,
          args
        );
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

    createReport: mutationGeneric({
      args: {
        profileSlug: v.string(),
        name: v.string(),
        description: v.optional(v.string()),
        slug: v.optional(v.string()),
        createdByKey: v.optional(v.string()),
      },
      handler: async (ctx, args) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "insert", entityType: "report" });
        }
        return await ctx.runMutation(component.reportEngine.createReport, args);
      },
    }),

    archiveReport: mutationGeneric({
      args: {
        reportId: v.string(),
        updatedByKey: v.optional(v.string()),
      },
      handler: async (ctx, args) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "update", entityType: "report" });
        }
        return await ctx.runMutation(component.reportEngine.archiveReport, args);
      },
    }),

    updateReportMeta: mutationGeneric({
      args: {
        reportId: v.string(),
        name: v.optional(v.string()),
        description: v.optional(v.string()),
        slug: v.optional(v.string()),
        isArchived: v.optional(v.boolean()),
        updatedByKey: v.optional(v.string()),
      },
      handler: async (ctx, args) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "update", entityType: "report" });
        }
        return await ctx.runMutation(component.reportEngine.updateReportMeta, args);
      },
    }),

    addReportWidget: mutationGeneric({
      args: createReportWidgetArgsValidator,
      handler: async (ctx, args) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "update", entityType: "report" });
        }
        return await ctx.runMutation(component.reportEngine.addReportWidget, args);
      },
    }),

    removeReportWidget: mutationGeneric({
      args: { widgetId: v.string() },
      handler: async (ctx, args) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "update", entityType: "report" });
        }
        return await ctx.runMutation(component.reportEngine.removeReportWidget, args);
      },
    }),

    reorderReportWidgets: mutationGeneric({
      args: {
        reportId: v.string(),
        widgetIds: v.array(v.string()),
      },
      handler: async (ctx, args) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "update", entityType: "report" });
        }
        return await ctx.runMutation(component.reportEngine.reorderReportWidgets, args);
      },
    }),

    upsertDataSource: mutationGeneric({
      args: {
        profileSlug: v.optional(v.string()),
        sourceKey: v.string(),
        label: v.string(),
        adapterKey: v.optional(v.string()),
        sourceKind: v.optional(v.literal("materialized_rows")),
        entityType: v.string(),
        scopeDefinition: v.optional(v.any()),
        selectedFieldKeys: v.optional(v.array(v.string())),
        dateFieldKey: v.optional(v.string()),
        rowKeyStrategy: v.optional(v.string()),
        schedulePreset: v.union(
          v.literal("manual"),
          v.literal("daily"),
          v.literal("weekly_monday"),
          v.literal("monthly_first_day")
        ),
        fieldCatalog: v.optional(
          v.array(
            v.object({
              key: v.string(),
              label: v.string(),
              valueType: v.string(),
              filterable: v.optional(v.boolean()),
              sourcePath: v.optional(v.string()),
              sourceTable: v.optional(v.string()),
              referenceTable: v.optional(v.string()),
              isSystem: v.optional(v.boolean()),
              isNullable: v.optional(v.boolean()),
              isArray: v.optional(v.boolean()),
            })
          )
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

    replaceMaterializedRows: mutationGeneric({
      args: {
        sourceKey: v.string(),
        rows: v.array(
          v.object({
            rowKey: v.string(),
            occurredAt: v.number(),
            rowData: v.any(),
            sourceRecordId: v.optional(v.string()),
            sourceEntityType: v.optional(v.string()),
          })
        ),
      },
      handler: async (ctx, args) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "update", entityType: "dataSource" });
        }
        return await ctx.runMutation(component.snapshotEngine.replaceMaterializedRows, args);
      },
    }),

    upsertIndicator: mutationGeneric({
      args: {
        profileSlug: v.string(),
        slug: v.string(),
        version: v.number(),
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

    rebuildIndicatorReportUsageCounters: mutationGeneric({
      args: {
        profileSlug: v.optional(v.string()),
      },
      handler: async (ctx, args) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "update", entityType: "indicator" });
        }
        return await ctx.runMutation(component.snapshotEngine.rebuildIndicatorReportUsageCounters, args);
      },
    }),

    upsertCalculationDefinition: mutationGeneric({
      args: {
        profileSlug: v.string(),
        indicatorSlug: v.string(),
        indicatorVersion: v.number(),
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
        filters: calculationFiltersValidator,
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

    upsertDerivedIndicator: mutationGeneric({
      args: {
        profileSlug: v.string(),
        slug: v.string(),
        version: v.number(),
        label: v.string(),
        unit: v.optional(v.string()),
        description: v.optional(v.string()),
        formula: derivedFormulaValidator,
        enabled: v.optional(v.boolean()),
      },
      handler: async (ctx, args) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "insert", entityType: "definition" });
        }
        return await ctx.runMutation(component.snapshotEngine.upsertDerivedIndicator, args);
      },
    }),

    listDerivedIndicators: queryGeneric({
      args: { profileSlug: v.string() },
      handler: async (ctx, args) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "read", entityType: "definition" });
        }
        return await ctx.runQuery(component.snapshotEngine.listDerivedIndicators, args);
      },
    }),

    replaceProfileDefinitions: mutationGeneric({
      args: {
        profileSlug: v.string(),
        definitions: v.array(
          v.object({
            indicatorSlug: v.string(),
            indicatorVersion: v.number(),
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
            filters: calculationFiltersValidator,
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

    getIntegrationValueByExternalId: queryGeneric({
      args: {
        externalId: v.string(),
      },
      handler: async (ctx, args) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "read", entityType: "integration_value" });
        }
        return await ctx.runQuery(component.snapshotEngine.getIntegrationValueByExternalId, args);
      },
    }),

    listIntegrationValuesForSync: queryGeneric({
      args: {
        profileSlug: v.optional(v.string()),
        limit: v.optional(v.number()),
      },
      handler: async (ctx, args) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "read", entityType: "integration_value" });
        }
        return await ctx.runQuery(component.snapshotEngine.listIntegrationValuesForSync, args);
      },
    }),

    setIntegrationValueExternalId: mutationGeneric({
      args: {
        integrationValueId: v.string(),
        externalId: v.string(),
      },
      handler: async (ctx, args) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "update", entityType: "integration_value" });
        }
        return await ctx.runMutation(component.snapshotEngine.setIntegrationValueExternalId, args);
      },
    }),

    listExports: queryGeneric({
      args: {
        requestedBy: v.optional(v.string()),
        includePinned: v.optional(v.boolean()),
        limit: v.optional(v.number()),
      },
      handler: async (ctx, args) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "read", entityType: "export" });
        }
        return await ctx.runQuery(component.snapshotEngine.listExports, args);
      },
    }),

    requestExport: actionGeneric({
      args: {
        requestedBy: v.optional(v.string()),
        name: v.optional(v.string()),
        dataSourceKey: v.string(),
        filters: v.object({
          startDate: v.optional(v.number()),
          endDate: v.optional(v.number()),
          dateFieldKey: v.optional(v.string()),
          fieldFilters: v.optional(
            v.array(
              v.object({
                fieldKey: v.string(),
                values: v.array(v.string()),
              })
            )
          ),
        }),
        clonedFromExportId: v.optional(v.string()),
      },
      handler: async (ctx, args) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "insert", entityType: "export" });
        }
        return await ctx.runAction(component.snapshotEngine.requestExport, args);
      },
    }),

    regenerateExport: actionGeneric({
      args: {
        exportId: v.string(),
        requestedBy: v.optional(v.string()),
        name: v.optional(v.string()),
      },
      handler: async (ctx, args) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "insert", entityType: "export" });
        }
        return await ctx.runAction(component.snapshotEngine.regenerateExport, args);
      },
    }),

    getExportDownloadUrl: queryGeneric({
      args: { exportId: v.string() },
      handler: async (ctx, args) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "read", entityType: "export" });
        }
        return await ctx.runQuery(component.snapshotEngine.getExportDownloadUrl, args);
      },
    }),

    deleteExportPermanently: mutationGeneric({
      args: { exportId: v.string() },
      handler: async (ctx, args) => {
        if (options?.auth) {
          await options.auth(ctx, { type: "update", entityType: "export" });
        }
        return await ctx.runMutation(component.snapshotEngine.deleteExportPermanently, args);
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

    syncIntegrationValuesToOkrhub: actionGeneric({
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
          await options.auth(ctx, { type: "sync", entityType: "integration_value" });
        }
        const okrhubComponent = requireOkrhubComponent(options);
        const okrhubConfig = getOkrhubConfig(options);
        const sourceApp = args.sourceApp ?? okrhubConfig.sourceApp;
        if (!sourceApp) {
          throw new Error("`sourceApp` è obbligatorio per sincronizzare i valori verso OKRHub.");
        }

        const integrationValues = await ctx.runQuery(component.snapshotEngine.listIntegrationValuesForSync, {
          profileSlug: args.profileSlug,
          limit: args.limit,
        });

        let succeeded = 0;
        let failed = 0;
        let skippedMissingIndicatorExternalId = 0;

        for (const integrationValue of integrationValues) {
          if (!integrationValue.indicatorExternalId) {
            skippedMissingIndicatorExternalId++;
            continue;
          }
          const result = await ctx.runMutation(okrhubComponent.okrhub.createIndicatorValue, {
            sourceApp,
            sourceUrl: args.sourceUrl ?? okrhubConfig.sourceUrl,
            indicatorExternalId: integrationValue.indicatorExternalId,
            value: integrationValue.value,
            date: integrationValue.measuredAt,
          });
          if (!result.success) {
            failed++;
            continue;
          }
          await ctx.runMutation(component.snapshotEngine.setIntegrationValueExternalId, {
            integrationValueId: integrationValue._id,
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
          processed: integrationValues.length,
          succeeded,
          failed,
          skippedMissingIndicatorExternalId,
          queueResult,
        };
      },
    }),
  };
}
