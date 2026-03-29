/* eslint-disable */
/**
 * Generated `api` utility.
 *
 * THIS CODE IS AUTOMATICALLY GENERATED.
 *
 * To regenerate, run `npx convex dev`.
 * @module
 */

import type * as externalSources from "../externalSources.js";
import type * as myFunctions from "../myFunctions.js";
import type * as seed from "../seed.js";
import type * as sync from "../sync.js";

import type {
  ApiFromModules,
  FilterApi,
  FunctionReference,
} from "convex/server";

declare const fullApi: ApiFromModules<{
  externalSources: typeof externalSources;
  myFunctions: typeof myFunctions;
  seed: typeof seed;
  sync: typeof sync;
}>;

/**
 * A utility for referencing Convex functions in your app's public API.
 *
 * Usage:
 * ```js
 * const myFunctionReference = api.myModule.myFunction;
 * ```
 */
export declare const api: FilterApi<
  typeof fullApi,
  FunctionReference<any, "public">
>;

/**
 * A utility for referencing Convex functions in your app's internal API.
 *
 * Usage:
 * ```js
 * const myFunctionReference = internal.myModule.myFunction;
 * ```
 */
export declare const internal: FilterApi<
  typeof fullApi,
  FunctionReference<any, "internal">
>;

export declare const components: {
  kpiSnapshot: {
    exportWorkflows: {
      regenerateExport: FunctionReference<
        "action",
        "internal",
        { exportId: string; name?: string; requestedBy?: string },
        string
      >;
      requestExport: FunctionReference<
        "action",
        "internal",
        {
          clonedFromExportId?: string;
          dataSourceKey: string;
          filters: {
            dateFieldKey?: string;
            endDate?: number;
            fieldFilters?: Array<{ fieldKey: string; values: Array<string> }>;
            startDate?: number;
          };
          name?: string;
          profileSlug?: string;
          requestedBy?: string;
        },
        string
      >;
    };
    externalSources: {
      addExternalSource: FunctionReference<
        "mutation",
        "internal",
        {
          authType?: string;
          deploymentUrl?: string;
          name: string;
          targetEntityId: string;
        },
        string
      >;
      listExternalSources: FunctionReference<
        "query",
        "internal",
        {},
        Array<{
          _creationTime: number;
          _id: string;
          authType?: string;
          createdAt: number;
          deploymentUrl?: string;
          name: string;
          targetEntityId: string;
          updatedAt?: number;
        }>
      >;
      listSyncRuns: FunctionReference<
        "query",
        "internal",
        { externalSourceId?: string; limit?: number },
        Array<{
          _creationTime: number;
          _id: string;
          errorMessage?: string;
          externalSourceId: string;
          finishedAt?: number;
          startedAt: number;
          status: string;
          valuesSynced?: number;
        }>
      >;
      updateExternalSource: FunctionReference<
        "mutation",
        "internal",
        {
          authType?: string;
          deploymentUrl?: string;
          id: string;
          name?: string;
          targetEntityId?: string;
        },
        null
      >;
    };
    materialization: {
      appendMaterializationRowsBatch: FunctionReference<
        "mutation",
        "internal",
        {
          jobId: string;
          offset: number;
          rows: Array<{
            occurredAt: number;
            rowData: any;
            rowKey: string;
            sourceEntityType?: string;
            sourceRecordId?: string;
          }>;
        },
        { stagedCount: number }
      >;
      completeMaterializationJob: FunctionReference<
        "mutation",
        "internal",
        {
          frozenExportId?: string;
          jobId: string;
          snapshotId?: string;
          snapshotRunId?: string;
        },
        { rowCount: number }
      >;
      deleteDataSource: FunctionReference<
        "mutation",
        "internal",
        { profileSlug?: string; sourceKey: string },
        { deleted: boolean }
      >;
      failMaterializationJob: FunctionReference<
        "mutation",
        "internal",
        { errorMessage: string; jobId: string },
        null
      >;
      getDataSourceByKey: FunctionReference<
        "query",
        "internal",
        { sourceKey: string },
        any | null
      >;
      insertMaterializationRowsBatch: FunctionReference<
        "mutation",
        "internal",
        { batchSize?: number; jobId: string },
        { hasMore: boolean; insertedCount: number }
      >;
      purgeExistingMaterializedRowsBatch: FunctionReference<
        "mutation",
        "internal",
        { batchSize?: number; jobId: string },
        { deletedCount: number; hasMore: boolean }
      >;
      startMaterializationJob: FunctionReference<
        "mutation",
        "internal",
        {
          requestedBy?: string;
          sourceKey: string;
          triggerKind: "manual" | "schedule" | "upsert" | "snapshot";
        },
        { jobId: string; sourceKey: string }
      >;
    };
    materializationReader: {
      listMaterializableRows: FunctionReference<
        "query",
        "internal",
        {
          batchSize?: number;
          cursor?: string;
          dateFieldKey?: string;
          indexKey?: string;
          scopeKind: "all" | "last_3_months";
          tableName: string;
        },
        { continueCursor: string | null; isDone: boolean; page: Array<any> }
      >;
    };
    reportEngine: {
      addReportWidget: FunctionReference<
        "mutation",
        "internal",
        {
          chartKind?: "line" | "area" | "bar" | "pie";
          description?: string;
          layout?: {
            emphasis: "default" | "accent" | "subtle";
            height: "sm" | "md" | "lg";
            width: "compact" | "wide" | "full";
          };
          member?: {
            indicatorKind: "base" | "derived";
            indicatorLabel: string;
            indicatorSlug: string;
            indicatorUnit?: string;
            sourceProfileSlug: string;
          };
          members?: Array<{
            indicatorKind: "base" | "derived";
            indicatorLabel: string;
            indicatorSlug: string;
            indicatorUnit?: string;
            sourceProfileSlug: string;
          }>;
          reportId: string;
          timeRange?: { limit: number; mode: "latest_n_snapshots" };
          title?: string;
          widgetType: "single_value" | "chart";
        },
        string
      >;
      archiveReport: FunctionReference<
        "mutation",
        "internal",
        { reportId: string; updatedByKey?: string },
        null
      >;
      createReport: FunctionReference<
        "mutation",
        "internal",
        {
          createdByKey?: string;
          description?: string;
          lockedSourceKey: string;
          name: string;
          pinnedSnapshotId?: string;
          profileSlug: string;
          slug?: string;
        },
        { reportId: string; slug: string }
      >;
      getReport: FunctionReference<
        "query",
        "internal",
        { reportId: string },
        {
          report: {
            _creationTime: number;
            _id: string;
            createdAt: number;
            createdByKey?: string;
            description?: string;
            isArchived: boolean;
            lockedDataSourceId: string | null;
            lockedSourceKey?: string;
            lockedSourceLabel: string | null;
            name: string;
            pinnedSnapshotAt: number | null;
            pinnedSnapshotId: string | null;
            profileId: string;
            profileSlug: string;
            slug: string;
            updatedAt: number;
            updatedByKey?: string;
          };
          widgets: Array<
            | {
                _creationTime: number;
                _id: string;
                createdAt: number;
                description?: string;
                layout?: {
                  emphasis: "default" | "accent" | "subtle";
                  height: "sm" | "md" | "lg";
                  width: "compact" | "wide" | "full";
                };
                members: Array<{
                  indicatorKind: "base" | "derived";
                  indicatorLabel: string;
                  indicatorSlug: string;
                  indicatorUnit?: string;
                  sourceProfileId: string;
                  sourceProfileSlug: string;
                }>;
                order: number;
                reportId: string;
                title: string;
                updatedAt?: number;
                widgetType: "single_value";
              }
            | {
                _creationTime: number;
                _id: string;
                chartKind: "line" | "area" | "bar" | "pie";
                createdAt: number;
                description?: string;
                layout?: {
                  emphasis: "default" | "accent" | "subtle";
                  height: "sm" | "md" | "lg";
                  width: "compact" | "wide" | "full";
                };
                members: Array<{
                  indicatorKind: "base" | "derived";
                  indicatorLabel: string;
                  indicatorSlug: string;
                  indicatorUnit?: string;
                  sourceProfileId: string;
                  sourceProfileSlug: string;
                }>;
                order: number;
                reportId: string;
                timeRange?: { limit: number; mode: "latest_n_snapshots" };
                title: string;
                updatedAt?: number;
                widgetType: "chart";
              }
          >;
        } | null
      >;
      getReportBySlug: FunctionReference<
        "query",
        "internal",
        { slug: string },
        {
          report: {
            _creationTime: number;
            _id: string;
            createdAt: number;
            createdByKey?: string;
            description?: string;
            isArchived: boolean;
            lockedDataSourceId: string | null;
            lockedSourceKey?: string;
            lockedSourceLabel: string | null;
            name: string;
            pinnedSnapshotAt: number | null;
            pinnedSnapshotId: string | null;
            profileId: string;
            profileSlug: string;
            slug: string;
            updatedAt: number;
            updatedByKey?: string;
          };
          widgets: Array<
            | {
                _creationTime: number;
                _id: string;
                createdAt: number;
                description?: string;
                layout?: {
                  emphasis: "default" | "accent" | "subtle";
                  height: "sm" | "md" | "lg";
                  width: "compact" | "wide" | "full";
                };
                members: Array<{
                  indicatorKind: "base" | "derived";
                  indicatorLabel: string;
                  indicatorSlug: string;
                  indicatorUnit?: string;
                  sourceProfileId: string;
                  sourceProfileSlug: string;
                }>;
                order: number;
                reportId: string;
                title: string;
                updatedAt?: number;
                widgetType: "single_value";
              }
            | {
                _creationTime: number;
                _id: string;
                chartKind: "line" | "area" | "bar" | "pie";
                createdAt: number;
                description?: string;
                layout?: {
                  emphasis: "default" | "accent" | "subtle";
                  height: "sm" | "md" | "lg";
                  width: "compact" | "wide" | "full";
                };
                members: Array<{
                  indicatorKind: "base" | "derived";
                  indicatorLabel: string;
                  indicatorSlug: string;
                  indicatorUnit?: string;
                  sourceProfileId: string;
                  sourceProfileSlug: string;
                }>;
                order: number;
                reportId: string;
                timeRange?: { limit: number; mode: "latest_n_snapshots" };
                title: string;
                updatedAt?: number;
                widgetType: "chart";
              }
          >;
        } | null
      >;
      listReports: FunctionReference<
        "query",
        "internal",
        { includeArchived?: boolean; profileSlug?: string },
        Array<{
          _creationTime: number;
          _id: string;
          createdAt: number;
          createdByKey?: string;
          description?: string;
          isArchived: boolean;
          lockedDataSourceId: string | null;
          lockedSourceKey?: string;
          lockedSourceLabel: string | null;
          name: string;
          pinnedSnapshotAt: number | null;
          pinnedSnapshotId: string | null;
          profileId: string;
          profileSlug: string;
          slug: string;
          updatedAt: number;
          updatedByKey?: string;
        }>
      >;
      removeReportWidget: FunctionReference<
        "mutation",
        "internal",
        { widgetId: string },
        null
      >;
      reorderReportWidgets: FunctionReference<
        "mutation",
        "internal",
        { reportId: string; widgetIds: Array<string> },
        null
      >;
      updateReportMeta: FunctionReference<
        "mutation",
        "internal",
        {
          description?: string;
          isArchived?: boolean;
          lockedSourceKey?: string;
          name?: string;
          pinnedSnapshotId?: string;
          reportId: string;
          slug?: string;
          updatedByKey?: string;
        },
        { reportId: string; slug: string }
      >;
      updateReportWidget: FunctionReference<
        "mutation",
        "internal",
        { description?: string; title?: string; widgetId: string },
        null
      >;
    };
    schemaRegistry: {
      deleteSchemaImport: FunctionReference<
        "mutation",
        "internal",
        { schemaImportId: string },
        null
      >;
      listCatalogResetJobs: FunctionReference<
        "query",
        "internal",
        {},
        Array<any>
      >;
      listSchemaImports: FunctionReference<"query", "internal", {}, Array<any>>;
      regenerateCatalogFromSchemas: FunctionReference<
        "mutation",
        "internal",
        {},
        { generatedSettingsCount: number; schemaImportCount: number }
      >;
      regenerateSchemaCatalog: FunctionReference<
        "mutation",
        "internal",
        {},
        { generatedSettingsCount: number; schemaImportCount: number }
      >;
      replaceSchemaImport: FunctionReference<
        "mutation",
        "internal",
        {
          checksum: string;
          databaseKey: string;
          fileName: string;
          schemaSource: string;
          tables: Array<{
            defaultDateFieldKey?: string;
            defaultRowKeyFieldKey?: string;
            fields: Array<{
              filterable?: boolean;
              isArray?: boolean;
              isNullable?: boolean;
              isSystem?: boolean;
              key: string;
              label: string;
              referenceTable?: string;
              sourcePath?: string;
              sourceTable?: string;
              valueType: string;
            }>;
            indexes: Array<{
              fields: Array<string>;
              key: string;
              label: string;
            }>;
            label: string;
            tableKey: string;
            tableName: string;
          }>;
        },
        { schemaImportId: string }
      >;
      startCatalogReset: FunctionReference<
        "mutation",
        "internal",
        { batchSize?: number; requestedBy?: string },
        { jobId: string }
      >;
    };
    snapshotEngine: {
      archiveDataSourceSetting: FunctionReference<
        "mutation",
        "internal",
        { entityType: string },
        null
      >;
      archiveSnapshotProfile: FunctionReference<
        "mutation",
        "internal",
        { profileSlug: string },
        null
      >;
      attachSnapshotValueEvidence: FunctionReference<
        "mutation",
        "internal",
        {
          uploads: Array<{
            fileName: string;
            mimeType: string;
            rowCount: number;
            sha256?: string;
            snapshotRunItemId: string;
            snapshotValueId: string;
            storageId: string;
          }>;
        },
        null
      >;
      backfillIndicatorLabelSnapshot: FunctionReference<
        "mutation",
        "internal",
        { dryRun?: boolean; profileSlug?: string },
        {
          scanned: number;
          skippedAlreadySet: number;
          skippedMissingIndicator: number;
          updated: number;
        }
      >;
      createSnapshotProfile: FunctionReference<
        "mutation",
        "internal",
        {
          description?: string;
          isActive?: boolean;
          name: string;
          slug?: string;
        },
        string
      >;
      createSnapshotRun: FunctionReference<
        "mutation",
        "internal",
        {
          note?: string;
          profileSlug: string;
          snapshotAt?: number;
          triggerKind?:
            | "manual"
            | "source_materialization"
            | "scheduled_materialization";
          triggerSourceKey?: string;
          triggeredBy?: string;
        },
        {
          errorsCount: number;
          processedCount: number;
          snapshotId: string;
          snapshotRunId: string;
          status:
            | "queued"
            | "loading"
            | "processing"
            | "deriving"
            | "freezing"
            | "completed"
            | "error";
        }
      >;
      deleteDerivedIndicator: FunctionReference<
        "mutation",
        "internal",
        { profileSlug: string; slug: string },
        { deleted: boolean }
      >;
      deleteExportPermanently: FunctionReference<
        "mutation",
        "internal",
        { exportId: string },
        null
      >;
      deleteIndicator: FunctionReference<
        "mutation",
        "internal",
        { profileSlug: string; slug: string },
        { deleted: boolean; deletedDefinitionCount: number }
      >;
      getDataSourceFilterOptions: FunctionReference<
        "query",
        "internal",
        { sourceKey: string },
        Array<any>
      >;
      getDerivedIndicatorBySlug: FunctionReference<
        "query",
        "internal",
        { profileSlug: string; slug: string },
        any | null
      >;
      getExportDownloadUrl: FunctionReference<
        "query",
        "internal",
        { exportId: string },
        string | null
      >;
      getIndicatorByExternalId: FunctionReference<
        "query",
        "internal",
        { externalId: string },
        any | null
      >;
      getIndicatorBySlug: FunctionReference<
        "query",
        "internal",
        { profileSlug: string; slug: string },
        any | null
      >;
      getIndicatorHistory: FunctionReference<
        "query",
        "internal",
        {
          indicatorKind: "base" | "derived";
          indicatorSlug: string;
          limit?: number;
          profileSlug: string;
        },
        {
          indicatorKind: "base" | "derived";
          indicatorLabel: string;
          indicatorSlug: string;
          indicatorUnit: string | null;
          points: Array<{
            computedAt: number;
            isStaleInactive: boolean;
            recordedValue: number | null;
            snapshotAt: number | null;
            snapshotId: string;
            staleReason: "indicator_disabled" | "operand_disabled" | null;
            value: number | null;
          }>;
          profileSlug: string;
        }
      >;
      getIntegrationValueByExternalId: FunctionReference<
        "query",
        "internal",
        { externalId: string },
        any | null
      >;
      getLatestSnapshotValuesForProfile: FunctionReference<
        "query",
        "internal",
        { profileSlug: string },
        {
          snapshotAt: number | null;
          snapshotId: string | null;
          values: Array<any>;
        }
      >;
      getReportWidgetData: FunctionReference<
        "query",
        "internal",
        {
          pinnedSnapshotId?: string;
          widget:
            | {
                _id: string;
                createdAt: number;
                description?: string;
                layout?: {
                  emphasis: "default" | "accent" | "subtle";
                  height: "sm" | "md" | "lg";
                  width: "compact" | "wide" | "full";
                };
                members: Array<{
                  indicatorKind: "base" | "derived";
                  indicatorLabel: string;
                  indicatorSlug: string;
                  indicatorUnit?: string;
                  sourceProfileId: string;
                  sourceProfileSlug: string;
                }>;
                order: number;
                reportId: string;
                title: string;
                updatedAt?: number;
                widgetType: "single_value";
              }
            | {
                _id: string;
                chartKind: "line" | "area" | "bar" | "pie";
                createdAt: number;
                description?: string;
                layout?: {
                  emphasis: "default" | "accent" | "subtle";
                  height: "sm" | "md" | "lg";
                  width: "compact" | "wide" | "full";
                };
                members: Array<{
                  indicatorKind: "base" | "derived";
                  indicatorLabel: string;
                  indicatorSlug: string;
                  indicatorUnit?: string;
                  sourceProfileId: string;
                  sourceProfileSlug: string;
                }>;
                order: number;
                reportId: string;
                timeRange?: { limit: number; mode: "latest_n_snapshots" };
                title: string;
                updatedAt?: number;
                widgetType: "chart";
              };
        },
        any
      >;
      getReportWidgetsData: FunctionReference<
        "query",
        "internal",
        {
          pinnedSnapshotId?: string;
          widgets: Array<
            | {
                _id: string;
                createdAt: number;
                description?: string;
                layout?: {
                  emphasis: "default" | "accent" | "subtle";
                  height: "sm" | "md" | "lg";
                  width: "compact" | "wide" | "full";
                };
                members: Array<{
                  indicatorKind: "base" | "derived";
                  indicatorLabel: string;
                  indicatorSlug: string;
                  indicatorUnit?: string;
                  sourceProfileId: string;
                  sourceProfileSlug: string;
                }>;
                order: number;
                reportId: string;
                title: string;
                updatedAt?: number;
                widgetType: "single_value";
              }
            | {
                _id: string;
                chartKind: "line" | "area" | "bar" | "pie";
                createdAt: number;
                description?: string;
                layout?: {
                  emphasis: "default" | "accent" | "subtle";
                  height: "sm" | "md" | "lg";
                  width: "compact" | "wide" | "full";
                };
                members: Array<{
                  indicatorKind: "base" | "derived";
                  indicatorLabel: string;
                  indicatorSlug: string;
                  indicatorUnit?: string;
                  sourceProfileId: string;
                  sourceProfileSlug: string;
                }>;
                order: number;
                reportId: string;
                timeRange?: { limit: number; mode: "latest_n_snapshots" };
                title: string;
                updatedAt?: number;
                widgetType: "chart";
              }
          >;
        },
        Array<any>
      >;
      getSnapshotExplain: FunctionReference<
        "query",
        "internal",
        { snapshotId: string },
        any | null
      >;
      getSnapshotIndicatorSlice: FunctionReference<
        "query",
        "internal",
        {
          members: Array<{
            indicatorKind: "base" | "derived";
            indicatorLabel: string;
            indicatorSlug: string;
            indicatorUnit?: string;
            sourceProfileSlug: string;
          }>;
          profileSlug?: string;
          snapshotId?: string;
        },
        {
          items: Array<{
            computedAt: number | null;
            indicatorKind: "base" | "derived";
            indicatorLabel: string;
            indicatorSlug: string;
            indicatorUnit: string | null;
            isStaleInactive: boolean;
            memberKey: string;
            recordedValue: number | null;
            snapshotAt: number | null;
            snapshotId: string | null;
            sourceProfileSlug: string;
            staleReason: "indicator_disabled" | "operand_disabled" | null;
            value: number | null;
          }>;
          snapshotAt: number | null;
          snapshotId: string | null;
        }
      >;
      getSnapshotRunStatus: FunctionReference<
        "query",
        "internal",
        { snapshotRunId: string },
        any | null
      >;
      getSnapshotValueEvidenceDownloadUrl: FunctionReference<
        "query",
        "internal",
        { snapshotValueId: string },
        string | null
      >;
      ingestSourceRows: FunctionReference<
        "mutation",
        "internal",
        {
          profileSlug: string;
          rows: Array<{ occurredAt: number; rowData: any }>;
          sourceKey: string;
        },
        { inserted: number }
      >;
      listDataSources: FunctionReference<
        "query",
        "internal",
        { includeDisabled?: boolean },
        Array<any>
      >;
      listDataSourceSettings: FunctionReference<
        "query",
        "internal",
        { includeArchived?: boolean },
        Array<any>
      >;
      listDerivedIndicators: FunctionReference<
        "query",
        "internal",
        { profileSlug: string },
        Array<any>
      >;
      listExports: FunctionReference<
        "query",
        "internal",
        {
          includeGlobal?: boolean;
          includePinned?: boolean;
          limit?: number;
          profileSlug?: string;
          requestedBy?: string;
        },
        Array<any>
      >;
      listIntegrationValuesForSync: FunctionReference<
        "query",
        "internal",
        { limit?: number; profileSlug?: string },
        Array<any>
      >;
      listProfileDataSources: FunctionReference<
        "query",
        "internal",
        { profileSlug: string },
        Array<any>
      >;
      listProfileDefinitions: FunctionReference<
        "query",
        "internal",
        { profileSlug: string },
        any
      >;
      listProfileIndicatorsBySource: FunctionReference<
        "query",
        "internal",
        { profileSlug: string; sourceKey: string },
        { indicators: Array<any>; profileSlug: string; sourceKey: string }
      >;
      listProfileMembers: FunctionReference<
        "query",
        "internal",
        { profileSlug: string },
        Array<any>
      >;
      listProfileSlugsBySourceKey: FunctionReference<
        "query",
        "internal",
        { sourceKey: string },
        Array<string>
      >;
      listScheduledRefreshTargets: FunctionReference<
        "query",
        "internal",
        { schedulePreset: "daily" | "weekly_monday" | "monthly_first_day" },
        Array<any>
      >;
      listSnapshotProfiles: FunctionReference<
        "query",
        "internal",
        { includeArchived?: boolean },
        Array<any>
      >;
      listSnapshotRunErrors: FunctionReference<
        "query",
        "internal",
        { snapshotRunId: string },
        Array<any>
      >;
      listSnapshots: FunctionReference<
        "query",
        "internal",
        { limit?: number; profileSlug?: string; sourceKey?: string },
        Array<any>
      >;
      listSnapshotValues: FunctionReference<
        "query",
        "internal",
        { snapshotId: string },
        Array<any>
      >;
      rebuildIndicatorReportUsageCounters: FunctionReference<
        "mutation",
        "internal",
        { profileSlug?: string },
        {
          activeReportCount: number;
          activeWidgetCount: number;
          updatedBaseIndicatorCount: number;
          updatedDerivedIndicatorCount: number;
        }
      >;
      regenerateExport: FunctionReference<
        "action",
        "internal",
        { exportId: string; name?: string; requestedBy?: string },
        string
      >;
      removeGroupByFromCalculationDefinitions: FunctionReference<
        "mutation",
        "internal",
        { dryRun?: boolean; profileSlug?: string },
        { scanned: number; skippedWithoutGroupBy: number; updated: number }
      >;
      removeProfileMember: FunctionReference<
        "mutation",
        "internal",
        { memberKey: string; profileSlug: string },
        null
      >;
      replaceMaterializedRows: FunctionReference<
        "mutation",
        "internal",
        {
          rows: Array<{
            occurredAt: number;
            rowData: any;
            rowKey: string;
            sourceEntityType?: string;
            sourceRecordId?: string;
          }>;
          sourceKey: string;
        },
        { rowCount: number }
      >;
      replaceProfileDefinitions: FunctionReference<
        "mutation",
        "internal",
        {
          definitions: Array<{
            enabled?: boolean;
            fieldPath?: string;
            filters: {
              fieldRuleTree?: any;
              fieldRules: Array<{
                field: string;
                op: "eq" | "neq" | "gt" | "gte" | "lt" | "lte" | "in";
                rightOperand:
                  | { kind: "literal"; value: any }
                  | { field: string; kind: "field" };
              }>;
              timeRange?: {
                kind:
                  | "last_month"
                  | "last_3_months"
                  | "month_to_date"
                  | "year_to_date";
              };
            };
            indicatorSlug: string;
            indicatorVersion: number;
            normalization?: any;
            operation:
              | "sum"
              | "count"
              | "avg"
              | "min"
              | "max"
              | "distinct_count";
            priority?: number;
            ruleVersion?: number;
            sourceKey: string;
          }>;
          profileSlug: string;
        },
        { created: number; deleted: number }
      >;
      requestExport: FunctionReference<
        "action",
        "internal",
        {
          clonedFromExportId?: string;
          dataSourceKey: string;
          filters: {
            dateFieldKey?: string;
            endDate?: number;
            fieldFilters?: Array<{ fieldKey: string; values: Array<string> }>;
            startDate?: number;
          };
          name?: string;
          profileSlug?: string;
          requestedBy?: string;
        },
        string
      >;
      setIndicatorExternalId: FunctionReference<
        "mutation",
        "internal",
        { externalId: string; indicatorId: string },
        null
      >;
      setIntegrationValueExternalId: FunctionReference<
        "mutation",
        "internal",
        { externalId: string; integrationValueId: string },
        null
      >;
      simulateSnapshot: FunctionReference<
        "query",
        "internal",
        {
          profileSlug: string;
          snapshotAt?: number;
          sourcePayloads?: Array<{
            rows: Array<{ occurredAt: number; rowData: any }>;
            sourceKey: string;
          }>;
        },
        Array<any>
      >;
      toggleCalculation: FunctionReference<
        "mutation",
        "internal",
        { definitionId: string; enabled: boolean },
        null
      >;
      transferIndicatorAcrossProfiles: FunctionReference<
        "mutation",
        "internal",
        {
          indicatorKind: "base" | "derived";
          mode: "copy" | "move";
          slug: string;
          sourceProfileSlug: string;
          targetCategory?: string;
          targetDefinition?: {
            enabled?: boolean;
            fieldPath?: string;
            filters: {
              fieldRuleTree?: any;
              fieldRules: Array<{
                field: string;
                op: "eq" | "neq" | "gt" | "gte" | "lt" | "lte" | "in";
                rightOperand:
                  | { kind: "literal"; value: any }
                  | { field: string; kind: "field" };
              }>;
              timeRange?: {
                kind:
                  | "last_month"
                  | "last_3_months"
                  | "month_to_date"
                  | "year_to_date";
              };
            };
            normalization?: any;
            operation:
              | "sum"
              | "count"
              | "avg"
              | "min"
              | "max"
              | "distinct_count";
            priority?: number;
            ruleVersion?: number;
            sourceKey: string;
          };
          targetDescription?: string;
          targetEnabled?: boolean;
          targetFormula?:
            | {
                formulaVersion?: 1;
                kind: "ratio" | "difference" | "sum";
                operands: Array<{
                  indicatorSlug: string;
                  role?: "numerator" | "denominator" | "term";
                  weight?: number;
                }>;
              }
            | {
                formulaVersion: 2;
                nodes: Array<
                  | {
                      id: string;
                      indicatorKind: "base" | "derived";
                      indicatorSlug: string;
                      type: "ref";
                    }
                  | { id: string; type: "constant"; value: number }
                  | {
                      id: string;
                      leftNodeId: string;
                      op: "add" | "sub" | "mul" | "div";
                      rightNodeId: string;
                      type: "operation";
                    }
                >;
                rootNodeId: string;
              };
          targetLabel?: string;
          targetLockedSourceKey?: string;
          targetProfileSlug: string;
          targetUnit?: string;
        },
        {
          definitionCount: number;
          indicatorKind: "base" | "derived";
          mode: "copy" | "move";
          sourceProfileSlug: string;
          targetIndicatorId: string;
          targetProfileSlug: string;
        }
      >;
      updateSnapshotProfile: FunctionReference<
        "mutation",
        "internal",
        {
          currentSlug: string;
          description?: string;
          isActive?: boolean;
          name?: string;
          slug?: string;
        },
        string
      >;
      upsertCalculationDefinition: FunctionReference<
        "mutation",
        "internal",
        {
          enabled?: boolean;
          fieldPath?: string;
          filters: {
            fieldRuleTree?: any;
            fieldRules: Array<{
              field: string;
              op: "eq" | "neq" | "gt" | "gte" | "lt" | "lte" | "in";
              rightOperand:
                | { kind: "literal"; value: any }
                | { field: string; kind: "field" };
            }>;
            timeRange?: {
              kind:
                | "last_month"
                | "last_3_months"
                | "month_to_date"
                | "year_to_date";
            };
          };
          indicatorSlug: string;
          indicatorVersion: number;
          normalization?: any;
          operation: "sum" | "count" | "avg" | "min" | "max" | "distinct_count";
          priority?: number;
          profileSlug: string;
          ruleVersion?: number;
          sourceKey: string;
        },
        string
      >;
      upsertDataSource: FunctionReference<
        "mutation",
        "internal",
        {
          adapterKey?: string;
          dateFieldKey?: string;
          enabled?: boolean;
          entityType: string;
          fieldCatalog?: Array<{
            filterable?: boolean;
            isArray?: boolean;
            isNullable?: boolean;
            isSystem?: boolean;
            key: string;
            label: string;
            referenceTable?: string;
            sourcePath?: string;
            sourceTable?: string;
            valueType: string;
          }>;
          label: string;
          materializationIndexKey?: string;
          metadata?: any;
          profileSlug?: string;
          rowKeyStrategy?: string;
          schedulePreset:
            | "manual"
            | "daily"
            | "weekly_monday"
            | "monthly_first_day";
          scopeDefinition?: any;
          selectedFieldKeys?: Array<string>;
          sourceKey: string;
          sourceKind?: "materialized_rows";
        },
        string
      >;
      upsertDataSourceSetting: FunctionReference<
        "mutation",
        "internal",
        {
          adapterKey?: string;
          allowedRowKeyStrategies: Array<{ key: string; label: string }>;
          allowedScopes: Array<{ key: string; label: string }>;
          databaseKey?: string;
          defaultDateFieldKey?: string;
          defaultRowKeyStrategy?: string;
          defaultScopeKey?: string;
          defaultSelectedFieldKeys: Array<string>;
          entityType: string;
          fieldCatalog: Array<{
            filterable?: boolean;
            isArray?: boolean;
            isNullable?: boolean;
            isSystem?: boolean;
            key: string;
            label: string;
            referenceTable?: string;
            sourcePath?: string;
            sourceTable?: string;
            valueType: string;
          }>;
          idFieldSuggestions?: Array<{ key: string; label: string }>;
          indexSuggestions?: Array<{
            fields: Array<string>;
            key: string;
            label: string;
          }>;
          isActive?: boolean;
          label: string;
          metadata?: any;
          schemaImportId?: string;
          sourceKind: "materialized_rows";
          tableKey?: string;
          tableLabel?: string;
          tableName?: string;
        },
        string
      >;
      upsertDerivedIndicator: FunctionReference<
        "mutation",
        "internal",
        {
          description?: string;
          enabled?: boolean;
          formula:
            | {
                formulaVersion?: 1;
                kind: "ratio" | "difference" | "sum";
                operands: Array<{
                  indicatorSlug: string;
                  role?: "numerator" | "denominator" | "term";
                  weight?: number;
                }>;
              }
            | {
                formulaVersion: 2;
                nodes: Array<
                  | {
                      id: string;
                      indicatorKind: "base" | "derived";
                      indicatorSlug: string;
                      type: "ref";
                    }
                  | { id: string; type: "constant"; value: number }
                  | {
                      id: string;
                      leftNodeId: string;
                      op: "add" | "sub" | "mul" | "div";
                      rightNodeId: string;
                      type: "operation";
                    }
                >;
                rootNodeId: string;
              };
          label: string;
          lockedSourceKey: string;
          profileSlug: string;
          slug: string;
          unit?: string;
          version: number;
        },
        {
          derivedIndicatorId: string;
          warning: {
            code: "mixed_base_source_schedule_presets";
            dependencies: Array<{
              indicatorLabel: string;
              indicatorSlug: string;
              sources: Array<{
                schedulePreset:
                  | "manual"
                  | "daily"
                  | "weekly_monday"
                  | "monthly_first_day";
                schedulePresetLabel: string;
                sourceKey: string;
                sourceLabel: string;
              }>;
            }>;
            dependencyCount: number;
            distinctSchedulePresetLabels: Array<string>;
            distinctSchedulePresets: Array<
              "manual" | "daily" | "weekly_monday" | "monthly_first_day"
            >;
            message: string;
            sourceCount: number;
          } | null;
        }
      >;
      upsertIndicator: FunctionReference<
        "mutation",
        "internal",
        {
          category?: string;
          description?: string;
          enabled?: boolean;
          externalId?: string;
          label: string;
          profileSlug: string;
          slug: string;
          unit?: string;
          version: number;
        },
        string
      >;
      upsertProfileMember: FunctionReference<
        "mutation",
        "internal",
        {
          assignedBy?: string;
          isActive?: boolean;
          memberKey: string;
          profileSlug: string;
        },
        string
      >;
      validateDerivedIndicatorSameSnapshotWarning: FunctionReference<
        "query",
        "internal",
        {
          formula:
            | {
                formulaVersion?: 1;
                kind: "ratio" | "difference" | "sum";
                operands: Array<{
                  indicatorSlug: string;
                  role?: "numerator" | "denominator" | "term";
                  weight?: number;
                }>;
              }
            | {
                formulaVersion: 2;
                nodes: Array<
                  | {
                      id: string;
                      indicatorKind: "base" | "derived";
                      indicatorSlug: string;
                      type: "ref";
                    }
                  | { id: string; type: "constant"; value: number }
                  | {
                      id: string;
                      leftNodeId: string;
                      op: "add" | "sub" | "mul" | "div";
                      rightNodeId: string;
                      type: "operation";
                    }
                >;
                rootNodeId: string;
              };
          lockedSourceKey?: string;
          profileSlug: string;
          slug?: string;
        },
        {
          warning: {
            code: "mixed_base_source_schedule_presets";
            dependencies: Array<{
              indicatorLabel: string;
              indicatorSlug: string;
              sources: Array<{
                schedulePreset:
                  | "manual"
                  | "daily"
                  | "weekly_monday"
                  | "monthly_first_day";
                schedulePresetLabel: string;
                sourceKey: string;
                sourceLabel: string;
              }>;
            }>;
            dependencyCount: number;
            distinctSchedulePresetLabels: Array<string>;
            distinctSchedulePresets: Array<
              "manual" | "daily" | "weekly_monday" | "monthly_first_day"
            >;
            message: string;
            sourceCount: number;
          } | null;
        }
      >;
    };
    sync: {
      pullFromExternal: FunctionReference<
        "action",
        "internal",
        {
          authToken?: string;
          deploymentUrl?: string;
          externalSourceId: string;
          since?: number;
        },
        {
          errorMessage?: string;
          status: string;
          syncRunId: string;
          valuesStaged: number;
        }
      >;
    };
  };
};
