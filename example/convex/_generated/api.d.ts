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
          dateFieldKey?: string;
          indexKey?: string;
          scopeKind: "all" | "last_3_months";
          tableName: string;
        },
        Array<any>
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
          status: "success" | "error";
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
      getIntegrationValueByExternalId: FunctionReference<
        "query",
        "internal",
        { externalId: string },
        any | null
      >;
      getSnapshotExplain: FunctionReference<
        "query",
        "internal",
        { snapshotId: string },
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
      listProfileMembers: FunctionReference<
        "query",
        "internal",
        { profileSlug: string },
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
        { limit?: number; profileSlug?: string },
        Array<any>
      >;
      listSnapshotValues: FunctionReference<
        "query",
        "internal",
        { snapshotId: string },
        Array<any>
      >;
      regenerateExport: FunctionReference<
        "mutation",
        "internal",
        { exportId: string; name?: string; requestedBy?: string },
        string
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
                value: any;
              }>;
              timeRange?: {
                kind:
                  | "last_month"
                  | "last_3_months"
                  | "month_to_date"
                  | "year_to_date";
              };
            };
            groupBy?: Array<string>;
            indicatorSlug: string;
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
        "mutation",
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
              value: any;
            }>;
            timeRange?: {
              kind:
                | "last_month"
                | "last_3_months"
                | "month_to_date"
                | "year_to_date";
            };
          };
          groupBy?: Array<string>;
          indicatorSlug: string;
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
          metadata?: any;
          profileSlug?: string;
          rowKeyStrategy?: string;
          schedulePreset?:
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
          formula: {
            kind: "ratio" | "difference" | "sum";
            operands: Array<{
              indicatorSlug: string;
              role?: "numerator" | "denominator" | "term";
              weight?: number;
            }>;
          };
          label: string;
          profileSlug: string;
          slug: string;
          unit?: string;
        },
        string
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
