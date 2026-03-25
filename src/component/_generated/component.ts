/* eslint-disable */
/**
 * Generated `ComponentApi` utility.
 *
 * THIS CODE IS AUTOMATICALLY GENERATED.
 *
 * To regenerate, run `npx convex dev`.
 * @module
 */

import type { FunctionReference } from "convex/server";

/**
 * A utility for referencing a Convex component's exposed API.
 *
 * Useful when expecting a parameter like `components.myComponent`.
 * Usage:
 * ```ts
 * async function myFunction(ctx: QueryCtx, component: ComponentApi) {
 *   return ctx.runQuery(component.someFile.someQuery, { ...args });
 * }
 * ```
 */
export type ComponentApi<Name extends string | undefined = string | undefined> =
  {
    exportWorkflows: {
      regenerateExport: FunctionReference<
        "action",
        "internal",
        { exportId: string; name?: string; requestedBy?: string },
        string,
        Name
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
        string,
        Name
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
        string,
        Name
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
        }>,
        Name
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
        }>,
        Name
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
        null,
        Name
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
        { stagedCount: number },
        Name
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
        { rowCount: number },
        Name
      >;
      deleteDataSource: FunctionReference<
        "mutation",
        "internal",
        { profileSlug?: string; sourceKey: string },
        { deleted: boolean },
        Name
      >;
      failMaterializationJob: FunctionReference<
        "mutation",
        "internal",
        { errorMessage: string; jobId: string },
        null,
        Name
      >;
      getDataSourceByKey: FunctionReference<
        "query",
        "internal",
        { sourceKey: string },
        any | null,
        Name
      >;
      insertMaterializationRowsBatch: FunctionReference<
        "mutation",
        "internal",
        { batchSize?: number; jobId: string },
        { hasMore: boolean; insertedCount: number },
        Name
      >;
      purgeExistingMaterializedRowsBatch: FunctionReference<
        "mutation",
        "internal",
        { batchSize?: number; jobId: string },
        { deletedCount: number; hasMore: boolean },
        Name
      >;
      startMaterializationJob: FunctionReference<
        "mutation",
        "internal",
        {
          requestedBy?: string;
          sourceKey: string;
          triggerKind: "manual" | "schedule" | "upsert" | "snapshot";
        },
        { jobId: string; sourceKey: string },
        Name
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
        { continueCursor: string | null; isDone: boolean; page: Array<any> },
        Name
      >;
    };
    reportEngine: {
      addReportWidget: FunctionReference<
        "mutation",
        "internal",
        {
          indicatorKind: "base" | "derived";
          indicatorLabel: string;
          indicatorSlug: string;
          indicatorUnit?: string;
          reportId: string;
          sourceProfileSlug: string;
        },
        string,
        Name
      >;
      archiveReport: FunctionReference<
        "mutation",
        "internal",
        { reportId: string; updatedByKey?: string },
        null,
        Name
      >;
      createReport: FunctionReference<
        "mutation",
        "internal",
        {
          createdByKey?: string;
          description?: string;
          name: string;
          profileSlug: string;
          slug?: string;
        },
        { reportId: string; slug: string },
        Name
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
            name: string;
            profileId: string;
            profileSlug: string;
            slug: string;
            updatedAt: number;
            updatedByKey?: string;
          };
          widgets: Array<{
            _creationTime: number;
            _id: string;
            createdAt: number;
            indicatorKind: "base" | "derived";
            indicatorLabel: string;
            indicatorSlug: string;
            indicatorUnit?: string;
            order: number;
            reportId: string;
            sourceProfileId: string;
            sourceProfileSlug: string;
            updatedAt?: number;
          }>;
        } | null,
        Name
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
            name: string;
            profileId: string;
            profileSlug: string;
            slug: string;
            updatedAt: number;
            updatedByKey?: string;
          };
          widgets: Array<{
            _creationTime: number;
            _id: string;
            createdAt: number;
            indicatorKind: "base" | "derived";
            indicatorLabel: string;
            indicatorSlug: string;
            indicatorUnit?: string;
            order: number;
            reportId: string;
            sourceProfileId: string;
            sourceProfileSlug: string;
            updatedAt?: number;
          }>;
        } | null,
        Name
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
          name: string;
          profileId: string;
          profileSlug: string;
          slug: string;
          updatedAt: number;
          updatedByKey?: string;
        }>,
        Name
      >;
      removeReportWidget: FunctionReference<
        "mutation",
        "internal",
        { widgetId: string },
        null,
        Name
      >;
      reorderReportWidgets: FunctionReference<
        "mutation",
        "internal",
        { reportId: string; widgetIds: Array<string> },
        null,
        Name
      >;
      updateReportMeta: FunctionReference<
        "mutation",
        "internal",
        {
          description?: string;
          isArchived?: boolean;
          name?: string;
          reportId: string;
          slug?: string;
          updatedByKey?: string;
        },
        { reportId: string; slug: string },
        Name
      >;
    };
    schemaRegistry: {
      deleteSchemaImport: FunctionReference<
        "mutation",
        "internal",
        { schemaImportId: string },
        null,
        Name
      >;
      listCatalogResetJobs: FunctionReference<
        "query",
        "internal",
        {},
        Array<any>,
        Name
      >;
      listSchemaImports: FunctionReference<
        "query",
        "internal",
        {},
        Array<any>,
        Name
      >;
      regenerateCatalogFromSchemas: FunctionReference<
        "mutation",
        "internal",
        {},
        { generatedSettingsCount: number; schemaImportCount: number },
        Name
      >;
      regenerateSchemaCatalog: FunctionReference<
        "mutation",
        "internal",
        {},
        { generatedSettingsCount: number; schemaImportCount: number },
        Name
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
        { schemaImportId: string },
        Name
      >;
      startCatalogReset: FunctionReference<
        "mutation",
        "internal",
        { batchSize?: number; requestedBy?: string },
        { jobId: string },
        Name
      >;
    };
    snapshotEngine: {
      archiveDataSourceSetting: FunctionReference<
        "mutation",
        "internal",
        { entityType: string },
        null,
        Name
      >;
      archiveSnapshotProfile: FunctionReference<
        "mutation",
        "internal",
        { profileSlug: string },
        null,
        Name
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
        null,
        Name
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
        },
        Name
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
        string,
        Name
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
        },
        Name
      >;
      deleteDerivedIndicator: FunctionReference<
        "mutation",
        "internal",
        { profileSlug: string; slug: string },
        { deleted: boolean },
        Name
      >;
      deleteExportPermanently: FunctionReference<
        "mutation",
        "internal",
        { exportId: string },
        null,
        Name
      >;
      deleteIndicator: FunctionReference<
        "mutation",
        "internal",
        { profileSlug: string; slug: string },
        { deleted: boolean; deletedDefinitionCount: number },
        Name
      >;
      getDataSourceFilterOptions: FunctionReference<
        "query",
        "internal",
        { sourceKey: string },
        Array<any>,
        Name
      >;
      getDerivedIndicatorBySlug: FunctionReference<
        "query",
        "internal",
        { profileSlug: string; slug: string },
        any | null,
        Name
      >;
      getExportDownloadUrl: FunctionReference<
        "query",
        "internal",
        { exportId: string },
        string | null,
        Name
      >;
      getIndicatorByExternalId: FunctionReference<
        "query",
        "internal",
        { externalId: string },
        any | null,
        Name
      >;
      getIndicatorBySlug: FunctionReference<
        "query",
        "internal",
        { profileSlug: string; slug: string },
        any | null,
        Name
      >;
      getIntegrationValueByExternalId: FunctionReference<
        "query",
        "internal",
        { externalId: string },
        any | null,
        Name
      >;
      getLatestSnapshotValuesForProfile: FunctionReference<
        "query",
        "internal",
        { profileSlug: string },
        {
          snapshotAt: number | null;
          snapshotId: string | null;
          values: Array<any>;
        },
        Name
      >;
      getSnapshotExplain: FunctionReference<
        "query",
        "internal",
        { snapshotId: string },
        any | null,
        Name
      >;
      getSnapshotRunStatus: FunctionReference<
        "query",
        "internal",
        { snapshotRunId: string },
        any | null,
        Name
      >;
      getSnapshotValueEvidenceDownloadUrl: FunctionReference<
        "query",
        "internal",
        { snapshotValueId: string },
        string | null,
        Name
      >;
      ingestSourceRows: FunctionReference<
        "mutation",
        "internal",
        {
          profileSlug: string;
          rows: Array<{ occurredAt: number; rowData: any }>;
          sourceKey: string;
        },
        { inserted: number },
        Name
      >;
      listDataSources: FunctionReference<
        "query",
        "internal",
        { includeDisabled?: boolean },
        Array<any>,
        Name
      >;
      listDataSourceSettings: FunctionReference<
        "query",
        "internal",
        { includeArchived?: boolean },
        Array<any>,
        Name
      >;
      listDerivedIndicators: FunctionReference<
        "query",
        "internal",
        { profileSlug: string },
        Array<any>,
        Name
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
        Array<any>,
        Name
      >;
      listIntegrationValuesForSync: FunctionReference<
        "query",
        "internal",
        { limit?: number; profileSlug?: string },
        Array<any>,
        Name
      >;
      listProfileDataSources: FunctionReference<
        "query",
        "internal",
        { profileSlug: string },
        Array<any>,
        Name
      >;
      listProfileDefinitions: FunctionReference<
        "query",
        "internal",
        { profileSlug: string },
        any,
        Name
      >;
      listProfileMembers: FunctionReference<
        "query",
        "internal",
        { profileSlug: string },
        Array<any>,
        Name
      >;
      listProfileSlugsBySourceKey: FunctionReference<
        "query",
        "internal",
        { sourceKey: string },
        Array<string>,
        Name
      >;
      listScheduledRefreshTargets: FunctionReference<
        "query",
        "internal",
        { schedulePreset: "daily" | "weekly_monday" | "monthly_first_day" },
        Array<any>,
        Name
      >;
      listSnapshotProfiles: FunctionReference<
        "query",
        "internal",
        { includeArchived?: boolean },
        Array<any>,
        Name
      >;
      listSnapshotRunErrors: FunctionReference<
        "query",
        "internal",
        { snapshotRunId: string },
        Array<any>,
        Name
      >;
      listSnapshots: FunctionReference<
        "query",
        "internal",
        { limit?: number; profileSlug?: string },
        Array<any>,
        Name
      >;
      listSnapshotValues: FunctionReference<
        "query",
        "internal",
        { snapshotId: string },
        Array<any>,
        Name
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
        },
        Name
      >;
      regenerateExport: FunctionReference<
        "action",
        "internal",
        { exportId: string; name?: string; requestedBy?: string },
        string,
        Name
      >;
      removeProfileMember: FunctionReference<
        "mutation",
        "internal",
        { memberKey: string; profileSlug: string },
        null,
        Name
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
        { rowCount: number },
        Name
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
            groupBy?: Array<string>;
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
        { created: number; deleted: number },
        Name
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
        string,
        Name
      >;
      setIndicatorExternalId: FunctionReference<
        "mutation",
        "internal",
        { externalId: string; indicatorId: string },
        null,
        Name
      >;
      setIntegrationValueExternalId: FunctionReference<
        "mutation",
        "internal",
        { externalId: string; integrationValueId: string },
        null,
        Name
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
        Array<any>,
        Name
      >;
      toggleCalculation: FunctionReference<
        "mutation",
        "internal",
        { definitionId: string; enabled: boolean },
        null,
        Name
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
        string,
        Name
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
          groupBy?: Array<string>;
          indicatorSlug: string;
          indicatorVersion: number;
          normalization?: any;
          operation: "sum" | "count" | "avg" | "min" | "max" | "distinct_count";
          priority?: number;
          profileSlug: string;
          ruleVersion?: number;
          sourceKey: string;
        },
        string,
        Name
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
        string,
        Name
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
        string,
        Name
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
          profileSlug: string;
          slug: string;
          unit?: string;
          version: number;
        },
        string,
        Name
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
        string,
        Name
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
        string,
        Name
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
        },
        Name
      >;
    };
  };
