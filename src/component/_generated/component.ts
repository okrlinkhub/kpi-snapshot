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
        { profileSlug: string; sourceKey: string },
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
        { jobId: string; profileSlug: string; sourceKey: string },
        Name
      >;
    };
    snapshotEngine: {
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
          slug: string;
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
          status: "success" | "error";
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
      getSnapshotExplain: FunctionReference<
        "query",
        "internal",
        { snapshotId: string },
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
        { includePinned?: boolean; limit?: number; requestedBy?: string },
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
      listSnapshotProfiles: FunctionReference<
        "query",
        "internal",
        {},
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
      regenerateExport: FunctionReference<
        "mutation",
        "internal",
        { exportId: string; name?: string; requestedBy?: string },
        string,
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
            filters?: any;
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
        { created: number; deleted: number },
        Name
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
      upsertCalculationDefinition: FunctionReference<
        "mutation",
        "internal",
        {
          enabled?: boolean;
          fieldPath?: string;
          filters?: any;
          groupBy?: Array<string>;
          indicatorSlug: string;
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
          entityType?: string;
          fieldCatalog?: Array<{
            filterable?: boolean;
            key: string;
            label: string;
            valueType: string;
          }>;
          label: string;
          metadata?: any;
          profileSlug: string;
          rowKeyStrategy?: string;
          schedulePreset?:
            | "manual"
            | "daily"
            | "weekly_monday"
            | "monthly_first_day";
          scopeDefinition?: any;
          selectedFieldKeys?: Array<string>;
          sourceKey: string;
          sourceKind:
            | "component_table"
            | "external_reader"
            | "materialized_rows";
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
