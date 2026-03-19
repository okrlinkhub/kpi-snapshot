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
          sourcePayloads: Array<{
            rows: Array<{ occurredAt: number; rowData: any }>;
            sourceKey: string;
          }>;
          triggeredBy?: string;
        },
        {
          errorsCount: number;
          evidencePayloads: Array<{
            csvContent: string;
            fileName: string;
            rowCount: number;
            snapshotRunItemId: string;
            snapshotValueId: string;
          }>;
          processedCount: number;
          snapshotId: string;
          snapshotRunId: string;
          status: "success" | "error";
        },
        Name
      >;
      getIndicatorByExternalId: FunctionReference<
        "query",
        "internal",
        { externalId: string },
        any,
        Name
      >;
      getIndicatorBySlug: FunctionReference<
        "query",
        "internal",
        { profileSlug: string; slug: string },
        any,
        Name
      >;
      getSnapshotExplain: FunctionReference<
        "query",
        "internal",
        { snapshotId: string },
        any,
        Name
      >;
      getSnapshotValueEvidenceDownloadUrl: FunctionReference<
        "query",
        "internal",
        { snapshotValueId: string },
        string | null,
        Name
      >;
      getValueByExternalId: FunctionReference<
        "query",
        "internal",
        { externalId: string },
        any,
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
      listProfileDataSources: FunctionReference<
        "query",
        "internal",
        { profileSlug: string },
        any,
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
        Array<{
          _creationTime: number;
          _id: string;
          createdAt: number;
          description?: string;
          isActive: boolean;
          name: string;
          slug: string;
          updatedAt?: number;
          version: number;
        }>,
        Name
      >;
      listSnapshotRunErrors: FunctionReference<
        "query",
        "internal",
        { snapshotRunId: string },
        any,
        Name
      >;
      listSnapshots: FunctionReference<
        "query",
        "internal",
        { limit?: number; profileSlug?: string },
        any,
        Name
      >;
      listSnapshotValues: FunctionReference<
        "query",
        "internal",
        { snapshotId: string },
        any,
        Name
      >;
      listValuesForSync: FunctionReference<
        "query",
        "internal",
        { limit?: number; profileSlug?: string },
        any,
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
      setIndicatorExternalId: FunctionReference<
        "mutation",
        "internal",
        { externalId: string; indicatorId: string },
        null,
        Name
      >;
      setValueExternalId: FunctionReference<
        "mutation",
        "internal",
        { externalId: string; valueId: string },
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
        any,
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
          enabled?: boolean;
          label: string;
          metadata?: any;
          profileSlug: string;
          sourceKey: string;
          sourceKind:
            | "component_table"
            | "external_reader"
            | "materialized_rows";
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
