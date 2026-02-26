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
    snapshotEngine: {
      createSnapshot: FunctionReference<
        "mutation",
        "internal",
        {
          note?: string;
          profileSlug: string;
          snapshotAt?: number;
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
      createSnapshotProfile: FunctionReference<
        "mutation",
        "internal",
        {
          description?: string;
          isActive?: boolean;
          name: string;
          slug: string;
        },
        string
      >;
      getSnapshotExplain: FunctionReference<
        "query",
        "internal",
        { snapshotId: string },
        any
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
      listProfileDefinitions: FunctionReference<
        "query",
        "internal",
        { profileSlug: string },
        any
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
        }>
      >;
      listSnapshotRunErrors: FunctionReference<
        "query",
        "internal",
        { snapshotRunId: string },
        any
      >;
      listSnapshots: FunctionReference<
        "query",
        "internal",
        { limit?: number; profileSlug?: string },
        any
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
        { created: number; deleted: number }
      >;
      simulateSnapshot: FunctionReference<
        "query",
        "internal",
        { profileSlug: string; snapshotAt?: number },
        any
      >;
      toggleCalculation: FunctionReference<
        "mutation",
        "internal",
        { definitionId: string; enabled: boolean },
        null
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
        string
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
        string
      >;
      upsertIndicator: FunctionReference<
        "mutation",
        "internal",
        {
          category?: string;
          description?: string;
          enabled?: boolean;
          label: string;
          profileSlug: string;
          slug: string;
          unit?: string;
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
