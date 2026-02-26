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
import type * as mockWriter from "../mockWriter.js";
import type * as myFunctions from "../myFunctions.js";
import type * as sync from "../sync.js";

import type {
  ApiFromModules,
  FilterApi,
  FunctionReference,
} from "convex/server";

declare const fullApi: ApiFromModules<{
  externalSources: typeof externalSources;
  mockWriter: typeof mockWriter;
  myFunctions: typeof myFunctions;
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
          linkHubCompanyId: string;
          name: string;
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
          linkHubCompanyId: string;
          name: string;
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
          linkHubCompanyId?: string;
          name?: string;
        },
        null
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
      syncToLinkHub: FunctionReference<
        "action",
        "internal",
        {
          batch: Array<{ date: number; indicatorSlug: string; value: number }>;
          linkHubCompanyId: string;
          writeHandle: string;
        },
        { errors: Array<string>; synced: number }
      >;
    };
  };
};
