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
          linkHubCompanyId: string;
          name: string;
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
          linkHubCompanyId: string;
          name: string;
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
          linkHubCompanyId?: string;
          name?: string;
        },
        null,
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
      syncToLinkHub: FunctionReference<
        "action",
        "internal",
        {
          batch: Array<{ date: number; indicatorSlug: string; value: number }>;
          linkHubCompanyId: string;
          writeHandle: string;
        },
        { errors: Array<string>; synced: number },
        Name
      >;
    };
  };
