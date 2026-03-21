/**
 * Template wrapper per @okrlinkhub/kpi-snapshot.
 * Copia questo file in convex/ della tua app (es. come kpiSnapshot.ts), poi:
 * 1. Sostituisci requireAuth(ctx) con il tuo controllo accessi (es. requireAdmin che legge userSettings).
 * 2. Verifica che il nome del componente in convex.config.ts sia "kpiSnapshot" (o adatta components.kpiSnapshot sotto).
 *
 * Il client userà api.kpiSnapshot.* invece di accedere al componente direttamente.
 * Questo esempio usa l'helper `exposeApi(...)` del package per ridurre il boilerplate.
 *
 * Nota: in questo repo _generated non esiste in templates/, quindi gli import sotto possono dare errore;
 * una volta copiato in convex/ della tua app, esegui "npx convex dev" e gli import si risolvono.
 */
// eslint-disable-next-line @typescript-eslint/ban-ts-comment -- template: _generated esiste in convex/ dell'app
// @ts-nocheck
import { components } from "./_generated/api";
import { exposeApi } from "@okrlinkhub/kpi-snapshot";

async function requireAuth(ctx: { db: any }) {
  // TODO: sostituire con il tuo controllo (es. getAuthenticatedUserId + userSettings.isAdmin)
  // await getAuthenticatedUserId(ctx);
  // const settings = await ctx.db.query("userSettings").withIndex("by_user", (q) => q.eq("userId", userId)).first();
  // if (!settings?.isAdmin) throw new Error("Solo gli admin possono gestire i KPI");
}

export const {
  listProfiles,
  listProfileDefinitions,
  listProfileDataSources,
  listDataSources,
  listDerivedIndicators,
  getDataSourceFilterOptions,
  simulateSnapshot,
  listSnapshots,
  listSnapshotValues,
  getSnapshotValueEvidenceDownloadUrl,
  getSnapshotExplain,
  listSnapshotRunErrors,
  createSnapshotProfile,
  upsertDataSource,
  replaceMaterializedRows,
  upsertIndicator,
  upsertCalculationDefinition,
  upsertDerivedIndicator,
  replaceProfileDefinitions,
  toggleCalculation,
  getIndicatorBySlug,
  getIndicatorByExternalId,
  setIndicatorExternalId,
  getIntegrationValueByExternalId,
  listIntegrationValuesForSync,
  setIntegrationValueExternalId,
  listExports,
  requestExport,
  regenerateExport,
  getExportDownloadUrl,
  deleteExportPermanently,
} = exposeApi(components.kpiSnapshot, {
  auth: async (ctx) => {
    await requireAuth(ctx as any);
  },
});
