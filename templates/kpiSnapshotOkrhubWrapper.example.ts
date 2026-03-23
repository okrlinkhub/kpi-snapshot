/**
 * Template wrapper integrato per @okrlinkhub/kpi-snapshot + @okrlinkhub/okrhub.
 * Copia questo file in convex/ della tua app (es. come kpiSnapshot.ts), poi:
 * 1. Sostituisci requireAuth(ctx) con il tuo controllo accessi.
 * 2. Verifica che i componenti si chiamino "kpiSnapshot" e "okrhub"
 *    (oppure adatta `components.kpiSnapshot` / `components.okrhub`).
 * 3. Personalizza `sourceApp` e `sourceUrl`.
 *
 * Nota: in questo repo _generated non esiste in templates/, quindi gli import sotto
 * possono dare errore; una volta copiato in convex/ della tua app, esegui
 * "npx convex dev" e gli import si risolvono.
 */
// eslint-disable-next-line @typescript-eslint/ban-ts-comment -- template: _generated esiste in convex/ dell'app
// @ts-nocheck
import { components } from "./_generated/api";
import { exposeApi } from "@okrlinkhub/kpi-snapshot";

async function requireAuth(ctx: { db: any }) {
  // TODO: sostituire con il tuo controllo accessi (es. getAuthenticatedUserId + userSettings.isAdmin)
}

export const {
  listProfiles,
  listProfileDefinitions,
  getIndicatorBySlug,
  listIntegrationValuesForSync,
  ensureIndicatorOkrhubLink,
  syncIntegrationValuesToOkrhub,
} = exposeApi(components.kpiSnapshot, {
  auth: async (ctx) => {
    await requireAuth(ctx as any);
  },
  okrhubComponent: components.okrhub,
  okrhub: {
    sourceApp: "my-app",
    sourceUrl: "",
    processSyncQueueByDefault: false,
    processSyncQueueBatchSize: 10,
  },
});

/**
 * Esempio:
 * 1. Collega un indicatore locale a OKRHub (crea il parent se manca)
 * 2. Crea snapshot
 * 3. Invia i nuovi integration values a OKRHub
 *
 * await ensureIndicatorOkrhubLink({
 *   profileSlug: "finance",
 *   indicatorSlug: "fatturato_mensile",
 *   companyExternalId: "my-app:company:...",
 *   symbol: "EUR",
 *   periodicity: "monthly",
 * });
 *
 * await api.kpiSnapshot.createSnapshot({ profileSlug: "finance", triggeredBy: "manual" });
 *
 * await syncIntegrationValuesToOkrhub({
 *   profileSlug: "finance",
 *   processSyncQueue: true,
 * });
 */
