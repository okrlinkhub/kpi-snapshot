import React, { useState } from "react";
import { useMutation, useQuery, useAction } from "convex/react";
import type { FunctionReference } from "convex/server";
import { api } from "../convex/_generated/api";

/** Type for sendTestBatchToLinkHub (workaround when generated api.d.ts doesn’t list it yet). */
type SendTestBatchToLinkHubRef = FunctionReference<
  "action",
  "public",
  {
    linkHubCompanyId: string;
    batch: Array<{ indicatorSlug: string; value: number; date: number }>;
  },
  { synced: number; errors: string[] }
>;

/** Source indicators (from third-party app / example). In production this list would come from a query. */
const SOURCE_INDICATORS: { slug: string; label: string }[] = [
  { slug: "revenue_q1", label: "Revenue Q1" },
  { slug: "nps", label: "NPS" },
  { slug: "conversion_rate", label: "Conversion rate" },
  { slug: "mau", label: "MAU" },
  { slug: "churn_rate", label: "Churn rate" },
];

/** Destination indicators (from LinkHub, filtered by company). Mock list for UI; in production from LinkHub API. */
const DESTINATION_INDICATORS: { slug: string; label: string }[] = [
  { slug: "revenue_q1", label: "Revenue Q1" },
  { slug: "nps", label: "NPS" },
  { slug: "conversion_rate", label: "Conversion rate" },
  { slug: "mau", label: "MAU" },
  { slug: "churn_rate", label: "Churn rate" },
];

export default function App() {
  const sources = useQuery(api.externalSources.listExternalSources);
  const syncRuns = useQuery(api.externalSources.listSyncRuns, { limit: 10 });
  const receivedValues = useQuery(api.mockWriter.listReceivedValues, { limit: 15 });
  const addSource = useMutation(api.externalSources.addExternalSource);
  const sendTestBatch = useAction(
    (api.sync as { sendTestBatchToLinkHub?: SendTestBatchToLinkHubRef }).sendTestBatchToLinkHub!
  );

  const [companyId, setCompanyId] = useState("");
  const [sourceIndicatorSlug, setSourceIndicatorSlug] = useState(SOURCE_INDICATORS[0]?.slug ?? "");
  const [destinationIndicatorSlug, setDestinationIndicatorSlug] = useState(DESTINATION_INDICATORS[0]?.slug ?? "");
  const [value, setValue] = useState("");
  const [dateStr, setDateStr] = useState(() => {
    const d = new Date();
    return d.toISOString().slice(0, 10);
  });
  const [sendError, setSendError] = useState<string | null>(null);
  const [sendSuccess, setSendSuccess] = useState<string | null>(null);

  const handleSendTest = async () => {
    setSendError(null);
    setSendSuccess(null);
    const num = Number(value);
    const dateMs = dateStr ? new Date(dateStr + "T00:00:00.000Z").getTime() : 0;
    if (Number.isNaN(num)) {
      setSendError("Inserisci un valore numerico.");
      return;
    }
    if (!dateStr || Number.isNaN(dateMs) || dateMs <= 0) {
      setSendError("Seleziona una data valida.");
      return;
    }
    if (!companyId.trim()) {
      setSendError("Inserisci un Company ID (LinkHub).");
      return;
    }
    try {
      const result = await sendTestBatch({
        linkHubCompanyId: companyId.trim(),
        batch: [{ indicatorSlug: destinationIndicatorSlug, value: num, date: dateMs }],
      });
      if (result.errors.length > 0) {
        setSendError(result.errors.join("; "));
      } else {
        setSendSuccess(`Inviato 1 valore (mock writer). Synced: ${result.synced}`);
      }
    } catch (err: unknown) {
      const msg =
        err && typeof err === "object" && "message" in err
          ? (err as { message: string }).message
          : String(err);
      setSendError(msg);
    }
  };

  return (
    <main className="p-6 max-w-2xl mx-auto space-y-8">
      <h1 className="text-2xl font-bold">KPI Snapshot – Example</h1>
      <p className="text-sm text-slate-600 dark:text-slate-400">
        Simula il flusso: <strong>app terza (example)</strong> → kpi-snapshot → <strong>LinkHub</strong>. Scegli company, associa indicatore sorgente a indicatore destinazione, poi invia il valore.
      </p>

      {/* External sources (optional shortcut for company) */}
      <section>
        <h2 className="text-lg font-semibold mb-2">External sources</h2>
        <p className="text-sm text-slate-600 dark:text-slate-400 mb-2">
          Associano un nome alla company LinkHub. Servono per il pull da sistema esterno e per precompilare la company sotto. Opzionale: puoi inserire il company ID a mano nel passo 1.
        </p>
        {sources === undefined ? (
          <p className="text-slate-500">Caricamento…</p>
        ) : sources.length === 0 ? (
          <p className="text-slate-600 dark:text-slate-400">Nessuna source. Aggiungine una oppure inserisci la company nel passo 1.</p>
        ) : (
          <ul className="list-disc pl-5 space-y-1">
            {sources.map((s) => (
              <li key={s._id}>
                {s.name} – <code>{s.linkHubCompanyId}</code>
              </li>
            ))}
          </ul>
        )}
        <button
          type="button"
          className="mt-2 px-3 py-1.5 bg-slate-800 text-white rounded text-sm hover:bg-slate-700"
          onClick={() =>
            addSource({
              name: "Example Source",
              linkHubCompanyId: "example-company",
            })
          }
        >
          Aggiungi source di esempio
        </button>
      </section>

      {/* Step 1: Company */}
      <section className="rounded-lg border border-slate-200 dark:border-slate-700 p-4 bg-slate-50/50 dark:bg-slate-900/30">
        <h2 className="text-lg font-semibold mb-1">
          <span className="inline-flex items-center justify-center w-6 h-6 rounded-full bg-slate-700 text-white text-sm mr-2">1</span>
          Company di destinazione (LinkHub)
        </h2>
        <p className="text-sm text-slate-600 dark:text-slate-400 mb-3">
          La company su LinkHub a cui inviare i valori. La lista indicatori di destinazione (passo 2) in produzione è filtrata per questa company.
        </p>
        <div className="flex gap-2 flex-wrap items-center">
          <input
            type="text"
            className="flex-1 min-w-[10rem] border border-slate-300 dark:border-slate-600 rounded px-2 py-1.5 bg-white dark:bg-slate-800"
            placeholder="es. example-company"
            value={companyId}
            onChange={(e) => setCompanyId(e.target.value)}
          />
          {sources && sources.length > 0 && (
            <select
              className="border border-slate-300 dark:border-slate-600 rounded px-2 py-1.5 bg-white dark:bg-slate-800 text-sm"
              value=""
              onChange={(e) => {
                const v = e.target.value;
                if (v) setCompanyId(v);
              }}
            >
              <option value="">Usa company da source…</option>
              {sources.map((s) => (
                <option key={s._id} value={s.linkHubCompanyId}>
                  {s.name} ({s.linkHubCompanyId})
                </option>
              ))}
            </select>
          )}
        </div>
      </section>

      {/* Step 2: Association */}
      <section className="rounded-lg border border-slate-200 dark:border-slate-700 p-4 bg-slate-50/50 dark:bg-slate-900/30">
        <h2 className="text-lg font-semibold mb-1">
          <span className="inline-flex items-center justify-center w-6 h-6 rounded-full bg-slate-700 text-white text-sm mr-2">2</span>
          Associazione indicatore
        </h2>
        <p className="text-sm text-slate-600 dark:text-slate-400 mb-3">
          Scegli l’indicatore di partenza (dall’app terza) e a quale indicatore su LinkHub deve essere mappato. Il valore verrà inviato con lo slug di destinazione.
        </p>
        <div className="flex flex-wrap items-end gap-3 gap-y-2">
          <div className="flex-1 min-w-[12rem]">
            <label className="block text-sm font-medium mb-1 text-slate-700 dark:text-slate-300">Da (sorgente – app terza)</label>
            <select
              className="w-full border border-slate-300 dark:border-slate-600 rounded px-2 py-1.5 bg-white dark:bg-slate-800"
              value={sourceIndicatorSlug}
              onChange={(e) => setSourceIndicatorSlug(e.target.value)}
            >
              {SOURCE_INDICATORS.map((o) => (
                <option key={o.slug} value={o.slug}>
                  {o.label} ({o.slug})
                </option>
              ))}
            </select>
          </div>
          <span className="text-slate-500 dark:text-slate-400 font-medium pb-1.5" aria-hidden="true">→</span>
          <div className="flex-1 min-w-[12rem]">
            <label className="block text-sm font-medium mb-1 text-slate-700 dark:text-slate-300">A (destinazione – LinkHub)</label>
            <select
              className="w-full border border-slate-300 dark:border-slate-600 rounded px-2 py-1.5 bg-white dark:bg-slate-800"
              value={destinationIndicatorSlug}
              onChange={(e) => setDestinationIndicatorSlug(e.target.value)}
            >
              {DESTINATION_INDICATORS.map((o) => (
                <option key={o.slug} value={o.slug}>
                  {o.label} ({o.slug})
                </option>
              ))}
            </select>
          </div>
        </div>
      </section>

      {/* Step 3: Send value */}
      <section className="rounded-lg border border-slate-200 dark:border-slate-700 p-4 bg-slate-50/50 dark:bg-slate-900/30">
        <h2 className="text-lg font-semibold mb-1">
          <span className="inline-flex items-center justify-center w-6 h-6 rounded-full bg-slate-700 text-white text-sm mr-2">3</span>
          Invia valore
        </h2>
        <p className="text-sm text-slate-600 dark:text-slate-400 mb-3">
          Valore e data da inviare per l’associazione scelta. Verrà usato lo slug di destinazione (<code>{destinationIndicatorSlug}</code>).
        </p>
        <div className="flex flex-wrap items-end gap-4 gap-y-2">
          <div>
            <label className="block text-sm font-medium mb-1">Valore</label>
            <input
              type="number"
              className="w-28 border border-slate-300 dark:border-slate-600 rounded px-2 py-1.5 bg-white dark:bg-slate-800"
              value={value}
              onChange={(e) => setValue(e.target.value)}
            />
          </div>
          <div>
            <label className="block text-sm font-medium mb-1">Data</label>
            <input
              type="date"
              className="border border-slate-300 dark:border-slate-600 rounded px-2 py-1.5 bg-white dark:bg-slate-800"
              value={dateStr}
              onChange={(e) => setDateStr(e.target.value)}
            />
          </div>
          <button
            type="button"
            className="px-3 py-1.5 bg-slate-800 text-white rounded text-sm hover:bg-slate-700"
            onClick={handleSendTest}
          >
            Invia a LinkHub (mock)
          </button>
        </div>
        {sendError && (
          <div className="mt-2 p-2 rounded bg-red-100 dark:bg-red-900/30 text-red-800 dark:text-red-200 text-sm" role="alert">
            {sendError}
          </div>
        )}
        {sendSuccess && (
          <div className="mt-2 p-2 rounded bg-green-100 dark:bg-green-900/30 text-green-800 dark:text-green-200 text-sm" role="status">
            {sendSuccess}
          </div>
        )}
      </section>

      {/* Received values (mock writer log) */}
      <section>
        <h2 className="text-lg font-semibold mb-2">Valori ricevuti (mock)</h2>
        {receivedValues === undefined ? (
          <p className="text-slate-500">Caricamento…</p>
        ) : receivedValues.length === 0 ? (
          <p className="text-slate-600 dark:text-slate-400">Nessun valore ancora. Usa il sync test sopra.</p>
        ) : (
          <ul className="list-disc pl-5 space-y-1 text-sm">
            {receivedValues.map((r) => (
              <li key={r._id}>
                <code>{r.indicatorSlug}</code> = {r.value} @ {new Date(r.date).toISOString()} (company: {r.companyId})
              </li>
            ))}
          </ul>
        )}
      </section>

      {/* Sync runs */}
      <section>
        <h2 className="text-lg font-semibold mb-2">Sync runs (ultimi 10)</h2>
        {syncRuns === undefined ? (
          <p className="text-slate-500">Caricamento…</p>
        ) : syncRuns.length === 0 ? (
          <p className="text-slate-600 dark:text-slate-400">Nessun sync run.</p>
        ) : (
          <ul className="list-disc pl-5 space-y-1 text-sm">
            {syncRuns.map((r) => (
              <li key={r._id}>
                {r.status} – {new Date(r.startedAt).toISOString()}
              </li>
            ))}
          </ul>
        )}
      </section>
    </main>
  );
}
