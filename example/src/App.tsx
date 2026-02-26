import React, { useState } from "react";
import { useMutation, useQuery } from "convex/react";
import { api } from "../convex/_generated/api";

const PROFILE_SLUG = "finance_demo";

export default function App() {
  const sources = useQuery(api.externalSources.listExternalSources);
  const syncRuns = useQuery(api.externalSources.listSyncRuns, { limit: 10 });
  const invoicesByCategory = useQuery(api.seed.listInvoicesByCategory, { profileSlug: PROFILE_SLUG });
  const invoices = useQuery(api.seed.listInvoices, { profileSlug: PROFILE_SLUG, limit: 12 });
  const simulated = useQuery(api.seed.simulateSnapshot, { profileSlug: PROFILE_SLUG });
  const snapshots = useQuery(api.seed.listSnapshots, { profileSlug: PROFILE_SLUG, limit: 5 });
  const latestExplain = useQuery(api.seed.getLatestSnapshotExplain, { profileSlug: PROFILE_SLUG });

  const addSource = useMutation(api.externalSources.addExternalSource);
  const setupDefaultSnapshotConfig = useMutation(api.seed.setupDefaultSnapshotConfig);
  const seedInvoices = useMutation(api.seed.seedInvoices);
  const createManualSnapshot = useMutation(api.seed.createManualSnapshot);

  const [opMessage, setOpMessage] = useState<string | null>(null);

  return (
    <main className="p-6 max-w-4xl mx-auto space-y-8">
      <h1 className="text-2xl font-bold">KPI Snapshot – Dynamic Example</h1>
      <p className="text-sm text-slate-600 dark:text-slate-400">
        Demo completa del motore snapshot configurabile: setup profilo, seed invoices multi-categoria, simulazione calcoli, snapshot manuale e debug explain.
      </p>

      <section>
        <h2 className="text-lg font-semibold mb-2">1) Setup profilo e data source</h2>
        <div className="flex flex-wrap gap-2">
          <button
            type="button"
            className="px-3 py-1.5 bg-slate-800 text-white rounded text-sm hover:bg-slate-700"
            onClick={async () => {
              await setupDefaultSnapshotConfig({ profileSlug: PROFILE_SLUG });
              await addSource({
                name: "Invoices Source",
                targetEntityId: "example-target",
              });
              setOpMessage("Setup completato: profilo, regole e source generica creati.");
            }}
          >
            Configura demo
          </button>
          <button
            type="button"
            className="px-3 py-1.5 bg-slate-700 text-white rounded text-sm hover:bg-slate-600"
            onClick={async () => {
              const res = await seedInvoices({ profileSlug: PROFILE_SLUG, count: 40 });
              setOpMessage(`Seed completato: ${res.inserted} invoices create e materializzate.`);
            }}
          >
            Seed invoices
          </button>
          <button
            type="button"
            className="px-3 py-1.5 bg-emerald-700 text-white rounded text-sm hover:bg-emerald-600"
            onClick={async () => {
              const res = await createManualSnapshot({ profileSlug: PROFILE_SLUG, note: "manual snapshot from UI" });
              setOpMessage(`Snapshot creato: ${res.status}, processate ${res.processedCount} regole.`);
            }}
          >
            Crea snapshot manuale
          </button>
        </div>
        {opMessage && <p className="mt-2 text-sm text-emerald-700 dark:text-emerald-300">{opMessage}</p>}
      </section>

      <section className="rounded-lg border border-slate-200 dark:border-slate-700 p-4 bg-slate-50/50 dark:bg-slate-900/30">
        <h2 className="text-lg font-semibold mb-2">2) Stato dati invoices</h2>
        <div className="grid md:grid-cols-2 gap-4">
          <div>
            <h3 className="font-medium mb-2">Per categoria</h3>
            {invoicesByCategory === undefined ? (
              <p className="text-slate-500 text-sm">Caricamento…</p>
            ) : invoicesByCategory.length === 0 ? (
              <p className="text-slate-600 dark:text-slate-400 text-sm">Nessuna invoice.</p>
            ) : (
              <ul className="list-disc pl-5 text-sm space-y-1">
                {invoicesByCategory.map((row) => (
                  <li key={row.category}>
                    <code>{row.category}</code>: count {row.count}, total {row.total}, max {row.max}
                  </li>
                ))}
              </ul>
            )}
          </div>
          <div>
            <h3 className="font-medium mb-2">Ultime invoices</h3>
            {invoices === undefined ? (
              <p className="text-slate-500 text-sm">Caricamento…</p>
            ) : (
              <ul className="list-disc pl-5 text-sm space-y-1">
                {invoices.map((row) => (
                  <li key={row._id}>
                    {row.category} - {row.amount} ({new Date(row.issuedAt).toISOString().slice(0, 10)})
                  </li>
                ))}
              </ul>
            )}
          </div>
        </div>
      </section>

      <section className="rounded-lg border border-slate-200 dark:border-slate-700 p-4 bg-slate-50/50 dark:bg-slate-900/30">
        <h2 className="text-lg font-semibold mb-2">3) Simulazione snapshot (dry-run)</h2>
        {simulated === undefined ? (
          <p className="text-slate-500 text-sm">Caricamento…</p>
        ) : simulated.length === 0 ? (
          <p className="text-slate-600 dark:text-slate-400 text-sm">Nessuna regola attiva.</p>
        ) : (
          <ul className="list-disc pl-5 text-sm space-y-1">
            {simulated.map((item) => (
              <li key={item.definitionId}>
                {item.indicatorSlug} - {item.operation} = {String(item.normalizedResult)} (input: {item.inputCount})
              </li>
            ))}
          </ul>
        )}
      </section>

      <section className="rounded-lg border border-slate-200 dark:border-slate-700 p-4 bg-slate-50/50 dark:bg-slate-900/30">
        <h2 className="text-lg font-semibold mb-2">4) Snapshot creati + explain</h2>
        {snapshots === undefined ? (
          <p className="text-slate-500 text-sm">Caricamento…</p>
        ) : snapshots.length === 0 ? (
          <p className="text-slate-600 dark:text-slate-400 text-sm">Nessuno snapshot ancora.</p>
        ) : (
          <ul className="list-disc pl-5 text-sm space-y-1">
            {snapshots.map((snapshot) => (
              <li key={snapshot._id}>
                {snapshot.status} - {new Date(snapshot.snapshotAt).toISOString()}
                {snapshot.errorMessage ? ` - errore: ${snapshot.errorMessage}` : ""}
              </li>
            ))}
          </ul>
        )}
        {latestExplain && (
          <div className="mt-3 text-sm">
            <p className="font-medium">Explain ultimo snapshot</p>
            <p>
              Run status: <code>{latestExplain.run?.status ?? "n/a"}</code>, values:{" "}
              <code>{latestExplain.values.length}</code>, traces: <code>{latestExplain.traces.length}</code>
            </p>
          </div>
        )}
      </section>

      <section>
        <h2 className="text-lg font-semibold mb-2">External sources + sync runs</h2>
        {sources && sources.length > 0 && (
          <ul className="list-disc pl-5 space-y-1 text-sm mb-3">
            {sources.map((s) => (
              <li key={s._id}>
                {s.name} - <code>{s.targetEntityId}</code>
              </li>
            ))}
          </ul>
        )}
        {syncRuns === undefined ? (
          <p className="text-slate-500">Caricamento…</p>
        ) : syncRuns.length === 0 ? (
          <p className="text-slate-600 dark:text-slate-400">Nessun sync run.</p>
        ) : (
          <ul className="list-disc pl-5 space-y-1 text-sm">
            {syncRuns.map((r) => (
              <li key={r._id}>
                {r.status} - {new Date(r.startedAt).toISOString()}
              </li>
            ))}
          </ul>
        )}
      </section>
    </main>
  );
}
