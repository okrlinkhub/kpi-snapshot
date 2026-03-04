# @okrlinkhub/kpi-snapshot

Componente Convex per **KPI snapshot dinamici e configurabili**: profili, sorgenti dati, regole di calcolo (`sum`, `count`, `avg`, `min`, `max`, `distinct_count`), snapshot manuale o pianificato e tracciamento dei calcoli.

## Requisiti

- Node.js 18+
- Convex (versione compatibile con i componenti)
- App Convex con le tue tabelle (es. fatture, preavvisi) da cui derivare i KPI

## Installazione

```bash
npm install @okrlinkhub/kpi-snapshot
```

---

## Percorso di configurazione (dall’installazione all’uso)

Segui questi passi nell’ordine: così avrai tutto sotto mano per usare il componente nell’app padre.

### Passo 1 — Aggiungere il componente all’app

In **`convex/convex.config.ts`** della tua app:

```ts
import { defineApp } from "convex/server";
import kpiSnapshot from "@okrlinkhub/kpi-snapshot/convex.config.js";

const app = defineApp();
app.use(kpiSnapshot, { name: "kpiSnapshot" });
export default app;
```

Esegui `npx convex dev`: il componente espone le sue funzioni sotto `components.kpiSnapshot` (es. `components.kpiSnapshot.snapshotEngine.createSnapshot`). Il client della tua app **non** userà questi path direttamente: userà le funzioni che esponi tu (passo 2).

---

### Passo 2 — Funzioni nell’app padre (wrapper + sync)

Devi esporre due tipi di funzioni nella tua app.

**2.1 Wrapper (pass-through con controllo accessi)**  
Un file (es. `convex/kpiSnapshot.ts`) che espone query e mutation verso `components.kpiSnapshot.snapshotEngine.*` e applica il tuo controllo accessi (es. solo admin). L’unico punto da adattare è la funzione di auth (es. `requireAdmin(ctx)`).

- **File di esempio**: [templates/kpiSnapshotWrapper.example.ts](templates/kpiSnapshotWrapper.example.ts)  
  Copialo in `convex/` della tua app (es. come `kpiSnapshot.ts`) e sostituisci solo il controllo accessi in `requireAuth(ctx)`.
- **Funzioni da esporre** (per l’UI e per il cron): `listSnapshotProfiles`, `listProfileDefinitions`, `simulateSnapshot`, `listSnapshots`, `getSnapshotExplain`, `createSnapshotProfile`, `upsertDataSource`, `upsertIndicator`, `upsertCalculationDefinition`, `createSnapshot`, `ingestSourceRows`. Il client userà `api.kpiSnapshot.*` (es. `api.kpiSnapshot.listProfiles`, `api.kpiSnapshot.createSnapshot`).

**2.2 Sync dei dati (mutation che inviano i dati al componente)**  
I dati vivono nelle **tabelle della tua app**. L’app deve leggerli, normalizzarli e inviarli al componente con `ingestSourceRows`. Crea uno o più file (es. `convex/kpiSnapshotSync.ts`) con mutation che:

1. Leggono una tabella (es. `invoices`, `notices`).
2. Mappano ogni riga in `{ occurredAt: number, rowData: object }` (es. `occurredAt` = data fattura, `rowData` = campi usati dalle regole, incluso almeno il campo su cui fai sum/count, es. `value`).
3. Chiamano `ctx.runMutation(components.kpiSnapshot.snapshotEngine.ingestSourceRows, { profileSlug, sourceKey, rows })`.

Esempio minimo per una tabella “fatture”:

```ts
const invoices = await ctx.db.query("invoices").collect();
const rows = invoices.map((inv) => ({
  occurredAt: inv.date,
  rowData: { value: inv.value, date: inv.date, customer: inv.customer /* ... */ },
}));
await ctx.runMutation(components.kpiSnapshot.snapshotEngine.ingestSourceRows, {
  profileSlug: "my_profile",
  sourceKey: "invoices",
  rows,
});
```

Puoi esporre questa logica come mutation pubblica (con auth) per un pulsante “Sincronizza fatture” nell’UI, oppure riusarla dentro un’internal mutation chiamata dal cron (vedi passo 4).

---

### Passo 3 — UI per configurare profili e snapshot

Ti consigliamo di creare una **pagina admin** (es. “Impostazioni KPI” o “Configurazione KPI Snapshot”) che permetta di:

1. **Profili**: elenco profili (`listProfiles`), creazione nuovo profilo (`createSnapshotProfile`), selezione del profilo attivo.
2. **Data source**: per il profilo selezionato, elenco data source (`listProfileDefinitions`), aggiunta data source (`upsertDataSource`) con **source key** (es. `invoices`, `notices`) e tipo (es. `materialized_rows`). Il source key deve coincidere con quello usato nelle tue mutation di sync.
3. **Indicatori**: aggiunta indicatori (`upsertIndicator`) con slug e label (es. “Totale fatture”, “Conteggio preavvisi”).
4. **Regole di calcolo**: aggiunta regole (`upsertCalculationDefinition`) che collegano indicatore, data source, operazione (sum, count, avg, …) e field path (es. `value`).
5. **Sincronizzazione**: pulsanti che chiamano le tue mutation di sync (es. “Sincronizza fatture”, “Sincronizza preavvisi”) per inviare i dati al componente.
6. **Snapshot**: pulsante “Crea snapshot” che chiama `createSnapshot`; lista “Ultimi snapshot” con `listSnapshots` e dettaglio con `getSnapshotExplain`. Opzionale: “Simulazione” con `simulateSnapshot` prima di creare.

Puoi basarti sulle API esposte dal wrapper (`api.kpiSnapshot.*`). Non forniamo un’UI pronta: la struttura sopra è la checklist da implementare nella tua app (React, Vue, ecc.). Un’app di esempio che usa il componente può essere disponibile nel repo (cartella `example/`) come riferimento.

---

### Passo 4 — Cron jobs (snapshot pianificati)

Dopo aver configurato almeno un profilo (passo 3), puoi far eseguire sync + snapshot in automatico con un **cron**.

**Dove si definisce il QUANDO**  
Solo nel file **`convex/crons.ts`** della tua app: lì imposti orario e frequenza (giornaliero, mensile, ecc.) e quale internal mutation chiamare con quali argomenti (es. `profileSlug`). [Doc Convex: Cron Jobs](https://docs.convex.dev/scheduling/cron-jobs).

**Cosa fare quando scatta il cron**  
Un’**internal mutation** (es. in `convex/kpiSnapshotCron.ts`) che: (1) legge le tue tabelle e chiama `ingestSourceRows` per ogni data source del profilo (stessa logica delle mutation di sync del passo 2), (2) chiama `createSnapshot` del componente con `triggeredBy: "cron"`. Nessun controllo utente (il cron non ha sessione).

**Esempio `convex/crons.ts`** (solo scheduling):

```ts
import { cronJobs } from "convex/server";
import { internal } from "./_generated/api";

const crons = cronJobs();
crons.daily(
  "KPI snapshot giornaliero",
  { hourUTC: 9, minuteUTC: 15 }, // es. 10:15 ora italiana (CET)
  internal.kpiSnapshotCron.runScheduledSnapshot,
  { profileSlug: "finance" }
);
export default crons;
```

**Esempio internal mutation** (`convex/kpiSnapshotCron.ts`): definisci `runScheduledSnapshot(profileSlug)` che (1) legge le tabelle dell’app, costruisce le righe nel formato `{ occurredAt, rowData }`, chiama `ingestSourceRows` per ogni `sourceKey` usato dal profilo, (2) chiama `createSnapshot` con `triggeredBy: "cron"`. Restituisci un esito (es. `{ ok: true, noticesCount, snapshotStatus }`) per i log nella dashboard Convex.

Importante: in `crons.ts` non va logica di lettura DB né chiamate al componente; solo scheduling. La logica “cosa fare” sta tutta nell’internal mutation.

---

## Quando viene eseguito uno snapshot

Uno snapshot viene eseguito **solo quando la tua app** chiama la mutation `createSnapshot` del componente. Puoi farlo:

- da un **pulsante** nell’UI (es. nella pagina admin);
- da un **cron job** definito nella tua app (passo 4).

Il componente non ha cron interni: espone solo l’API; è l’app che decide quando chiamarla.

---

## Riepilogo file nell’app padre

| File | Ruolo |
|------|--------|
| `convex/convex.config.ts` | Registra il componente (`app.use(kpiSnapshot, { name: "kpiSnapshot" })`). |
| `convex/kpiSnapshot.ts` | Wrapper: pass-through a `components.kpiSnapshot.snapshotEngine.*` + auth. |
| `convex/kpiSnapshotSync.ts` (o simile) | Mutation di sync: leggono le tue tabelle e chiamano `ingestSourceRows`. |
| `convex/crons.ts` | Definizione del **QUANDO**: schedule + chiamata all’internal mutation. |
| `convex/kpiSnapshotCron.ts` (o simile) | Internal mutation: sync (stessa logica di sync) + `createSnapshot`. |

---

## Template wrapper (dettaglio)

Il file [templates/kpiSnapshotWrapper.example.ts](templates/kpiSnapshotWrapper.example.ts) mostra un pass-through minimo per: `listProfiles`, `listProfileDefinitions`, `simulateSnapshot`, `listSnapshots`, `getSnapshotExplain`, `createSnapshot`, `ingestSourceRows`. Copialo in `convex/` della tua app, rinominalo (es. `kpiSnapshot.ts`) e sostituisci solo `requireAuth(ctx)` con il tuo controllo (es. `requireAdmin`). Aggiungi altri pass-through se ti servono: `createSnapshotProfile`, `upsertDataSource`, `upsertIndicator`, `upsertCalculationDefinition`, `replaceProfileDefinitions`, `toggleCalculation`, `listSnapshotRunErrors`.

---

## Sync dei dati (dettaglio)

Ogni riga inviata al componente ha la forma `{ occurredAt: number, rowData: object }`. `occurredAt` è un timestamp (es. data fattura); `rowData` contiene i campi usati dalle regole di calcolo (es. `value` per una somma). Il `sourceKey` (es. `invoices`, `notices`) deve coincidere con quello configurato nella data source del profilo (passo 3). Puoi usare la stessa logica di mappatura sia nelle mutation pubbliche (pulsanti “Sincronizza”) sia nell’internal mutation del cron.

---

## Modello dati del componente

Tabelle principali: `snapshotProfiles`, `dataSources`, `indicators`, `calculationDefinitions`, `sourceRows` (righe materializzate), `snapshots`, `snapshotRuns`, `snapshotRunItems`, `snapshotValues`, `calculationTraces`, `values` (storico KPI). I valori calcolati restano nelle tabelle del componente; nessun invio verso sistemi esterni.

---

## API principali (riferimento)

Configurazione: `createSnapshotProfile`, `upsertDataSource`, `upsertIndicator`, `upsertCalculationDefinition`, `replaceProfileDefinitions`, `listProfileDefinitions`, `listSnapshotProfiles`.  
Esecuzione: `ingestSourceRows`, `simulateSnapshot`, `createSnapshot`, `listSnapshots`, `getSnapshotExplain`, `listSnapshotRunErrors`.

---

## Tipi

```ts
import type { ComponentApi } from "@okrlinkhub/kpi-snapshot";
```

---

## Test con convex-test

```ts
import { convexTest } from "convex-test";
import { register } from "@okrlinkhub/kpi-snapshot/test";

const t = convexTest(schema, modules);
register(t, "kpiSnapshot");
```

---

## Sviluppo locale (dal repo del componente)

Dalla root del repo: `npm install` → `npm run build:codegen` → (opzionale) `npm link` e `npm link @okrlinkhub/kpi-snapshot` nell’app di test → `npm run dev`. L’example app usa `example/convex` e importa da `@okrlinkhub/kpi-snapshot/convex.config.js`.

---

## Documentazione Convex

- [Authoring Components](https://docs.convex.dev/components/authoring)
- [Cron Jobs](https://docs.convex.dev/scheduling/cron-jobs)
- [Function Handles](https://docs.convex.dev/functions/function-handles)
