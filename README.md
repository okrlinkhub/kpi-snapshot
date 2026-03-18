# @okrlinkhub/kpi-snapshot

Componente Convex per KPI snapshot dinamici e configurabili: profili, sorgenti dati, regole di calcolo (`sum`, `count`, `avg`, `min`, `max`, `distinct_count`), snapshot manuali o pianificati e tracciamento dei calcoli.

Da questa versione il package può essere usato in due modi:

- come componente standalone, limitandosi a calcolare e storicizzare KPI;
- come componente dell'universo LinkHub, collegando indicatori e valori a `@okrlinkhub/okrhub` tramite `externalId`.

## Requisiti

- Node.js 18+
- Convex compatibile con i componenti
- Una tua app Convex con tabelle sorgente da cui derivare i KPI

## Installazione

Setup standalone:

```bash
npm install @okrlinkhub/kpi-snapshot
```

Setup integrato con OKRHub:

```bash
npm install @okrlinkhub/kpi-snapshot @okrlinkhub/okrhub
```

## Cosa fa il package e cosa resta all'app host

`kpi-snapshot` si occupa di:

- definizione dei profili e delle regole di calcolo;
- ricezione di `sourceRows` materializzate dalla tua app;
- generazione di `snapshot`, `snapshotValues` e storico `values`;
- persistenza opzionale di `externalId` su `indicators` e `values`;
- helper client-side `exposeApi(...)` per ridurre il boilerplate nell'app host.

La tua app host continua a possedere:

- autenticazione e autorizzazione;
- lettura e normalizzazione dei dati di dominio;
- scheduling dei cron;
- metadati di business necessari a OKRHub, ad esempio `companyExternalId`, `symbol` e `periodicity`.

## Setup minimo nell'app host

### 1. Registra il componente

In `convex/convex.config.ts`:

```ts
import { defineApp } from "convex/server";
import kpiSnapshot from "@okrlinkhub/kpi-snapshot/convex.config.js";

const app = defineApp();

app.use(kpiSnapshot, { name: "kpiSnapshot" });

export default app;
```

### 2. Crea un wrapper Convex nella tua app

Il package esporta un helper `exposeApi(...)` che genera query e mutation pass-through già pronte. Tu devi solo applicare il tuo controllo accessi.

Template pronto, incluso nel package:

- [`templates/kpiSnapshotWrapper.example.ts`](templates/kpiSnapshotWrapper.example.ts)

Esempio minimo:

```ts
import { components } from "./_generated/api";
import { exposeApi } from "@okrlinkhub/kpi-snapshot";

async function requireAdmin(ctx: { db: any }) {
  // Implementa qui il tuo controllo accessi.
}

export const {
  listProfiles,
  listProfileDefinitions,
  createSnapshotProfile,
  upsertDataSource,
  upsertIndicator,
  upsertCalculationDefinition,
  createSnapshot,
  ingestSourceRows,
} = exposeApi(components.kpiSnapshot, {
  auth: async (ctx) => {
    await requireAdmin(ctx as any);
  },
});
```

### 3. Invia i tuoi dati al componente

Le righe che mandi al componente devono essere nel formato:

```ts
{
  occurredAt: number;
  rowData: Record<string, unknown>;
}
```

Esempio:

```ts
const invoices = await ctx.db.query("invoices").collect();
const rows = invoices.map((invoice) => ({
  occurredAt: invoice.date,
  rowData: {
    amount: invoice.amount,
    customer: invoice.customer,
    date: invoice.date,
  },
}));

await ctx.runMutation(components.kpiSnapshot.snapshotEngine.ingestSourceRows, {
  profileSlug: "finance",
  sourceKey: "invoices",
  rows,
});
```

### 4. Crea lo snapshot quando vuoi tu

Il componente non ha cron interni. Uno snapshot parte solo quando la tua app chiama `createSnapshot`, tipicamente:

- da una pagina admin;
- da un cron definito in `convex/crons.ts`;
- da una workflow mutation o action della tua app.

## Integrazione opzionale con OKRHub

Se installi anche `@okrlinkhub/okrhub`, lo stesso helper `exposeApi(...)` può orchestrare il wiring tra i due componenti.

Template pronto, incluso nel package:

- [`templates/kpiSnapshotOkrhubWrapper.example.ts`](templates/kpiSnapshotOkrhubWrapper.example.ts)

Esempio:

```ts
import { components } from "./_generated/api";
import { exposeApi } from "@okrlinkhub/kpi-snapshot";

export const {
  createSnapshot,
  ensureIndicatorOkrhubLink,
  syncValuesToOkrhub,
} = exposeApi(components.kpiSnapshot, {
  auth: async (ctx) => {
    // Il tuo controllo accessi
  },
  okrhubComponent: components.okrhub,
  okrhub: {
    sourceApp: "my-app",
    sourceUrl: "",
    processSyncQueueByDefault: false,
  },
});
```

### Flusso consigliato

1. La tua app crea o aggiorna il profilo KPI.
2. La tua app materializza i dati sorgente con `ingestSourceRows`.
3. La tua app collega gli indicatori locali a OKRHub con `ensureIndicatorOkrhubLink(...)`.
4. La tua app crea uno snapshot con `createSnapshot(...)`.
5. La tua app invia i nuovi `values` a OKRHub con `syncValuesToOkrhub(...)`.

## Modifiche minime richieste nell'app host

Se vuoi usare il package nel modo raccomandato, le modifiche minime lato app sono queste:

1. Registrare `kpiSnapshot` in `convex/convex.config.ts`.
2. Creare un file wrapper locale, ad esempio `convex/kpiSnapshot.ts`, che usi `exposeApi(...)`.
3. Implementare il tuo controllo accessi nell'opzione `auth`.
4. Creare almeno una mutation o action che legga le tue tabelle e chiami `ingestSourceRows`.
5. Chiamare `createSnapshot` da UI, cron o automazione.

Se integri anche OKRHub, aggiungi inoltre:

1. Registrazione del componente `okrhub` nella stessa app.
2. Passaggio di `components.okrhub` a `exposeApi(...)`.
3. Configurazione di `sourceApp` e, se serve, `sourceUrl`.
4. Passaggio dei metadati richiesti da OKRHub quando colleghi un indicatore, ad esempio `companyExternalId`, `symbol`, `periodicity`.

## Setup ottimizzato LinkHub universe

La modalità ottimizzata serve quando vuoi che `kpi-snapshot` non sia solo un motore di calcolo, ma anche una sorgente strutturata per OKRHub.

In questo setup:

- `indicators.externalId` salva il collegamento tra l'indicatore locale del componente e l'indicatore OKRHub;
- `values.externalId` salva l'identificativo del valore creato in OKRHub;
- `listValuesForSync` restituisce i valori locali ancora non collegati;
- `setIndicatorExternalId` e `setValueExternalId` permettono di backfillare o correggere manualmente i mapping;
- `syncValuesToOkrhub` esegue la sincronizzazione applicativa minima senza imporre logica di dominio al componente.

Limite importante: `kpi-snapshot` non può inventare da solo metadati come `companyExternalId`, `symbol` o `periodicity`. Questi dati devono continuare ad arrivare dalla tua app host quando chiami `ensureIndicatorOkrhubLink(...)`.

## API del wrapper `exposeApi(...)`

Funzioni principali esposte dal helper:

- lettura: `listProfiles`, `listProfileDefinitions`, `simulateSnapshot`, `listSnapshots`, `getSnapshotExplain`, `listSnapshotRunErrors`
- configurazione: `createSnapshotProfile`, `upsertDataSource`, `upsertIndicator`, `upsertCalculationDefinition`, `replaceProfileDefinitions`, `toggleCalculation`
- esecuzione: `createSnapshot`, `ingestSourceRows`
- mapping esterni: `getIndicatorBySlug`, `getIndicatorByExternalId`, `setIndicatorExternalId`, `getValueByExternalId`, `listValuesForSync`, `setValueExternalId`
- integrazione OKRHub: `ensureIndicatorOkrhubLink`, `syncValuesToOkrhub`

## Modello dati del componente

Tabelle principali:

- `snapshotProfiles`
- `dataSources`
- `indicators`
- `calculationDefinitions`
- `sourceRows`
- `snapshots`
- `snapshotRuns`
- `snapshotRunItems`
- `snapshotValues`
- `calculationTraces`
- `values`

Campi rilevanti per integrazione:

- `indicators.externalId` opzionale
- `values.externalId` opzionale

Questi campi restano opzionali per non rompere installazioni esistenti e sono usati solo quando vuoi collegare il componente a OKRHub o a un altro sistema esterno.

## Migrazione da versioni precedenti

### Nuovi `externalId`

Le installazioni esistenti non richiedono una migrazione obbligatoria: i nuovi campi `externalId` sono opzionali.

Puoi popolarli in modo incrementale:

- in fase di `upsertIndicator`, passando `externalId`;
- con `setIndicatorExternalId`, per collegare indicatori già esistenti;
- con `setValueExternalId`, per backfillare valori già sincronizzati altrove.

### Compatibilità `indicatorLabelSnapshot`

Per compatibilità con installazioni già in uso, il campo `snapshotValues.indicatorLabelSnapshot` è temporaneamente `optional`.

Backfill consigliato:

```ts
await ctx.runMutation(
  components.kpiSnapshot.snapshotEngine.backfillIndicatorLabelSnapshot,
  { dryRun: false }
);
```

Opzioni utili:

- `dryRun: true` per stimare gli aggiornamenti senza scrivere
- `profileSlug` per limitare il backfill a un profilo

## Ricette regole pronte

### Conteggio totale righe

```ts
await upsertCalculationDefinition({
  profileSlug: "finance",
  indicatorSlug: "count_all",
  sourceKey: "invoices",
  operation: "count",
  enabled: true,
});
```

### Somma di un campo numerico

```ts
await upsertCalculationDefinition({
  profileSlug: "finance",
  indicatorSlug: "amount_total",
  sourceKey: "invoices",
  operation: "sum",
  fieldPath: "amount",
  normalization: { round: 2 },
  enabled: true,
});
```

### Conteggio con filtro

```ts
await upsertCalculationDefinition({
  profileSlug: "finance",
  indicatorSlug: "count_selected_categories",
  sourceKey: "invoices",
  operation: "count",
  filters: [{ field: "category", op: "in", value: ["software", "services"] }],
  enabled: true,
});
```

### Media con soglia

```ts
await upsertCalculationDefinition({
  profileSlug: "finance",
  indicatorSlug: "avg_large_invoices",
  sourceKey: "invoices",
  operation: "avg",
  fieldPath: "amount",
  filters: [{ field: "amount", op: "gt", value: 1000 }],
  normalization: { round: 2, coalesce: 0 },
  enabled: true,
});
```

Note rapide:

- `count` può funzionare anche senza `fieldPath`
- `sum`, `avg`, `min`, `max`, `distinct_count` richiedono in genere `fieldPath`
- per debug: usa prima `simulateSnapshot`, poi `createSnapshot`, infine `getSnapshotExplain`

## FAQ

### Il componente sincronizza automaticamente verso OKRHub?

No. Il package fornisce helper per farlo bene, ma il momento in cui eseguire la sync resta una decisione della tua app host.

### Posso usare `kpi-snapshot` senza OKRHub?

Sì. L'integrazione con OKRHub è completamente opzionale.

### Perché servono ancora modifiche nella mia app?

Perché solo la tua app conosce auth, cron, dati di dominio e identificativi business come `companyExternalId`.

## Tipi

```ts
import type { ComponentApi } from "@okrlinkhub/kpi-snapshot";
import { exposeApi } from "@okrlinkhub/kpi-snapshot";
```

## Test con `convex-test`

```ts
import { convexTest } from "convex-test";
import { register } from "@okrlinkhub/kpi-snapshot/test";

const t = convexTest(schema, modules);
register(t, "kpiSnapshot");
```

## Sviluppo locale

Dalla root del repo:

```bash
npm install
npm run build:codegen
npm run typecheck
```

L'example app usa `example/convex` e importa da `@okrlinkhub/kpi-snapshot/convex.config.js`.

## Documentazione Convex

- [Authoring Components](https://docs.convex.dev/components/authoring)
- [Cron Jobs](https://docs.convex.dev/scheduling/cron-jobs)
- [Function Handles](https://docs.convex.dev/functions/function-handles)
