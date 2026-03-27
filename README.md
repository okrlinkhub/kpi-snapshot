# @okrlinkhub/kpi-snapshot

Componente Convex per KPI snapshot dinamici e configurabili: profili, `dataSources` globali dell'app, membership utenti-profili, regole di calcolo (`sum`, `count`, `avg`, `min`, `max`, `distinct_count`), snapshot manuali o pianificati e tracciamento dei calcoli.

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

`kpi-snapshot` 1.x si occupa di:

- definizione dei profili e delle regole di calcolo;
- possesso di una sola tabella `dataSources`, globale e riusabile da tutti i profili;
- periodicita` obbligatoria della `dataSource` tramite `schedulePreset`;
- membership utenti-profili tramite `profileMembers`;
- materializzazione globale per `dataSourceId`, senza binding persistiti source-profilo;
- generazione di export CSV globali automatici per audit/materializzazione e di export custom opzionalmente `profile-scoped`, con persistenza su storage Convex;
- generazione di `snapshot`, `snapshotValues` e storico `values`;
- audit interno `snapshotValue -> sourceExportIds`;
- versioning obbligatorio di indicatori base e derivati tramite coppia logica `slug` + `version`;
- calcolo di indicatori derivati direttamente nel componente;
- persistenza di report profile-scoped e widget ordinabili, con `slug` globale univoco per deep-link stabili;
- widget report di tipo `single_value` e `chart`, con supporto nativo a trend storici e confronti multi-indicatore;
- persistenza opzionale di `externalId` su `indicators` e `values`;
- helper client-side `exposeApi(...)` per ridurre il boilerplate nell'app host.

La tua app host continua a possedere:

- autenticazione e autorizzazione;
- adapter di lettura e normalizzazione dei dati di dominio;
- scheduling dei cron;
- eventuale orchestrazione UI/admin specifica del prodotto;
- metadati di business necessari a OKRHub, ad esempio `companyExternalId`, `symbol` e `periodicity`.

## Contratto di periodicita`

Da questa revisione il modello distingue in modo esplicito tre concetti:

- `dataSources.schedulePreset`: cadence operativa della source (`manual`, `daily`, `weekly_monday`, `monthly_first_day`);
- `snapshotAt`: istante a cui viene calcolato uno snapshot;
- `calculationDefinitions.filters.timeRange`: finestra analitica opzionale applicata ai record della source.

Regole operative:

- la periodicita` della `dataSource` e` obbligatoria e visibile anche quando si collega un indicatore;
- le source schedulate (`daily`, `weekly_monday`, `monthly_first_day`) vengono trattate come dataset operativi limitati all'ultimo anno like-for-like;
- `manual` resta l'unica modalita` senza finestra temporale automatica;
- un refresh/materializzazione di una source puo` generare snapshot solo per gli indicatori che dipendono da quella source, invece di ricalcolare forzatamente tutto il profilo;
- quando `snapshotAt` e` noto, il componente legge `analyticsMaterializedRows` tramite l'indice composto `by_data_source_and_occurred_at` invece di filtrare tutto in memoria;
- per leggere KPI con cadence diverse, l'host dovrebbe usare l'ultimo valore disponibile per indicatore invece di assumere un unico snapshot globale.

Nota importante sui KPI derivati:

- il motore di calcolo dei derivati resta volutamente `same snapshot only`: un derivato viene calcolato solo se tutte le sue dipendenze necessarie sono presenti nello stesso snapshot;
- se un KPI derivato combina KPI base alimentati da `dataSources` con `schedulePreset` differenti, il componente non blocca il salvataggio ma espone un warning esplicito in fase di authoring e `upsertDerivedIndicator(...)`;
- il warning ricorda che negli snapshot parziali avviati da una singola source (`triggerSourceKey`) il derivato puo` restare non calcolato finche' una delle parti manca nello stesso snapshot.

## Modello dati risultante

- `dataSources` e` globale nell'app: ogni profilo sceglie quali source usare quando definisce KPI o export custom.
- `materializationJobs` e `analyticsMaterializedRows` dipendono solo dalla source, non dal profilo.
- `calculationDefinitions`, `indicators`, `derivedIndicators`, `snapshots` e `reports` restano profile-scoped.
- `indicators` e `derivedIndicators` sono versionati: lo stesso `slug` puo` avere piu` versioni nello stesso profilo.
- le query runtime e i report risolvono sempre l'ultima `version` disponibile per ogni `slug`.
- ogni report ha uno `slug` globale univoco, così l'host puo` costruire route semplici come `/analytics/report/[slug]` senza includere il profilo nell'URL.
- `reportWidgets` non e` piu` limitata al vecchio modello `1 widget = 1 indicatore`: un widget puo` contenere `members[]` e dichiarare `widgetType` / `chartKind`.
- `analyticsExports` distingue:
  - export automatici globali, prodotti da materializzazione o freeze audit;
  - export custom legati opzionalmente a un profilo.
- L'host non deve mantenere un catalogo runtime parallelo delle source: entita`, scope, row key e campo data vivono nella tabella componente `dataSourceSettings`, mentre `dataSources` salva solo l'istanza concreta scelta dall'admin.

## Widget report chart-first

Il modello report del componente ora distingue due primitive:

- `single_value`: card numerica per un solo indicatore
- `chart`: widget grafico con `chartKind = line | area | bar | pie`

Campi principali di `reportWidgets`:

- `widgetType`
- `title`, `description`
- `layout`
- `members[]` con riferimenti indicatori profile-scoped
- `chartKind` e `timeRange` per i widget grafici

Query runtime dedicate:

- `getIndicatorHistory`: storico di un indicatore attraverso gli snapshot
- `getSnapshotIndicatorSlice`: confronto coerente di piu` indicatori nello stesso snapshot
- `getReportWidgetData` e `getReportWidgetsData`: adapter pronti per UI/report builder

Normalizzazione valori:

- se `indicatorUnit` e` `%`, le query runtime orientate alla UI (`getIndicatorHistory`, `getSnapshotIndicatorSlice`, `getReportWidgetData`, `getReportWidgetsData`) moltiplicano automaticamente il valore per `100` nel payload di trasporto;
- i valori persistiti nelle tabelle del componente restano invariati e continuano a rappresentare la frazione grezza, ad esempio `0.3551`;
- l'host puo` quindi mostrare direttamente `35.51 %` senza dover aggiungere conversioni custom in ogni consumer.

Regole operative:

- i trend leggono gli ultimi `N` snapshot del singolo indicatore, senza assumere un unico snapshot globale del profilo;
- i pie chart richiedono indicatori dello stesso profilo e li confrontano sullo stesso snapshot coerente;
- i contatori `reportUsageCount` vengono aggiornati per tutti i `members` del widget, non solo per il caso single KPI.

## Catalogo persistito schema-driven

Da questa revisione il flusso corretto e`:

- l'host carica uno o piu` `schema.ts` Convex tramite action Node lato app;
- il componente memorizza gli schema importati nel registry `schemaImports`;
- il componente rigenera automaticamente `dataSourceSettings` a partire da `databaseKey + tableName`;
- l'import schema non resetta piu` in-line dati materializzati o job; quel wipe vive in un reset job separato e batch-safe;
- la UI di creazione di una `dataSource` legge solo definizioni generate dal registry;
- `databaseKey` diversi da `app` sono materializzabili solo se l'host registra un reader esplicito per quel namespace;
- i reader interni del componente espongono paginazione a cursore, così l'host può leggere namespace esterni senza `collect()` monolitici;
- il solo `sourceKind` supportato e` `materialized_rows`.

Campi principali di `schemaImports`:

- `databaseKey`, `fileName`, `checksum`, `schemaSource`
- `tables[]` con `tableName`, `tableKey`, `fields[]`, `indexes[]`

Campi principali di `dataSourceSettings`:

- `entityType` derivato (`databaseKey__tableName`)
- `databaseKey`, `tableName`, `tableKey`, `tableLabel`
- `allowedScopes`
- `allowedRowKeyStrategies`
- `idFieldSuggestions`, `indexSuggestions`
- `defaultScopeKey`, `defaultRowKeyStrategy`, `defaultDateFieldKey`
- `defaultSelectedFieldKeys`
- `fieldCatalog`

Campi principali di `dataSources` lato runtime:

- `schedulePreset` come cadence operativa obbligatoria
- `dateFieldKey` come campo temporale usato da filtri/materializzazione
- `materializationIndexKey` risolto automaticamente dal componente scegliendo il primo indice host-side compatibile con `dateFieldKey`

Compatibilita` degli indici temporali:

- un indice e` compatibile se il suo primo campo coincide con `dateFieldKey`
- indici composti come `['createdAt', 'clinicId']` restano validi per il pruning temporale
- indici dove `dateFieldKey` non e` il primo campo non vengono usati dal reader

API componente rilevanti:

- `listSchemaImports`
- `replaceSchemaImport`
- `regenerateCatalogFromSchemas`
- `deleteSchemaImport`
- `listCatalogResetJobs`
- `startCatalogReset`
- `listDataSourceSettings`

## Linea guida cron per tutti i consumer

Per mantenere il componente semplice da distribuire e replicare su consumer diversi, `kpi-snapshot` non registra cron runtime e non richiede componenti cron dedicati.

Ogni consumer deve definire **3 cron statici** nel proprio `convex/crons.ts`:

- uno per le source con preset `daily`
- uno per le source con preset `weekly_monday`
- uno per le source con preset `monthly_first_day`

Il package mantiene:

- i preset di schedule nella tabella `dataSources`
- le query di supporto per target schedulati e profili impattati (`listScheduledRefreshTargets`, `listProfileSlugsBySourceKey`)
- il contratto dei metadati necessari per materializzare e mostrare la cadence della source

L'host continua a possedere:

- la pipeline batch-safe di materializzazione
- i reader sui database reali
- l'orchestrazione finale che, dopo la materializzazione, invoca `createSnapshotRun`

Esempio consigliato:

```ts
import { cronJobs } from "convex/server";
import { internal } from "./_generated/api";

const crons = cronJobs();

crons.cron(
  "kpi-snapshot-refresh-daily",
  "0 0 * * *",
  internal.analyticsExports.runScheduledRefreshes,
  { schedulePreset: "daily" }
);

crons.cron(
  "kpi-snapshot-refresh-weekly-monday",
  "0 0 * * 1",
  internal.analyticsExports.runScheduledRefreshes,
  { schedulePreset: "weekly_monday" }
);

crons.cron(
  "kpi-snapshot-refresh-monthly-first-day",
  "0 0 1 * *",
  internal.analyticsExports.runScheduledRefreshes,
  { schedulePreset: "monthly_first_day" }
);

export default crons;
```

Suggerimento implementativo host-side:

1. chiedi al componente i target con `listScheduledRefreshTargets({ schedulePreset })`
2. materializza ogni source con il reader host-side
3. dopo la materializzazione, invoca `createSnapshotRun` per ogni profilo impattato passando `triggerSourceKey`
4. usa `getSnapshotRunStatus({ snapshotRunId })` per polling finché il run non arriva a `completed` o `error`

In questo modo il refresh schedulato di una source aggiorna solo i KPI coerenti con quella cadence.

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
  listProfileDataSources,
  createSnapshotProfile,
  upsertDataSource,
  upsertIndicator,
  upsertCalculationDefinition,
  transferIndicatorAcrossProfiles,
  listSnapshotValues,
  getSnapshotValueEvidenceDownloadUrl,
} = exposeApi(components.kpiSnapshot, {
  auth: async (ctx) => {
    await requireAdmin(ctx as any);
  },
});
```

### 3. Parsing schema e materializzazione

L'app host:

- parse gli `schema.ts` con una action Node separata dal componente;
- salva il risultato nel componente e rigenera il catalogo persistito;
- materializza tramite un reader registry host-side leggendo `databaseKey`, `tableName`, `fieldCatalog`, `rowKeyStrategy` e l'eventuale `materializationIndexKey` risolto automaticamente sulla data source;
- quando legge tabelle del namespace `kpiSnapshot`, usa `materializationReader.listMaterializableRows` in modalità paginata (`cursor`, `batchSize`, `continueCursor`);
- puo` supportare namespace aggiuntivi (es. `kpiSnapshot`) solo registrando reader dedicati.

Il formato finale delle righe materializzate resta:

```ts
{
  rowKey: string;
  occurredAt: number;
  rowData: Record<string, unknown>;
  sourceRecordId?: string;
  sourceEntityType?: string;
}
```

Esempio adapter:

```ts
const invoices = await ctx.db.query("invoices").collect();
const rows = invoices.map((invoice) => ({
  rowKey: invoice.number,
  occurredAt: invoice.date,
  rowData: {
    amount: invoice.amount,
    customer: invoice.customer,
    date: invoice.date,
  },
}));
```

L'host aggiorna la data source del componente chiamando il proprio workflow di materializzazione batch-safe.
La source resta globale; il `profileSlug` serve solo se vuoi anche concatenare uno snapshot profilo-specifico subito dopo il refresh.

### 4. Crea uno snapshot orchestrato dalla tua app host

Il componente non ha cron interni. La tua app host puo':

- materializzare da UI
- materializzare dai 3 cron statici del consumer
- chiamare `createSnapshot` in modo esplicito quando vuole un run storico dedicato

Nel flusso consigliato, la materializzazione host aggiorna la source e il componente fa partire automaticamente `createSnapshotRun`:

```ts
await ctx.runMutation(api.kpiSnapshot.replaceMaterializedRows, {
  sourceKey: "invoices",
  rows,
})

await ctx.runAction(api.kpiSnapshot.createSnapshot, {
  profileSlug: "finance",
})
```

Durante `createSnapshot` orchestrato dalla tua app:

- legge le righe materializzate correnti del profilo;
- crea un `snapshotRunItem` per ogni definizione attiva;
- congela una volta sola il dataset di ogni source usata nello snapshot;
- salva l'export frozen in `analyticsExports`;
- mantiene la tracciabilita' fino a `snapshotValue -> sourceExportIds -> analyticsExports`.

`createSnapshotRun` non esegue piu' tutto in una singola mutation: crea subito `snapshot` + `snapshotRun`, mette il job in coda e prosegue in background con stati osservabili:

- `queued`
- `loading`
- `processing`
- `deriving`
- `freezing`
- `completed`
- `error`

Tipicamente la tua app usa `createSnapshot`:

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
2. La tua app legge e normalizza i dati sorgente di dominio.
3. La tua app collega gli indicatori locali a OKRHub con `ensureIndicatorOkrhubLink(...)`.
4. La tua app materializza le righe dominio con un workflow host batch-safe.
5. I 3 cron statici del consumer richiamano `runScheduledRefreshes` usando i preset della source.
6. La tua app crea uno snapshot manuale con `createSnapshot(...)` solo quando vuole un run storico esplicito.
7. La tua app invia i nuovi `values` a OKRHub con `syncValuesToOkrhub(...)`.

## Modifiche minime richieste nell'app host

Se vuoi usare il package nel modo raccomandato, le modifiche minime lato app sono queste:

1. Registrare `kpiSnapshot` in `convex/convex.config.ts`.
2. Creare un file wrapper locale, ad esempio `convex/kpiSnapshot.ts`, che usi `exposeApi(...)`.
3. Implementare il tuo controllo accessi nell'opzione `auth`.
4. Creare almeno una mutation o action che legga le tue tabelle e costruisca `sourcePayloads`.
5. Definire i 3 cron statici del consumer che richiamano `runScheduledRefreshes`.
6. Chiamare la tua action/mutation host `createSnapshot` da UI o automazione quando serve uno snapshot manuale.

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

- lettura: `listProfiles`, `listProfileDefinitions`, `listProfileDataSources`, `listDataSources`, `listDerivedIndicators`, `simulateSnapshot`, `listSnapshots`, `getSnapshotRunStatus`, `listSnapshotValues`, `getSnapshotValueEvidenceDownloadUrl`, `getSnapshotExplain`, `listSnapshotRunErrors`, `listExports`, `getExportDownloadUrl`, `getDataSourceFilterOptions`
- report builder: `listReports`, `getReport`, `getReportBySlug`, `createReport`, `archiveReport`, `updateReportMeta`, `addReportWidget`, `removeReportWidget`, `reorderReportWidgets`, `getReportWidgetData`, `getReportWidgetsData`
- chart data: `getIndicatorHistory`, `getSnapshotIndicatorSlice`
- configurazione: `createSnapshotProfile`, `upsertDataSource`, `replaceMaterializedRows`, `upsertIndicator`, `rebuildIndicatorReportUsageCounters`, `upsertCalculationDefinition`, `upsertDerivedIndicator`, `transferIndicatorAcrossProfiles`, `replaceProfileDefinitions`, `toggleCalculation`

Nota migrazione:

- se hai già report salvati prima di introdurre `reportUsageCount`, esegui una volta `rebuildIndicatorReportUsageCounters()` dopo il deploy per riallineare i contatori storici
- esecuzione: `createSnapshotRun` async con polling esplicito dello stato job
- mapping esterni: `getIndicatorBySlug`, `getIndicatorByExternalId`, `setIndicatorExternalId`, `getValueByExternalId`, `listValuesForSync`, `setValueExternalId`
- integrazione OKRHub: `ensureIndicatorOkrhubLink`, `syncValuesToOkrhub`

## Modello dati del componente

Tabelle principali:

- `snapshotProfiles`
- `dataSources`
- `analyticsMaterializedRows`
- `analyticsExports`
- `indicators`
- `calculationDefinitions`
- `derivedIndicators`
- `snapshots`
- `reports`
- `reportWidgets`
- `snapshotRuns`
- `snapshotRunItems`
- `snapshotValues`
- `derivedSnapshotValues`
- `calculationTraces`
- `values`

Campi rilevanti per integrazione:

- `indicators.externalId` opzionale
- `values.externalId` opzionale
- `snapshotValues.sourceExportIds`
- `snapshotRunItems.sourceExportIds`
- `derivedSnapshotValues.sourceExportIds`

Questi campi restano opzionali per non rompere installazioni esistenti e sono usati solo quando vuoi collegare il componente a OKRHub o a un altro sistema esterno.

Per i run snapshot, i campi di stato ora seguono il workflow operativo completo:

- `snapshots.status`: `queued | loading | processing | deriving | freezing | completed | error`
- `snapshotRuns.status`: `queued | loading | processing | deriving | freezing | completed | error`
- `snapshotRunSources.status`: `pending | processing | completed | error`

## Migrazione da versioni precedenti

### Breaking change di versioning

Questa revisione introduce un breaking change intenzionale sul modello dei KPI:

- `upsertIndicator(...)` richiede `version`;
- `upsertDerivedIndicator(...)` richiede `version`;
- `upsertCalculationDefinition(...)` e `replaceProfileDefinitions(...)` richiedono la `indicatorVersion` target;
- `indicators` e `derivedIndicators` non sono piu` tabelle con uno `slug` univoco per profilo, ma famiglie di versioni identificate da `(profileId, slug, version)`;
- `snapshotValues`, `snapshotRunItems`, `integrationValues` e `derivedSnapshotValues` salvano la `version` usata durante il calcolo.

Dato che `kpi-snapshot` non ha ancora installazioni attive, non e` previsto alcun compat layer.

### Regola operativa consigliata

Quando un utente scopre un errore nella formula:

1. non modificare la versione esistente;
2. crea una nuova versione dello stesso `slug`;
3. collega le nuove `calculationDefinitions` alla nuova versione;
4. lascia che report e query `latest` risolvano automaticamente la versione piu` recente.

In questo modo:

- lo `slug` resta stabile per UI e report;
- il componente evita sovrascritture in place;
- gli snapshot futuri usano la versione corrente;
- i valori di sync possono conservare i metadati della versione usata al momento del calcolo.
- `profileSlug` per limitare il backfill a un profilo

### Copy o move tra profili

Il componente espone `transferIndicatorAcrossProfiles(...)` per copiare o spostare un KPI base o derivato tra due profili diversi.

Vincoli importanti:

- `mode: "copy"` lascia invariato il sorgente e crea il KPI nel profilo di destinazione;
- `mode: "move"` esegue prima tutte le validazioni e rimuove il sorgente solo se la copia e` valida;
- per KPI base vengono copiate anche tutte le `calculationDefinitions` collegate;
- per KPI derivati, tutte le dipendenze devono gia` esistere nel profilo target;
- il componente blocca sempre collisioni di `slug`;
- il componente blocca anche collisioni di `label` e `description` tra KPI base e derivati nello stesso profilo, per evitare ambiguita` in UI;
- non vengono mai applicate rinominazioni automatiche silenziose.

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
