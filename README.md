# @okrlinkhub/kpi-snapshot

Componente Convex che funge da **connettore KPI snapshot** verso LinkHub: gestisce external sources, run di sync e l’invio di batch di valori (`indicatorSlug`, `value`, `date`) a LinkHub tramite un function handle (writer).

## Installazione

```bash
npm install @okrlinkhub/kpi-snapshot
```

## Configurazione in `convex.config.ts`

Nella tua app Convex:

```ts
import { defineApp } from "convex/server";
import kpiSnapshot from "@okrlinkhub/kpi-snapshot/convex.config.js";

const app = defineApp();
app.use(kpiSnapshot, { name: "kpiSnapshot" });
export default app;
```

Dopo `npx convex dev`, il componente espone le sue funzioni sotto `components.kpiSnapshot` (es. `components.kpiSnapshot.externalSources.listExternalSources`). L’app tipicamente espone **wrapper** (query/mutation/action) che delegano a queste funzioni, così il client usa l’API dell’app invece dei path interni del componente.

## Contratto reader (app utente)

Il componente non legge direttamente dalla tua app. Tu fornisci i dati da inviare a LinkHub in uno di questi modi:

- **`pullFromExternal`**: l’app chiama questa action del componente passando `externalSourceId`, e opzionalmente `deploymentUrl` / `authToken`. Il componente (o un’implementazione lato app) può usare questi dati per fare fetch da un’API esterna e popolare le tabelle interne del componente. In futuro il componente potrà invocare un **reader** (es. function handle di una query/action della tua app che restituisce `{ indicatorSlug, value, date }[]`).
- **`syncToLinkHub`**: l’app chiama questa action passando un batch `{ indicatorSlug, value, date }[]`, il `writeHandle` (function handle della mutation LinkHub) e `linkHubCompanyId`. I dati del batch possono provenire da una tua query/action (reader) che li prepara.

In sintesi: i dati da inviare possono essere costruiti dall’app (es. da una query che legge le tue tabelle e formatta gli oggetti `{ indicatorSlug, value, date }`) e poi passati a `syncToLinkHub`.

## Contratto writer (LinkHub)

LinkHub deve esporre una **mutation** (o un endpoint HTTP equivalente) che accetta un singolo valore KPI, ad esempio:

- `companyId: string`
- `indicatorSlug: string`
- `value: number`
- `date: number`

L’app che usa il componente ottiene un **function handle** per questa mutation (es. tramite `createFunctionHandle(api.kpiSnapshot.writeIndicatorValue)` o l’equivalente lato LinkHub) e lo passa a `syncToLinkHub` come `writeHandle`. Il componente invoca il handle per ogni elemento del batch.

## Esempio di cron nell’app utente

Puoi schedulare un cron che legge i dati dalla tua app e li invia a LinkHub:

```ts
// convex/crons.ts (o simile)
import { cronJobs } from "convex/server";
import { internal } from "./_generated/api";

const crons = cronJobs();
crons.interval(
  "sync-kpi-to-linkhub",
  { minutes: 60 },
  internal.sync.syncToLinkHub,
  {
    writeHandle: "<function handle da LinkHub>",
    linkHubCompanyId: "my-company",
    batch: [], // in pratica l’app potrebbe prima chiamare un’action che costruisce il batch
  }
);
export default crons;
```

In produzione il `batch` verrebbe tipicamente costruito da un’action che legge le tue tabelle (o le tabelle del componente dopo un `pullFromExternal`) e formatta l’array `{ indicatorSlug, value, date }[]`.

## Tipi e helper

Per tipizzare l’API del componente o i payload:

```ts
import type { ComponentApi, IndicatorValuePayload } from "@okrlinkhub/kpi-snapshot";
```

## Test con convex-test

Se usi [convex-test](https://docs.convex.dev/testing/convex-test), puoi registrare il componente così:

```ts
import { convexTest } from "convex-test";
import { register } from "@okrlinkhub/kpi-snapshot/test";

const t = convexTest(schema, modules);
register(t, "kpiSnapshot");
// poi usa t.run(...) come di consueto
```

## Sviluppo locale (dal repo del componente)

Dalla root del repo:

1. `npm install`
2. `npm run build:codegen` (genera `src/component/_generated` e compila in `dist/`)
3. Per far risolvere l’example a questo pacchetto: `npm link` e poi `npm link @okrlinkhub/kpi-snapshot`
4. `npm run dev` (avvia convex dev sull’example e, se configurato, watch della build)

L’example app usa `example/convex` (vedi `convex.json` con `"functions": "example/convex"`) e importa il componente da `@okrlinkhub/kpi-snapshot/convex.config.js`.

## Documentazione Convex

- [Authoring Components](https://docs.convex.dev/components/authoring)
- [Function Handles](https://docs.convex.dev/functions/function-handles)
