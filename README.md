# @okrlinkhub/component

Componente Convex per creare **KPI snapshot dinamici e configurabili**: definizione di profili, sorgenti dati, regole di calcolo (`sum`, `count`, `avg`, `min`, `max`, `distinct_count`), esecuzione snapshot manuale e tracciamento completo dei calcoli.

## Installazione

```bash
npm install @okrlinkhub/component
```

## Configurazione in `convex.config.ts`

Nella tua app Convex:

```ts
import { defineApp } from "convex/server";
import kpiSnapshot from "@okrlinkhub/component/convex.config.js";

const app = defineApp();
app.use(kpiSnapshot, { name: "kpiSnapshot" });
export default app;
```

Dopo `npx convex dev`, il componente espone le sue funzioni sotto `components.kpiSnapshot` (es. `components.kpiSnapshot.snapshotEngine.createSnapshot`). L’app tipicamente espone **wrapper** (query/mutation/action) che delegano a queste funzioni, così il client usa l’API dell’app invece dei path interni del componente.

## Modello dati del componente

Tabelle principali:

- `snapshotProfiles`, `dataSources`, `indicators`, `calculationDefinitions`
- `sourceRows` (righe materializzate su cui applicare le regole)
- `snapshots`, `snapshotRuns`, `snapshotRunItems`, `snapshotValues`, `calculationTraces`
- `values` (storico atomico dei KPI calcolati)

Questo approccio rende i calcoli modificabili a runtime, ispezionabili e facilmente debuggabili.

## API principali

Configurazione:

- `createSnapshotProfile`
- `upsertDataSource`
- `upsertIndicator`
- `upsertCalculationDefinition`
- `replaceProfileDefinitions`
- `listProfileDefinitions`

Esecuzione:

- `ingestSourceRows`
- `simulateSnapshot` (dry-run)
- `createSnapshot` (persistenza run + trace + values)
- `listSnapshots`
- `getSnapshotExplain`
- `listSnapshotRunErrors`

I valori calcolati restano nelle tabelle del componente (`snapshotValues`, `values`); nessun invio verso sistemi esterni.

## Note su scalabilità (Aggregate Component)

Per dataset molto grandi, puoi introdurre `@convex-dev/aggregate` su KPI “caldi” (sum/count/max/avg frequenti) mantenendo la trasparenza dei run tramite `snapshotRunItems` e `calculationTraces`.

## Tipi e helper

Per tipizzare l’API del componente o i payload:

```ts
import type { ComponentApi } from "@okrlinkhub/component";
```

## Test con convex-test

Se usi [convex-test](https://docs.convex.dev/testing/convex-test), puoi registrare il componente così:

```ts
import { convexTest } from "convex-test";
import { register } from "@okrlinkhub/component/test";

const t = convexTest(schema, modules);
register(t, "kpiSnapshot");
// poi usa t.run(...) come di consueto
```

## Sviluppo locale (dal repo del componente)

Dalla root del repo:

1. `npm install`
2. `npm run build:codegen` (genera `src/component/_generated` e compila in `dist/`)
3. Per far risolvere l’example a questo pacchetto: `npm link` e poi `npm link @okrlinkhub/component`
4. `npm run dev` (avvia convex dev sull’example e, se configurato, watch della build)

L’example app usa `example/convex` (vedi `convex.json` con `"functions": "example/convex"`) e importa il componente da `@okrlinkhub/component/convex.config.js`.

## Documentazione Convex

- [Authoring Components](https://docs.convex.dev/components/authoring)
- [Function Handles](https://docs.convex.dev/functions/function-handles)
