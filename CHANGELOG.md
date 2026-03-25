# Changelog

Tutte le modifiche rilevanti al progetto sono documentate in questo file.

Il formato è basato su [Keep a Changelog](https://keepachangelog.com/it/1.0.0/).

## [Unreleased]

## [1.1.0] - 2026-03-25

### Breaking changes

- `reportWidgets` non usa piu` il vecchio shape `1 widget = 1 indicatore`: il modello ora distingue `single_value` e `chart`, con `members[]`, `layout`, `chartKind` e `timeRange`.
- `createSnapshotRun` è ora puramente async: crea `snapshot` + `snapshotRun`, ritorna subito e completa il lavoro in background.
- `snapshots.status` e `snapshotRuns.status` usano ora il workflow operativo `queued | loading | processing | deriving | freezing | completed | error`.
- Nuova tabella `snapshotRunSources` per tracciare avanzamento e retry per source.
- `requestExport` e `regenerateExport` convergono sul workflow batch-safe di `exportWorkflows` invece del vecchio path monolitico.

### Changed

- Rimossi i full-scan `.collect()` dai path critici di snapshot/export; il caricamento delle righe materializzate avviene ora a batch.
- Il freeze export audit è parte dello stato finale del run snapshot, non un side effect fuori banda.
- Aggiunta query `getSnapshotRunStatus` per polling UI e osservabilità del progresso.
- `materializationReader.listMaterializableRows` è ora paginata con `cursor` e `batchSize`, per evitare letture monolitiche anche sui namespace letti tramite reader del componente.
- Aggiunte query `getIndicatorHistory`, `getSnapshotIndicatorSlice`, `getReportWidgetData` e `getReportWidgetsData` per costruire report grafici riusabili nei consumer.
- `addReportWidget` accetta ora widget persistiti chart-first, inclusi trend storici e pie chart multi-indicatore.
- Le query runtime orientate alla UI ora normalizzano automaticamente i valori con `indicatorUnit = '%'` moltiplicandoli per `100` nel payload di trasporto, mantenendo invariato il dato grezzo persistito nel database.

## [1.0.0] - 2026-03-23

### Breaking changes

- Rimossa esplicitamente la tabella `sourceRows` dal componente.
- `createSnapshot` e `simulateSnapshot` lavorano ora su `sourcePayloads` passati dall'app host.
- Ogni `snapshotValue` genera e salva la propria evidence CSV su storage Convex con `evidenceRef`.
- Aggiunti i metadati evidence su `snapshotValues` e `snapshotRunItems`.
- Rimossi `values.sourceRowId` e `calculationTraces.sampleRowIds`, sostituiti da preview e metadata file-based.
- Esteso `exposeApi(...)` con `listProfileDataSources`, `listSnapshotValues` e `getSnapshotValueEvidenceDownloadUrl`.
- README aggiornato con migrazione obbligatoria: svuotare `sourceRows` prima dell'upgrade.

## [0.2.0] - 2026-03-18

- Aggiunti `externalId` opzionali a `indicators` e `values` per il collegamento con OKRHub.
- Estesa l'API del componente con query e mutation per leggere e aggiornare i mapping esterni.
- Nuovo helper client-side `exposeApi(...)` per generare wrapper host-side con auth e integrazione opzionale con `@okrlinkhub/okrhub`.
- Aggiunti helper integrati `ensureIndicatorOkrhubLink` e `syncValuesToOkrhub`.
- README riscritto per distinguere setup standalone, setup minimo nell'app host e setup ottimizzato LinkHub universe.
- Aggiornati i template wrapper e aggiunto `templates/kpiSnapshotOkrhubWrapper.example.ts`.

## [0.1.0] - 2026-02-26

- Prima release come pacchetto npm.
- Componente Convex: external sources, sync runs, `pullFromExternal`.
- Valori snapshot mantenuti nelle tabelle del componente (nessun invio esterno).
