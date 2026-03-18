# Changelog

Tutte le modifiche rilevanti al progetto sono documentate in questo file.

Il formato è basato su [Keep a Changelog](https://keepachangelog.com/it/1.0.0/).

## [Unreleased]

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
