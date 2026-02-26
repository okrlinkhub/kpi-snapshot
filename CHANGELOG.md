# Changelog

Tutte le modifiche rilevanti al progetto sono documentate in questo file.

Il formato è basato su [Keep a Changelog](https://keepachangelog.com/it/1.0.0/).

## [Unreleased]

- Ristrutturazione repo come pacchetto npm `@okrlinkhub/kpi-snapshot`.
- Componente spostato in `src/component/`, example app in `example/`.
- Build con `tsconfig.build.json`, codegen e output in `dist/`.
- Entry point client `src/client/index.ts` (tipi `ComponentApi`, `IndicatorValuePayload`).
- Test helper `src/test.ts` per convex-test (`register(t, name)`).
- README, PUBLISHING.md e CHANGELOG.md aggiunti.

## [0.1.0] - 2026-02-26

- Prima release come pacchetto npm.
- Componente Convex: external sources, sync runs, `pullFromExternal`, `syncToLinkHub`.
- Contratto writer (function handle LinkHub) e reader (dati da app utente) documentati.
