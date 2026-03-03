# Pubblicazione del pacchetto @okrlinkhub/kpi-snapshot

Istruzioni per il maintainer per build, verifica e publish su npm.

## Prerequisiti

- Account npm con permessi sullo scope `@okrlinkhub`
- Node 20+ (consigliato, come nel template Convex)

## Login npm

```bash
npm login
```

Inserisci utente, password e OTP se abilitato.

## Build e verifica pre-publish

1. **Build pulita**

   ```bash
   npm run build:clean
   ```

   Esegue `rm -rf dist *.tsbuildinfo` e poi `build:codegen` (codegen del componente + `tsc`). Richiede rete per `convex codegen`.

2. **Typecheck**

   ```bash
   npm run typecheck
   ```

3. **Lint** (opzionale)

   ```bash
   npm run lint
   ```

4. **Verifica locale con `npm pack`**

   ```bash
   npm pack
   ```

   Crea un file `okrlinkhub-kpi-snapshot-0.1.0.tgz`. In un altro progetto di test:

   ```bash
   npm install /path/to/okrlinkhub-kpi-snapshot-0.1.0.tgz
   ```

   Verifica che gli import `@okrlinkhub/kpi-snapshot`, `@okrlinkhub/kpi-snapshot/convex.config.js` e `@okrlinkhub/kpi-snapshot/test` funzionino.

5. **Example app in repo**

   Dalla root del repo, dopo `npm run build:codegen`, avvia l’example:

   - `npm link` e poi `npm link @okrlinkhub/kpi-snapshot` (nella root) se l’example deve risolvere il pacchetto da locale, oppure
   - assicurati che `convex dev` usi `example/convex` (es. `convex.json` con `"functions": "example/convex"`) e che l’example importi da `@okrlinkhub/kpi-snapshot/convex.config.js`. Dopo la build, il pacchetto in `node_modules` (o linkato) fornirà gli export da `dist/`.

## Publish

1. **Versione**

   ```bash
   npm version patch   # oppure minor / major
   ```

   Aggiorna la versione in `package.json` e crea un tag git.

2. **Pubblicazione**

   ```bash
   npm publish --access public
   ```

   `--access public` è obbligatorio per pacchetti con scope (es. `@okrlinkhub/...`).

3. **Push e tag**

   ```bash
   git push --follow-tags
   ```

## Script di release

Per fare patch + publish + push in un colpo solo:

```bash
npm run release
```

Lo script è definito in `package.json` come:

`npm version patch && npm publish --access public && git push --follow-tags`

Esegui `npm run build:clean` e i test/typecheck/lint **prima** di `npm run release`.

## Riepilogo comandi

| Comando              | Descrizione                          |
|----------------------|--------------------------------------|
| `npm run build:clean`| Build pulita (codegen + tsc)         |
| `npm run typecheck`  | Solo typecheck                       |
| `npm run lint`       | Typecheck + eslint                   |
| `npm pack`           | Crea il tarball per verifica locale  |
| `npm version patch`  | Incrementa patch e crea tag         |
| `npm publish --access public` | Pubblica su npm              |
| `npm run release`    | version patch + publish + push       |
