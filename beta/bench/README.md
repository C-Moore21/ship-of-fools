# Click-latency bench

Measures how long the beta UI takes to respond to a click, and gives the
self-improving loop (`/perf-loop`) a number it cannot argue with.

## What it measures

For each scenario, `probe.mjs` is injected before app code and records:

| metric | meaning |
|---|---|
| `settleMs` | click → last DOM mutation, painted. **The headline.** |
| `firstPaintMs` | click → first DOM mutation painted. What the click *feels* like. |
| `blockingMs` | long-task time inside the window. Main-thread cost / jank. |
| `handlerMs` | the click handler's own duration (Event Timing API). |
| `batches` | MutationObserver callbacks — a proxy for React commit count. |

Timings are rebased on the browser's own `click` event timestamp, so Playwright's
driver overhead never lands in the measurement.

`overall.meanSettle` — the mean of each scenario's **median** settle over 15 reps —
is the single number the loop drives down.

Median, not p75, and 15 reps rather than 7. p75 of 7 samples is the 6th of 7, so it
tracks the worst case; two runs of an identical build disagreed by 100% on the
sub-100ms scenarios under it. `compare.mjs` additionally refuses to call a delta real
unless it clears both 8% and 5ms, and reports each scenario's IQR so a bimodal result
(one code path sometimes, another the rest of the time) is visible rather than averaged
away. Don't lower the reps to make runs faster.

## Modes

- **`replay`** (default) — every `/api/**` call is fulfilled from a recorded fixture
  with a fixed 250ms delay, Google Fonts is stubbed, and the bundle is served by
  `server.mjs` (a stand-in for Flask's `/beta` route built from the Vite manifest).
  Deterministic: any delta is a frontend delta, not Atlas jitter. **This is the loop's mode.**
- **`record`** — proxies `/api/**` to local Flask and saves every response to `fixtures/`.
- **`live`** — real Flask, real MongoDB, network included. Use to sanity-check that
  replay wins translate; do not use it for the loop, it is far too noisy.

## First-time setup

Fixtures come from a real Flask instance, so record them once:

```powershell
# terminal 1 — Flask needs your Atlas connection string
$env:SECRET_KEY="bench-local"
$env:MONGO_URI="<your atlas uri>"
python app.py
```

```powershell
# terminal 2
cd beta
npm run build
npm run bench:record      # populates bench/fixtures/
npm run bench             # replay run
npm run bench:baseline    # freeze it — the 50% goal is measured from here
```

Stop Flask afterwards; replay runs need nothing but Chrome.

## Day to day

```
cd beta
npm run build && npm run bench && npm run bench:compare
```

`compare.mjs` exit codes: `0` goal met · `1` improved, not there yet · `2` regressed vs
the previous run · `3` broken run (page errors / too few samples).

Useful flags: `--only=show-click-cold` · `--reps=11` · `--headed` · `--api-delay=0`
(isolate pure render cost) · `--label="what changed"`.

## Files

| file | role |
|---|---|
| `latency.mjs` | runner — modes, routing, stats, report |
| `probe.mjs` | in-page instrumentation, injected pre-app |
| `scenarios.mjs` | the eight click scenarios; targets discovered from the DOM |
| `server.mjs` | static server reproducing Flask's `/beta` from the Vite manifest |
| `compare.mjs` | latest vs baseline vs previous; drives the loop's exit codes |
| `JOURNAL.md` | the loop's memory — hypotheses, landed, **rejected** |
| `baseline.json` | frozen reference run (committed) |
| `results/`, `fixtures/` | gitignored — machine-specific |

## Caveat

Numbers are only comparable within one machine, one Chrome version, and one fixture
set. Re-record fixtures and re-baseline after backend response shapes change.
