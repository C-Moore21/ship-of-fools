# Click-latency journal

The loop's memory. Every iteration appends here **before** and **after** it acts, so
later iterations never re-try a hypothesis that already failed.

Metric: `overall.meanSettle` from `bench/results/latest.json` — the mean across scenarios
of the **median** of (click → last DOM mutation painted), over 15 reps, measured in
**replay mode** (fixtures + fixed 250ms API latency) so deltas are frontend deltas, not
Atlas jitter.

> The first baseline used p75 over 7 reps and had to be thrown away. p75 of 7 samples is
> the 6th of 7 — effectively the max — and two runs of an *identical* build disagreed by
> 100% on the sub-100ms scenarios, which made H1 look like a 40% win when it was 26%.
> Median of 15 plus `compare.mjs`'s noise floor (8% **and** 5ms) is what makes a delta
> mean something. Do not lower the reps to make the loop faster.

**Goal: `meanSettle` ≤ 50% of `bench/baseline.json`.** — met at iteration 2.

---

## Status

| | |
|---|---|
| Baseline | **137.3ms** mean settle (2026-09-06, replay @250ms, 15 reps, logged out) |
| Goal | **68.7ms** |
| Latest | **58.1ms** (-57.7%) after H1 + H3 — **GOAL MET** (runs: 57.4 / 51.2 / 53.0 / 58.1) |
| Iterations | 2 |

Per scenario (median settle, ±IQR):

| scenario | baseline | after H1 | after H1+H3 | total |
|---|---|---|---|---|
| show-click-cold | 838.4ms | 572.6ms | **288.3ms** ±14.2 | **-65.6%** |
| year-click | 50.6ms | 46.0ms | 50.3ms ±17.5 | noise |
| year-click-dense | 46.2ms | 50.4ms | 42.3ms ±10.2 | noise |
| tab-back-to-browse | 44.9ms | 34.7ms | 15.9ms ±4.8 | -64.6% |
| open-observatory | 43.7ms | 37.8ms | 17.7ms ±3.6 | -59.5% |
| show-click-warm | 30.4ms | 28.7ms | 14.3ms ±5.6 | -53.0% |
| open-search | 26.1ms | 22.0ms | 15.7ms ±12.1 | -39.8% |
| tab-stats | 17.7ms | 15.9ms | 14.9ms ±14.3 | noise |

### Not yet true in production

H3's win depends on `app.py`'s new `?include=tracks` branch, which is **not deployed**.
The bench measures it through synthesized fixtures (real production payloads reassembled
into the new envelope — see `synth-oneshot.mjs`). Against the currently-deployed backend
the parameter is ignored, `tracks` is absent, and the client correctly falls back to the
old two-trip path. Until `app.py` ships, users get H1's -26% and not H3's.

Two things to keep honest:

- **`show-click-cold` dominated the baseline** at 838.4ms of a 137.3ms mean — genuinely
  the slowest thing in the app, and it confirmed three serial round trips. It is now
  288.3ms and no longer dominates, so future iterations should watch the whole table
  rather than chasing one row.
- **Fixtures were recorded logged out** (`/api/auth/me` -> `{"username":null}`), from
  production. Ratings, notes, listens and the section panels are therefore near-empty,
  so `tab-stats` is not representative of Camden's logged-in view. Re-record against a
  logged-in session if the section tabs ever become the target.

---

## Hypothesis queue

Ordered by expected win. The loop takes the top **unresolved** item each iteration,
implements it, re-measures, and moves it to Landed or Rejected.

### H2 — hover prefetch stops one hop short *(open, high confidence — but NOT measurable as-is)*
**Partly overtaken by H3.** `prefetchShow` now warms the one-shot detail, which already
carries sources, weather and (on a server cache hit) the tracklist — so a hovered row is
mostly warm already. What remains is setlist-stats, which no longer blocks paint after H1.
The shared source pick this hypothesis called for exists now as `pickBestSource`.
Re-assess whether there is anything left here before implementing it.

**Read this before implementing it.** `show-click-cold` will show almost no improvement,
and that is a harness artifact, not a verdict. Playwright's `.click()` moves the pointer
onto the element immediately before pressing, so the prefetch fires a few ms before the
click — where a real user dwells for hundreds of ms. Implementing H2 and then rejecting it
on a flat number would be wrong.

To measure it properly, add a `show-click-hovered` scenario that hovers, waits ~400ms, then
clicks. That changes the scenario set and therefore the mean, so it needs a re-baseline —
do that deliberately in its own iteration, and keep `show-click-cold` (no dwell) as the
worst case.

**Watch:** gate on a short dwell (~80ms) so a pointer crossing 40 rows does not fire 120
requests.

### H4 — year click re-renders the whole show list *(open, medium)*
`ShowList` maps every row inline; `year-click-dense` should show it. Rows are not memoized
the way `TrackRow` is in `Setlist.tsx`. Extract a memoized `ShowRow` and check the
`commits` column in `compare.mjs` output — it counts MutationObserver batches, which
tracks React commit churn.

### H5 — `useShow` runs on every `selectedId` change with no cancellation of hop 2/3 *(open, low — but there is now evidence)*

Every replay run reports exactly one fixture miss: `/api/shows/1977-02-17/setlist-stats`.
It survived a targeted re-record, which means the request is not simply un-recorded — a
stale chain from a previously-selected show is firing its stats POST after the scenario
has moved on, with a body that never occurs in a clean sequence. It lands in the unmeasured
`prepare` phase and `show-click-cold` stays stable at ~292ms ±18, so it does not affect the
numbers — but it is a real symptom of the uncancelled work described below.
Clicking through five shows quickly leaves four in-flight chains that still resolve and
still `applySetlistStats`. `useAsync`'s `alive` flag drops the *state write* but not the
work. Cheap to fix, matters most on fast browsing.

### H6 — `Browse` subscribes to all audio events *(open, low — carried over)*
Known from the earlier perf sweep and never landed. `Browse` uses the structural
`useSofAudio`; a status-only split would stop a `showchange` from re-rendering the tree.
Only worth doing if a scenario actually shows it.

---

## Landed

### H1 — `useShow` blocked first paint on setlist-stats — **-31.7% on show-click-cold, -26.4% overall**
`useSofData.ts` awaited `cachedSetlistStats` before resolving, despite the comment above it
claiming the opposite ("fetched in the background so the setlist appears immediately"). It
made a cold show click three serial round trips with nothing on screen until hop 3.

Fix: `useAsync` now takes a `publish` callback so a run can paint an early, incomplete
result; `useShow` publishes the hydrated show as soon as tracks land and returns the
stats-decorated version when the POST resolves. 838.4ms -> 572.6ms, which is one 250ms
round trip plus overhead — exactly what was predicted.

`tab-back-to-browse` (-22.7%) and `open-observatory` (-13.5%) also cleared the noise floor;
both remount a tree containing show detail, so they plausibly ride the same fix.

### H3 — collapsed the cold show click to one round trip — **-65.6% on show-click-cold, -58.2% overall, GOAL MET**
`/api/shows/<date>` already returned sources + weather + community aggregates but was
never called; `useShow` fetched sources and weather separately (already parallel, so no
win there) and then had to wait for a second serial trip for tracks, because tracks cannot
be requested until sources names one.

`?include=tracks` now folds the top source's cached tracklist into that response, so a cold
click is one round trip. Cache-only by design — falling through to Archive.org would put a
slow third-party call on the critical path of the most-clicked endpoint, and Archive.org
blocks Render's IP anyway. On a miss the key is absent and the client falls back.

**H3a alone left it bimodal**: 8 reps at ~280ms, 7 at ~540ms, IQR 247ms. The client picked
its source by `archive_rating` alone while the server ranks by composite score, so on
roughly half of shows the client discarded the bundled tracklist and re-fetched.

**H3b fixed the ranking mismatch** and the IQR collapsed to 14ms. This is also a real bug
fix independent of speed: sorting on raw rating put a 5.0-with-one-review above a
4.79-with-297-reviews. `pickBestSource` now prefers the server's `recommended` flag.

> **Behaviour change worth knowing about:** for shows where the two rankings disagreed,
> a different recording is now selected by default. It is the better-supported one by the
> app's own documented scoring, but it is a change to what auto-plays.

## Rejected

_none yet — record every failed attempt here with its measured delta, so it is never retried_
