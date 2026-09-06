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

**Goal: meanSettleP75 ≤ 50% of `bench/baseline.json`.**

---

## Status

| | |
|---|---|
| Baseline | **137.3ms** mean settle (2026-09-06, replay @250ms, 15 reps, logged out) |
| Goal | **68.7ms** |
| Latest | **101.0ms** (-26.4%) after H1 |
| Iterations | 1 |

Per scenario (median settle, ±IQR):

| scenario | baseline | after H1 | delta |
|---|---|---|---|
| show-click-cold | 838.4ms | **572.6ms** ±24.7 | **-31.7%** |
| year-click | 50.6ms | 46.0ms ±11.3 | noise |
| year-click-dense | 46.2ms | 50.4ms ±13.0 | noise |
| tab-back-to-browse | 44.9ms | 34.7ms ±19.9 | -22.7% |
| open-observatory | 43.7ms | 37.8ms ±16.0 | -13.5% |
| show-click-warm | 30.4ms | 28.7ms ±11.8 | noise |
| open-search | 26.1ms | 22.0ms ±15.7 | noise |
| tab-stats | 17.7ms | 15.9ms ±6.8 | noise |

Two things to keep honest:

- **`show-click-cold` is 72% of the mean.** That is not a flaw in the metric — it is
  genuinely the slowest thing in the app, and 861.8ms against ~750ms of synthetic
  network confirms three serial round trips. But it does mean H1+H2 alone could hit
  the goal, so keep checking the other seven scenarios do not regress while it drops.
- **Fixtures were recorded logged out** (`/api/auth/me` -> `{"username":null}`), from
  production. Ratings, notes, listens and the section panels are therefore near-empty,
  so `tab-stats` is not representative of Camden's logged-in view. Re-record against a
  logged-in session if the section tabs ever become the target.

---

## Hypothesis queue

Ordered by expected win. The loop takes the top **unresolved** item each iteration,
implements it, re-measures, and moves it to Landed or Rejected.

### H2 — hover prefetch stops one hop short *(open, high confidence — but NOT measurable as-is)*
`prefetchShow` warms only sources + weather. Its comment says tracks depend on "which
source is chosen", but that choice is deterministic — the same `sort by archive_rating
desc` that `useShow` does. Extract the pick into a shared function and let prefetch chain
sources → tracks → setlist-stats. A hovered row then clicks warm.

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

### H3 — `/api/shows/<date>` one-shot endpoint exists but is unused *(open — TAKE THIS NEXT)*
CLAUDE.md documents `/api/shows/<date>` as "one-shot detail (venue + weather + community
stats + sources)", yet `useShow` still issues separate `sources` + `weather` calls.
Either the hook should use it, or it should be extended to also carry the best source's
tracks + setlist stats so a cold click is **one** round trip.
Unlike H2 this pays off on every click, hovered or not, so it is measurable in the
current bench and does not need a re-baseline. With H1 landed, a cold click is now two
serial trips (`[sources, weather]` → `tracks`); collapsing them to one should take
`show-click-cold` from ~572ms to ~320ms, which lands the overall goal.

**Constraint:** must be served from MongoDB cache only — no synchronous Archive.org calls
(Render 30s worker timeout, and Archive.org blocks Render's IP).

### H4 — year click re-renders the whole show list *(open, medium)*
`ShowList` maps every row inline; `year-click-dense` should show it. Rows are not memoized
the way `TrackRow` is in `Setlist.tsx`. Extract a memoized `ShowRow` and check the
`commits` column in `compare.mjs` output — it counts MutationObserver batches, which
tracks React commit churn.

### H5 — `useShow` runs on every `selectedId` change with no cancellation of hop 2/3 *(open, low)*
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

## Rejected

_none yet — record every failed attempt here with its measured delta, so it is never retried_
