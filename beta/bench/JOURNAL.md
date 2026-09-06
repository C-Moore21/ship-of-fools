# Click-latency journal

The loop's memory. Every iteration appends here **before** and **after** it acts, so
later iterations never re-try a hypothesis that already failed.

Metric: `meanSettleP75` from `bench/results/latest.json` — the mean across scenarios of
p75(click → last DOM mutation painted), measured in **replay mode** (fixtures + fixed
250ms API latency) so deltas are frontend deltas, not Atlas jitter.

**Goal: meanSettleP75 ≤ 50% of `bench/baseline.json`.**

---

## Status

| | |
|---|---|
| Baseline | **149.4ms** mean settle p75 (2026-09-06, replay @250ms, logged out) |
| Goal | **74.7ms** |
| Latest | 149.4ms (baseline run) |
| Iterations | 0 |

Baseline per scenario:

| scenario | settle p75 | first paint | commits |
|---|---|---|---|
| show-click-cold | **861.8ms** | 818.3ms | 2 |
| year-click | 70.5ms | 9.4ms | 4 |
| year-click-dense | 70.5ms | 7.6ms | 4 |
| open-observatory | 52.6ms | 19.0ms | 1 |
| tab-back-to-browse | 47.4ms | 13.8ms | 3 |
| show-click-warm | 38.6ms | 13.0ms | 3 |
| open-search | 37.5ms | 3.2ms | 1 |
| tab-stats | 16.1ms | 7.7ms | 1 |

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

### H1 — `useShow` blocks first paint on setlist-stats *(open, high confidence)*
`beta/src/hooks/useSofData.ts:246-254` awaits `cachedSetlistStats(...)` before resolving,
even though the comment directly above says it is "fetched in the background so the setlist
appears immediately". It is not. A cold show click is **three serial round trips**:
`[sources, weather]` → `tracks` → `setlist-stats`, and nothing renders until hop 3 lands.
Badges (Debut/Bust/Gap/Drought) are cosmetic and should merge in a second update.
**Expected:** removes ~1 full RTT from `show-click-cold`.
**Approach:** resolve after hop 2; have `useShow` expose a progressive second state that
merges `applySetlistStats` when the POST resolves. `useAsync` currently sets state exactly
once — it needs an optional "refine" callback, or `useShow` needs its own effect.

### H2 — hover prefetch stops one hop short *(open, high confidence)*
`prefetchShow` (`useSofData.ts:182`) warms only sources + weather. Its comment says tracks
depend on "which source is chosen", but source choice is deterministic — the same
`sort by archive_rating desc` in `useShow`. Extract that pick into a shared function and
let prefetch chain sources → tracks → setlist-stats. A hovered row then clicks warm.
**Expected:** near-zero `show-click-cold` for any row the pointer crossed.
**Watch:** don't fan out on fast pointer sweeps — gate on a short dwell (~80ms) so a mouse
crossing 40 rows doesn't fire 120 requests.

### H3 — `/api/shows/<date>` one-shot endpoint exists but is unused *(open, medium)*
CLAUDE.md documents `/api/shows/<date>` as "one-shot detail (venue + weather + community
stats + sources)", yet `useShow` still issues separate `sources` + `weather` calls.
Either the hook should use it, or it should be extended to also carry the best source's
tracks + setlist stats so a cold click is **one** round trip.
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

_none yet_

## Rejected

_none yet — record every failed attempt here with its measured delta, so it is never retried_
