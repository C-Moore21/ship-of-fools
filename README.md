# Ship of Fools

A Grateful Dead live concert browser built with Python/Flask, sourcing recordings directly from [Archive.org](https://archive.org/details/GratefulDead). Deployed at [ship-of-fools.onrender.com](https://ship-of-fools.onrender.com).

**Stack**: Python 3 + Flask + Gunicorn · MongoDB Atlas M0 · Render free tier · Vanilla HTML/CSS/JS

---

## Features

### Browsing & Discovery
- Browse every Grateful Dead year (1965–1995)
- Shows grouped by date with venue and location
- **Search** — find shows by date, venue, or location
- **Today in History (TIH)** — all recordings from today's date across every year, ranked by composite score, with a hero card and one-tap play
- **On This Tour** — shows within 30 days of the selected show, same year
- **Venue History** — other shows played at the same city/location

### Recordings & Scoring
- All recordings per show listed with type badges — **SBD**, **MTX**, **FOB**, **AUD** — sorted by a weighted composite quality score
- Composite score: Bayesian confidence-adjusted rating × source type multiplier (SBD 1.0 · MTX 0.9 · FOB 0.8 · AUD 0.7)
- Taper, transferer, and lineage info per recording
- Full setlist with track durations
- **Source selector** — collapsible menu, auto-collapses after selection

### Playback & Audio
- **Gapless playback** — dual audio element swap pattern for seamless track transitions
- **Aggressive pre-buffering** — preloads N+1 track; at 75% through current track, advances to N+2 if ready or kicks off fallback source fetch
- **Automatic source failover** — retries 3× on buffer error, then switches to next best recording automatically
- **Media Session API** — lock screen and AirPlay controls (iOS Safari + Apple TV), Android lock screen seek bar
- **Remote Playback API** — Chromecast support on Android Chrome
- Gapless swap disabled on iOS/AirPlay and when casting to respect platform constraints

### Personal & Social
- **User accounts** — register/login with session-based auth
- **Track ratings** — rate individual tracks 1–5 stars (half-star increments) inline
- **Show ratings** — rate entire shows 1–5 stars
- **Notes** — save personal notes per show
- **Listening history** — your most recent plays, collapsed by show
- **My Rated Shows & Tracks** — collapsible by show with totals
- **Leaderboard** — weekly, monthly, all-time with display name support

### Dark Star Observatory
Three-tab analytics modal (`⬡ Song Heatmap | 🗺 Crow's Nest | ◈ My Stats`):

- **Song Heatmap** — ~50 songs × 1965–1995 grid. Cells colored by avg Archive.org rating (blue→gold→red), opacity = review volume. Click a song name to drill into the scatter plot (duration × year, colored by source type). Navigation between songs via prev/next buttons. Backed by MongoDB; background warm-up thread populates at startup.
- **Crow's Nest** — Canvas Mercator map of the US + Europe. Every show plotted with era-colored glowing dot. Filled US land polygon with Great Lakes, state border overlays from US states GeoJSON. Scroll to zoom, drag to pan, Year/Month/Day scrubber modes, animated Play button. Click a city cluster → drill-down panel with venue-grouped show list.
- **My Stats** — Inline listening stats: total time, era affinity bar, top songs by show count, top shows by time. Year filter.

### Tour Runs
- Era-grouped accordion of named tour runs (1965–1995)
- Per-run show roster with ✓/○ checkmarks, progress bar, cohort table

### Navigation & Deep Linking
- Shareable URLs per show and recording (`?show=1977-05-08&src=gd1977-05-08.sbd...`)
- Mobile-friendly drill-down panel navigation and stacked player bar

---

## Architecture & Design Notes

### Free-Tier Constraints
- **MongoDB Atlas M0** — 512 MB shared storage, max 3 indexes per collection (including `_id`). All listen aggregation done in a single query with in-process grouping. LRU in-memory cache (500 entries, 5-min TTL) for Archive.org metadata.
- **Render free tier** — dyno sleeps after 15 min inactivity; self-ping keep-alive thread spawned automatically when `RENDER_EXTERNAL_URL` is set.
- **Archive.org** — no API key required; public search and metadata endpoints used directly; audio served from their download CDN.

### Performance
- **LRU cache** — bounded 500-entry `OrderedDict` (5-min TTL) for Archive.org metadata responses
- **Three-tier Observatory cache** — LRU in-memory → MongoDB `observatory_cache` → live fetch
- **Background warm-up thread** — two-pass daemon: heatmap (search-only, fast) for all ~50 songs, then scatter (per-recording metadata) for 12 improv-heavy songs; 90s startup delay
- **Query version flag** — `_OBS_QUERY_VERSION` bumped to force re-scrape when search query changes
- **Map cache** — `shows_map_cache` MongoDB collection, populated in background at startup; `/api/shows/map` never blocks on Archive.org
- **DOM caching** — player time/duration elements cached; playback row targeting by ID
- **Throttled timeupdate** — 250 ms throttle on audio time handler

### Reliability
- **Optimistic UI** — ratings and notes update instantly; revert on API error
- **Try/catch on all fetches** — all user-facing API calls wrapped with state revert fallback
- `_sourceLoading` flag prevents concurrent `switchSource()` calls during slow metadata fetches
- **Gunicorn-safe** — no synchronous Archive.org calls on request threads for map/observatory endpoints

### Gapless Playback & Failover
- Dual audio elements (`audio`, `preload`) swap at `playTrack()` if preload has buffered (readyState ≥ 3)
- At 75% playback, `preloadTrack(N+2)` is queued; if N+1 isn't ready, `_preloadFallbackSource` prefetches the next best recording
- Retry loop (3 attempts, 500 ms × attempt backoff) on preload error; on final failure, injects buffered fallback source without audio interruption

### Admin
- `POST /api/admin/rename-user` — renames a user across all collections atomically

---

## Stack

| Layer | Tech |
|---|---|
| Backend | Python 3, Flask, Gunicorn |
| Data | Archive.org public API (no key required) |
| Database | MongoDB Atlas M0 (free tier) |
| Frontend | Vanilla HTML/CSS/JS (single-file template) |
| Hosting | Render (free tier) |

---

## Run Locally

```bash
pip install -r requirements.txt
python app.py
```

Open http://localhost:5000. Defaults to `mongodb://localhost:27017/ship_of_fools`.

## Deploy on Render + MongoDB Atlas

1. Push repo to GitHub
2. Create a free [MongoDB Atlas](https://mongodb.com/atlas) M0 cluster; copy the connection string (include the database name)
3. On [Render](https://render.com): **New Web Service** → connect repo
4. Set environment variables:
   - `MONGO_URI` — Atlas connection string
   - `SECRET_KEY` — long random string (`openssl rand -hex 32`)
5. Build command: `pip install -r requirements.txt`
6. Start command: `gunicorn app:app`

`RENDER_EXTERNAL_URL` is detected automatically to enable the self-ping keep-alive.

---

## Project Structure

```
ship_of_fools/
  app.py              Flask app — routes, Archive.org proxy, scoring, Observatory, map
  templates/
    index.html        Entire frontend (styles + markup + JS, ~3500 lines)
  static/
    stealie.png       Steal Your Face logo
    observatory.svg   Dark Star Observatory tab icon
  CLAUDE.md           AI assistant context file
  Procfile            gunicorn start command
  requirements.txt
```
