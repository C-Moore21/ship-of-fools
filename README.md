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
- **Stats page** — total listening time, top shows, top songs (GD alias-normalized, ranked by number of shows), year filter
- **Leaderboard** — weekly, monthly, all-time with display name support

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
- **Single-pass listen aggregation** — `listen_stats` builds top shows and top songs in one MongoDB query with in-process grouping; `all_years` fetched separately (unfiltered) so the year picker never collapses
- **DOM caching** — player time/duration elements cached; playback row targeting by ID (no `querySelectorAll` sweep)
- **Throttled timeupdate** — 250 ms throttle on audio time handler to reduce reflow overhead

### Reliability
- **Optimistic UI** — ratings and notes update instantly; revert on API error
- **Try/catch on all fetches** — all user-facing API calls wrapped with state revert fallback
- `_sourceLoading` flag prevents concurrent `switchSource()` calls during slow metadata fetches
- **Safe index creation** — `session_id` unique index wrapped in try/except; logs a warning and falls back to non-unique if pre-existing duplicates are detected
- **Specific archive.org error handling** — `Timeout` → 502, `RequestException` → 502, other → 500; no raw exception text exposed to clients

### Gapless Playback & Failover
- Dual audio elements (`audio`, `preload`) swap at `playTrack()` if preload has buffered (readyState ≥ 3)
- At 75% playback, `preloadTrack(N+2)` is queued; if N+1 isn't ready, `_preloadFallbackSource` prefetches the next best recording
- Retry loop (3 attempts, 500 ms × attempt backoff) on preload error; on final failure, injects buffered fallback source without audio interruption

### Grateful Dead Song Aliases
Stats page normalizes track titles across recordings using a hardcoded alias map (e.g. `gdtrfb` → "Going Down the Road Feeling Bad", `nfa` → "Not Fade Away"). Normalization strips leading track numbers, transition arrows, apostrophes, ampersands, and punctuation before alias lookup.

### Admin
- `POST /api/admin/rename-user` — renames a user across all collections (users, listens, ratings, show_ratings, notes) atomically

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
  app.py              Flask app — routes, Archive.org proxy, scoring, stats, leaderboard
  templates/
    index.html        Entire frontend (styles + markup + JS)
  static/
    stealie.png       Steal Your Face logo
  Procfile            gunicorn start command
  requirements.txt
```
