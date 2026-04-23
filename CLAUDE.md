# Ship of Fools — Claude Context

Grateful Dead live concert browser. Flask + MongoDB Atlas M0 + Render free tier. Single-file frontend at `templates/index.html`.

## Key constraints
- MongoDB Atlas M0: 512MB, max 3 indexes per collection (incl `_id`)
- Render free tier: 30s Gunicorn worker timeout — never make synchronous Archive.org calls on request threads for slow endpoints (use MongoDB cache + background threads)
- Archive.org public API, no key: `https://archive.org/advancedsearch.php` + `https://archive.org/metadata/{id}`
- Collection constant: `COLLECTION = "GratefulDead"`

## Caching pattern
Three-tier: LRU in-memory (`_LRUCache`, 500 entries, 5min TTL) → MongoDB → live fetch.
Background daemon threads warm caches at startup (90s delay for observatory, 15s for map).
Bump `_OBS_QUERY_VERSION` in `app.py` to force re-scrape of Observatory data.

## MongoDB collections
- `users`, `listens`, `ratings`, `show_ratings`, `notes`, `sessions`
- `observatory_cache` — song scatter + heatmap data (indexed on `song_id` unique)
- `shows_map_cache` — geocoded show list (single doc `_id="shows_map"`)

## Observatory (`/api/observatory`, `/api/observatory/heatmap`)
- `_OBS_SONGS`: ~50 songs with `{id, label}` — id is lowercase search key
- Heatmap: search-only, year-bucket avg ratings, all songs, fast
- Scatter: per-recording metadata fetch via `ThreadPoolExecutor(max_workers=8)`, improv-heavy songs only
- Frontend: `renderHeatmap()` → default view; click song name → `renderObservatory(songId)` scatter drill-down

## Crow's Nest map (`/api/shows/map`, `/api/map/us-states`)
- Coverage string parsing: `_coords_for_coverage()` tries every comma segment (not just first — venue name comes first)
- State outlines: GeoJSON fetched from PublicaMundi via Flask proxy, rendered as canvas paths
- Scrubber: indexes over all show dates array; mode=year/month/day changes granularity

## Frontend structure
- Single file `templates/index.html` ~3500 lines: CSS → HTML → JS
- Three-column layout: years | shows | track panel
- Mobile: stacked panels, `mob-active` class, back buttons, 22px stars
- Observatory modal: inner tabs `obs-tab-heatmap/crows-nest/mystats`
- `tourRunGoTo(date)` — navigates main UI to a show by date

## Style conventions
- CSS vars: `--bg`, `--surface`, `--surface2`, `--border`, `--text`, `--muted`, `--accent` (#e8332a red), `--accent2` (#1a4bbf blue), `--white`
- Fonts: Playfair Display (headings), Space Mono (body/mono)
- No framework — vanilla JS, no npm, no build step
- `escapeHTML(str)` always used for user/external content in innerHTML
