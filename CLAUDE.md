# Ship of Fools — Claude Context

Private Grateful Dead live concert browser for Camden + 3 friends (jmac, caleb, zack). Flask + MongoDB Atlas M0 + Render free tier.

**Two UIs coexist:**
- **Classic** — vanilla-JS single-file frontend at `templates/index.html` (~6000 lines). The historic UI. No build step.
- **Beta** — React + TypeScript + Tailwind app at `beta/`. Built with Vite, output committed to `static/beta/`. Served by Flask at `/beta`. Toggle from either UI: classic header has `🧪 Try Beta`; beta header has `← Classic`. Both share the same Flask backend and MongoDB.

## Key constraints
- MongoDB Atlas M0: 512MB, max 3 indexes per collection (incl `_id`)
- Render free tier: 30s Gunicorn worker timeout — never make synchronous Archive.org calls on request threads for slow endpoints (use MongoDB cache + background threads)
- Archive.org public API, no key: `https://archive.org/advancedsearch.php` + `https://archive.org/metadata/{id}`
- Collection constant: `COLLECTION = "GratefulDead"`
- Render doesn't run Node — beta bundle must be pre-built locally and committed to `static/beta/`
- Lounge membership allowlist: `camden, jmac, caleb, zack` (case-insensitive). Not `zac`.
- Lounge reaction allowlist: `👍 ❤️ 👎 😭` — server-enforced

## Caching pattern
Three-tier: LRU in-memory (`_LRUCache`, 500 entries, 5min TTL) → MongoDB → live fetch.
Bump `_OBS_QUERY_VERSION` in `app.py` to force re-scrape of Observatory data.
gzip middleware (`@app.after_request`) compresses JSON/text ≥1KB.

## Cache seeding (Archive.org blocks Render's IP — must seed from local)
Run these scripts from a developer machine; they write directly to MongoDB Atlas:
- `seed_cache.py` — `shows_year_cache` (31 years) + `today_cache` (365 days)
- `seed_observatory.py` — `observatory_cache` (heatmap + scatter for 50 songs)
- `seed_setlists.py` — `setlist_cache` (every show's tracklist with position)
- `seed_releases.py` — `releases_cache` (deaddisc.com official releases)

Re-run monthly to pick up new Archive.org uploads. Render serves only from MongoDB — no background Archive.org workers on Render (they always failed). Per-request fallback paths still exist for cache misses; they'll just 502 on Render.

**Cache version keys** — bump the suffix when the response shape changes so old entries invalidate (e.g. `shows-v2:` — see `shows_year_cache` handling).

## MongoDB collections
- `users`, `listens`, `ratings` (per-track), `show_ratings` (per-show), `notes`, `sessions`
- `chat_messages` — Lounge; docs have `bumped_at` for reaction/reply polling
- `observatory_cache` — song scatter + heatmap data (indexed on `song_id` unique)
- `shows_map_cache` — geocoded show list (single doc `_id="shows_map"`)
- `setlist_cache` — `{song, date, position, source_id}` for gap/debut/drought/rarity computation
- `releases_cache` — `_id=show_date`, list of official releases (Dick's Picks, Dave's Picks, etc.)
- `_segue_col` — normalized song-pair segue tracking

## Key API endpoints
- `/api/years`, `/api/years/<year>/shows` — enriched with community_listens/avg/count + Archive.org avg_rating/num_reviews (best-per-date, weighted by log reviews)
- `/api/shows/<date>` — one-shot detail (venue + weather + community stats + sources)
- `/api/shows/<date>/sources` — per-source list (source_type, archive_rating, archive_reviews)
- `/api/sources/<id>/tracks` — sets + tracks + taper/transferer/lineage
- `/api/shows/<date>/setlist-stats` (POST) — per-song gap/is_debut/drought_rank/total plays; keyed by normalized song title, `raw` field maps back
- `/api/shows/<date>/weather`
- `/api/show-ratings/community?show_id=` — {avg, count} community ratings
- `/api/today` — today-in-history list, scored by rating × log(reviews) × source-type
- `/api/search?q=` — returns `{shows, songs, venues}`. `?legacy=1` for bare Show[]
- `/api/releases/all`, `/api/tours`, `/api/tours/<id>/progress`
- `/api/listens/history`, `/api/listens/stats`
- `/api/chat/lounge/*` — access, messages, send, react, mark-read, unread
- `/api/auth/me`, `/login`, `/register`, `/logout`

## Observatory (`/api/observatory`, `/api/observatory/heatmap`)
- `_OBS_SONGS`: ~50 songs with `{id, label}` — id is lowercase search key
- Heatmap: search-only, year-bucket avg ratings, all songs, fast
- Scatter: per-recording metadata fetch via `ThreadPoolExecutor(max_workers=8)`, improv-heavy songs only
- Classic frontend: `renderHeatmap()` → default; click song → `renderObservatory(songId)` scatter drill-down
- Beta: `beta/src/observatory/` — Heatmap + Scatter + CrowsNest (canvas map) + ObservatoryModal with tabs

## Crow's Nest map (`/api/shows/map`, `/api/map/us-states`)
- Coverage string parsing: `_coords_for_coverage()` tries every comma segment (venue name comes first)
- State outlines: GeoJSON fetched from PublicaMundi via Flask proxy, rendered as canvas paths
- Scrubber: indexes over all show dates array; mode=year/month/day changes granularity

## Classic frontend (`templates/index.html`)
- Single file ~6000 lines: CSS → HTML → JS
- Three-column layout: years | shows | track panel
- Mobile: stacked panels, `mob-active` class, back buttons, 22px stars
- Observatory modal: inner tabs `obs-tab-heatmap/crows-nest/mystats`
- `tourRunGoTo(date)` — navigates main UI to a show by date
- Audio state machine: `_pbTracks`, `_pbTrackIdx`, `_pbShowDate`, `_pbSourceId` (playback context — separate from view context so browsing doesn't hijack playback), `playTrack`, `onEnded`, `_finalizeCrossfade`, `_userVol()` helper (never hardcode audio.volume = 1), `snapToPlayingShow`, `tripMaybeAdvancePlayingShow`
- AirPlay-safe: never `.load()` when casting, MediaSession metadata before `play()`, single AVPlayer only, gapless handoff via preload element swap
- Trip Mode: auto-advance across tour-run shows, cap 3 shows, re-resolves for playing show if user browsed away
- Lounge: `_LOUNGE_MEMBERS`, `_LOUNGE_REACTIONS`, `_fmtLoungeTs`, `_loungeRenderText`, `loungePollNew` (4s open / 30s unread), drag-to-resize with `localStorage.sof_lounge_size`, mentions autocomplete, reply-with-flash

## Beta frontend (`beta/`)
Vite + React 18 + TS + Tailwind. Built to `static/beta/`. Flask reads `.vite/manifest.json` to find hashed entry files.

**Local dev:**
```
python app.py                    # :5000
cd beta && npm install && npm run dev  # :5173, proxies /api → :5000
```
Vite scripts must call `vite` directly (not `npx vite`) — PowerShell non-interactive shells don't have npx on PATH. Full path to npm on Windows: `C:\Program Files\nodejs\npm.cmd`. PowerShell may need `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`.

**Build for Render (no Node on Render):**
```
cd beta && npm run build         # outputs to ../static/beta/
git add beta/ static/beta/       # commit source + bundle
```

**Directory layout:**
- `beta/src/pages/Browse.tsx` — top-level composition (3-column layout, tabs, modals mounted here)
- `beta/src/components/` — AppHeader, YearRail, ShowList, ShowDetail, Setlist, PlayerBar (unused — see audio/), Visualizer, Badges, Stealie
- `beta/src/audio/` — engine.ts (2× `<audio>`, MediaSession, AirPlay-safe, gapless, Trip Mode), useSofAudio (structural), useSofAudioTime (time-only — only PlayerBar subscribes), PlayerBar, ResumePrompt, TripMode
- `beta/src/lounge/` — full-parity chat (reactions, replies, mentions, resize, timestamps, links + gd-date jumps)
- `beta/src/auth-and-social/` — useAuth, LoginModal, RatingStars (half-star), NoteEditor, useShowRating, useShowNote, useTrackRatings
- `beta/src/observatory/` — Heatmap, Scatter, CrowsNest (canvas), ObservatoryModal
- `beta/src/search/` — SearchPalette (⌘K / `/`)
- `beta/src/modals/` — TodayInHistoryModal, BlindTestModal, TripBanner
- `beta/src/sections/` — RatedPanel, StatsPanel, HistoryPanel, LeaderboardPanel (tabs in Browse)
- `beta/src/tours/` — ToursModal (⟳ launcher)
- `beta/src/api/` — client (typed fetch wrappers) + adapters (raw API → UI types)
- `beta/src/hooks/useSofData.ts` — data hooks + module-level Promise caches + `prefetchShow`/`prefetchShowsForYear` (hover prefetches)
- `beta/src/types/archive.ts` — Show/Track/YearEntry types

**Beta patterns:**
- Every heavy component is `React.memo`'d (YearRail, ShowList, ShowDetail, Setlist, TrackRow, AppHeader, RatingStars)
- All callbacks Browse passes to children are `useCallback`'d
- Time-only subscription (`useSofAudioTime`) prevents 4Hz timeupdate re-renders on non-PlayerBar components
- Modals + section panels are `React.lazy`; first-paint bundle stays small
- Hover prefetch on year rows + show rows
- Module-level Promise caches dedupe requests; `useShowRating` / `useShowNote` do write-back on save

**Adapter conventions:**
- `adaptShallowShow` — Archive.org public rating takes priority; community (4-friend) rating is fallback. Local friend rating still lives in ShowDetail's `RatingStars` widget.
- `normalizeSetName` — matches "Set 1"/"Set I"/"Encore" as whole words. Falls back to disc order when album tag is just a show title (was matching `e\b` in "live" before, buggy).
- `applySetlistStats` — merges `/api/shows/<date>/setlist-stats` response onto tracks (rarity from total plays, gap label from is_debut / drought_rank / gap_before)
- `splitByBreakMarkers` — heuristic set-splitter for single-album shows: short (<60s) "Tuning"/"Crowd" tracks that aren't first are set-break markers
- `shortenIdentifier` — source label parser prefers alphabetic taper tokens over numeric versions

**Resume-on-load** — engine writes `localStorage.sof_resume` (v:1 schema, 500ms debounce) with `{show, source, trackCount, trackIdx, elapsed}`. `ResumePrompt` mounts floating bottom-left; not auto-resumed.

## Style conventions
- CSS vars (classic): `--bg`, `--surface`, `--surface2`, `--border`, `--text`, `--muted`, `--accent` (#e8332a red), `--accent2` (#1a4bbf blue), `--white`
- Tailwind palette (beta): mirrors classic — `bg`, `surface`, `surface2`, `line`, `ink`, `muted`, `chalk`, `accent` (#e8332a), `royal` (#1a4bbf), `royal-bright`, `gold` (#8f7a3d — dimmed at user request), `moss`, `violet`, `amber`
- Fonts: Playfair Display (headings), Space Mono (body/mono) — loaded via preconnect + swap in `beta/index.html` and `templates/beta.html`
- Classic: `escapeHTML(str)` always used for user/external content in innerHTML
- Beta: no innerHTML — token-based text rendering in lounge (utils.ts `tokenizeText`), auto-linkifies URLs + gd-date/ISO refs

## Perf notes (beta)
- First-paint bundle ~30KB gzip (index) + 43KB (react-vendor) + 5KB (lucide). Lazy chunks defer Observatory (~8KB gz), Search (~3KB gz), section panels (~1.5KB gz each), modals.
- Backend gzip middleware active — 4–8× smaller JSON on the wire.
- Show list hover triggers `prefetchShow(date)` (sources + weather); year hover triggers `prefetchShowsForYear`.
- Set-list rows are memoized as `TrackRow`; only 2 rows re-render on trackchange (not 25+).
- No `backdrop-blur` anywhere — it's compositor-thrash on scroll.

## What NOT to do
- Don't fan out sub-agents unless the user explicitly asks — they cost the most tokens (each spins up a fresh context reading `app.py` + `templates/index.html` + several beta files).
- Don't add `.load()` on the audio element when casting/AirPlay is active.
- Don't hardcode `audio.volume = 1` — always route through `_userVol()` (classic) or engine.setVolume (beta).
- Don't add MongoDB indexes beyond 3 per collection.
- Don't remove `Vary: Accept-Encoding` from gzip middleware.
- Don't split beta into per-page bundles beyond what's already lazy — first-paint bundle is small enough.
- Don't ship Archive.org synchronous fetches on new endpoints — cache in MongoDB via a seed script first.
