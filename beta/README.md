# Ship of Fools — Beta UI

React + Vite + Tailwind. Alt UI mounted at `/beta` on the Flask server.

## Toggle

Classic UI header has a **🧪 Try Beta** button that flips to `/beta`. Beta's
header has a **← Classic** link that returns to `/`. State is stored in
`localStorage.sof_beta` (informational — routes are the source of truth).

## Local dev

Two servers running side-by-side:

```
# terminal 1 — Flask
python app.py                       # :5000

# terminal 2 — Vite (HMR)
cd beta
npm install
npm run dev                         # :5173, proxies /api → :5000
```

Visit `http://localhost:5173` for HMR development. `/api/*` calls proxy to
Flask automatically.

## Build for Render

Render doesn't run Node — build locally and commit the compiled bundle:

```
cd beta
npm run build                       # outputs to ../static/beta/
git add static/beta
git commit -m "beta: rebuild bundle"
```

Flask's `/beta` route reads `static/beta/.vite/manifest.json` to find the
hashed entry files, so every rebuild picks up new hashes automatically.

## Layout

- `src/index.tsx` — entry
- `src/App.tsx` — top-level (props: density, visualizer)
- `src/pages/Browse.tsx` — 3-column layout composition
- `src/components/*` — presentational pieces
- `src/data/*` — placeholder static data (real API wiring is TODO)
- `src/hooks/usePlayer.ts` — mock player state (real audio wiring is TODO)

## Deviations from the Magic Patterns source

Per user feedback:

1. `YearRail` — no era grouping headers, years only
2. `ShowDetail` — no "Listens" meta cell
3. `ShowDetail` — source dropdown (`SourcePicker`) replaces the static
   Soundboard/Audience label. Currently pulls from a placeholder list;
   swap `fakeAlternateSources()` for a real fetch when API is ready.
4. `Browse` — sticky "TODAY'S RECOMMENDED SHOW" banner between the
   `AppHeader` and the section tabs.

## TODO

- Replace `data/shows.ts` and `data/years.ts` with `/api/*` fetches
- Port the audio state machine from classic `templates/index.html`
  (AirPlay-safe, `_pb*` playback context separation, gapless handoff)
- Wire chat / reactions / mentions
