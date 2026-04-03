# Ship of Fools

A Grateful Dead live concert browser built with Python/Flask, sourcing recordings directly from [Archive.org](https://archive.org/details/GratefulDead).

## Features

- Browse every Grateful Dead year (1965–1995)
- Shows grouped by date with venue & location
- All recordings per show listed with type badges — **SBD**, **MTX**, **FOB**, **AUD** — sorted by quality, with Archive.org community ratings shown per source
- Taper, transferer, and lineage info per recording
- Full setlist with track durations
- Audio player with play/pause, prev/next, draggable seek slider, and near-gapless playback (double-buffer preload)
- **Show ratings** — rate entire shows 1–5 stars, shown as badges in the sidebar
- **Track ratings** — rate individual tracks 1–5 stars per recording
- **My Rated Shows & Tracks** view — shows grouped as collapsible dropdowns with your show rating on the header and rated tracks listed inside
- User accounts (register/login) with session-based auth

## Stack

- **Backend**: Python 3 + Flask
- **Data**: [Archive.org](https://archive.org) public search & metadata APIs — no API key required
- **Database**: TinyDB (local JSON file) for users, show ratings, and track ratings
- **Frontend**: Vanilla HTML/CSS/JS (single-file template)

## Run

```bash
pip install -r requirements.txt
python app.py
```

Then open http://localhost:5000

## Project structure

```
ship_of_fools/
  app.py              Flask app — routes, Archive.org proxy, ratings API
  templates/
    index.html        Entire frontend (styles + markup + JS)
  static/
    stealie.png       Steal Your Face logo
  db.json             TinyDB data file (auto-created on first run)
  requirements.txt
```
