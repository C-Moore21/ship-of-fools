# Ship of Fools

A Grateful Dead live concert browser built with Python/Flask, sourcing recordings directly from [Archive.org](https://archive.org/details/GratefulDead). Deployed at [ship-of-fools.onrender.com](https://ship-of-fools.onrender.com).

## Features

### Browsing & Discovery
- Browse every Grateful Dead year (1965–1995)
- Shows grouped by date with venue & location
- **Search** — find shows by date, venue, or location
- **Today in History (TIH)** — all recordings from today's date across every year, ranked by composite score, with a hero card and one-tap play
- **On This Tour** — shows within 30 days of the selected show, same year
- **Venue History** — other shows played at the same city/location

### Recordings & Scoring
- All recordings per show listed with type badges — **SBD**, **MTX**, **FOB**, **AUD** — sorted by a weighted composite quality score
- Composite score: Bayesian confidence-adjusted rating × source type multiplier (SBD 1.0 · MTX 0.9 · FOB 0.8 · AUD 0.7)
- Taper, transferer, and lineage info per recording
- Full setlist with track durations

### Playback
- Audio player with play/pause, prev/next, seek slider, and gapless preload (desktop/WiFi)
- **Automatic source failover** — retries 3× on error then switches to next best recording
- **Lock screen / AirPlay controls** via Media Session API (iOS Safari + Apple TV supported)
- **Android Cast / Chrome Remote Playback** via Remote Playback API
- Gapless preload disabled on cellular connections to save data

### Social & Personal
- User accounts (register/login) with session-based auth
- **Show ratings** — rate entire shows 1–5 stars
- **Track ratings** — rate individual tracks 1–5 stars per recording
- **Notes** — save personal notes per show
- **My Rated Shows & Tracks** view — collapsible by show
- **Listening History** — your 500 most recent plays
- **Stats** — total listening time, top shows, top tracks, recent plays
- **Leaderboard** — listening time across all users

### Navigation
- **Deep linking** — shareable URLs per show and recording (`?show=1977-05-08&src=gd1977-05-08.sbd...`)
- **Mobile-friendly** — drill-down panel navigation, stacked player bar, collapsible source list after selection

## Stack

- **Backend**: Python 3 + Flask + Gunicorn
- **Data**: [Archive.org](https://archive.org) public search & metadata APIs — no API key required
- **Database**: MongoDB Atlas (free M0 tier)
- **Frontend**: Vanilla HTML/CSS/JS (single-file template)
- **Hosting**: Render (free tier, with self-ping keep-alive)

## Run locally

```bash
pip install -r requirements.txt
python app.py
```

Then open http://localhost:5000

Uses a local MongoDB instance at `mongodb://localhost:27017/ship_of_fools` by default.

## Deploy (Render + MongoDB Atlas)

1. Push repo to GitHub
2. Create a free [MongoDB Atlas](https://mongodb.com/atlas) M0 cluster, get your connection string
3. On [Render](https://render.com): New Web Service → connect repo
4. Set environment variables:
   - `MONGO_URI` — your Atlas connection string (with database name in the URL)
   - `SECRET_KEY` — any long random string
5. Build command: `pip install -r requirements.txt`
6. Start command: `gunicorn app:app`

Render's `RENDER_EXTERNAL_URL` is detected automatically to enable the self-ping keep-alive thread.

## Project structure

```
ship_of_fools/
  app.py              Flask app — routes, Archive.org proxy, ratings API, scoring
  templates/
    index.html        Entire frontend (styles + markup + JS)
  static/
    stealie.png       Steal Your Face logo
  Procfile            Render start command
  requirements.txt
```
