"""
Pre-seed the Observatory scatter cache for all songs.
Run locally — Render's IP is blocked by Archive.org; your machine isn't.

Usage:
    MONGO_URI="mongodb+srv://..." python seed_observatory.py

Or set MONGO_URI in a .env file (python-dotenv will pick it up if installed).

Options:
    --force   Re-fetch all songs even if already cached
    --song    Seed a single song by id, e.g. --song "dark star"
"""
import os, sys, re, time, argparse
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from pymongo import MongoClient

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

MONGO_URI = os.environ.get("MONGO_URI") or input("Paste your MONGO_URI: ").strip()
_mongo    = MongoClient(MONGO_URI)
_db       = _mongo.get_default_database() if "?" in MONGO_URI or MONGO_URI.count("/") >= 3 else _mongo["ship_of_fools"]
_obs_col  = _db["observatory_cache"]

ARCHIVE_SEARCH   = "https://archive.org/advancedsearch.php"
ARCHIVE_METADATA = "https://archive.org/metadata"
COLLECTION       = "GratefulDead"
QUERY_VERSION    = 2   # must match _OBS_QUERY_VERSION in app.py
REFRESH_DAYS     = 14

OBS_SONGS = [
    {"id":"dark star",               "label":"Dark Star"},
    {"id":"the other one",           "label":"The Other One"},
    {"id":"st. stephen",             "label":"St. Stephen"},
    {"id":"china cat sunflower",     "label":"China Cat Sunflower"},
    {"id":"morning dew",             "label":"Morning Dew"},
    {"id":"death dont have no mercy","label":"Death Don't Have No Mercy"},
    {"id":"cryptical envelopment",   "label":"Cryptical Envelopment"},
    {"id":"turn on your love light", "label":"Turn On Your Love Light"},
    {"id":"truckin",                 "label":"Truckin'"},
    {"id":"casey jones",             "label":"Casey Jones"},
    {"id":"friend of the devil",     "label":"Friend of the Devil"},
    {"id":"ripple",                  "label":"Ripple"},
    {"id":"sugar magnolia",          "label":"Sugar Magnolia"},
    {"id":"playing in the band",     "label":"Playing in the Band"},
    {"id":"bertha",                  "label":"Bertha"},
    {"id":"deal",                    "label":"Deal"},
    {"id":"jack straw",              "label":"Jack Straw"},
    {"id":"tennessee jed",           "label":"Tennessee Jed"},
    {"id":"brown eyed women",        "label":"Brown Eyed Women"},
    {"id":"ramble on rose",          "label":"Ramble On Rose"},
    {"id":"mississippi half step",   "label":"Mississippi Half-Step"},
    {"id":"eyes of the world",       "label":"Eyes of the World"},
    {"id":"wharf rat",               "label":"Wharf Rat"},
    {"id":"here comes sunshine",     "label":"Here Comes Sunshine"},
    {"id":"the wheel",               "label":"The Wheel"},
    {"id":"franklins tower",         "label":"Franklin's Tower"},
    {"id":"weather report suite",    "label":"Weather Report Suite"},
    {"id":"i know you rider",        "label":"I Know You Rider"},
    {"id":"estimated prophet",       "label":"Estimated Prophet"},
    {"id":"terrapin station",        "label":"Terrapin Station"},
    {"id":"scarlet begonias",        "label":"Scarlet Begonias"},
    {"id":"fire on the mountain",    "label":"Fire on the Mountain"},
    {"id":"shakedown street",        "label":"Shakedown Street"},
    {"id":"stagger lee",             "label":"Stagger Lee"},
    {"id":"samson and delilah",      "label":"Samson and Delilah"},
    {"id":"touch of grey",           "label":"Touch of Grey"},
    {"id":"hell in a bucket",        "label":"Hell in a Bucket"},
    {"id":"throwing stones",         "label":"Throwing Stones"},
    {"id":"althea",                  "label":"Althea"},
    {"id":"alabama getaway",         "label":"Alabama Getaway"},
    {"id":"dire wolf",               "label":"Dire Wolf"},
    {"id":"ship of fools",           "label":"Ship of Fools"},
    {"id":"lazy lightning",          "label":"Lazy Lightning"},
    {"id":"lost sailor",             "label":"Lost Sailor"},
    {"id":"so many roads",           "label":"So Many Roads"},
    {"id":"drums",                   "label":"Drums"},
    {"id":"space",                   "label":"Space"},
    {"id":"not fade away",           "label":"Not Fade Away"},
    {"id":"going down the road",     "label":"Going Down the Road Feeling Bad"},
    {"id":"me and my uncle",         "label":"Me and My Uncle"},
]

def _parse_length(val):
    try:
        return float(val or 0)
    except (ValueError, TypeError):
        try:
            parts = str(val).split(':')
            if len(parts) == 2:
                return int(parts[0]) * 60 + float(parts[1])
            if len(parts) == 3:
                return int(parts[0]) * 3600 + int(parts[1]) * 60 + float(parts[2])
        except Exception:
            pass
    return 0

def archive_search(params):
    r = requests.get(ARCHIVE_SEARCH, params=params, timeout=20)
    r.raise_for_status()
    return r.json()

def fetch_song(song_meta, force=False):
    song_id = song_meta["id"]
    label   = song_meta["label"]

    # Check existing cache
    if not force:
        row = _obs_col.find_one({"song_id": song_id}, {"fetched_at": 1, "query_version": 1, "performances": 1})
        if row and row.get("performances") and row.get("query_version") == QUERY_VERSION:
            age_days = (time.time() - (row.get("fetched_at") or 0)) / 86400
            if age_days < REFRESH_DAYS:
                print(f"  {label}: already cached ({len(row['performances'])} perfs, {age_days:.1f}d old) — skipping")
                return

    print(f"  {label}: searching Archive.org...", flush=True)
    try:
        data = archive_search({
            "q": f'collection:{COLLECTION} AND (files.title:"{label}" OR title:"{label}" OR description:"{label}")',
            "fl[]": "identifier,date,avg_rating,num_reviews,source",
            "output": "json", "rows": 2000, "sort[]": "date asc",
        })
    except Exception as e:
        print(f"  {label}: search FAILED — {e}", file=sys.stderr)
        return

    docs = data.get("response", {}).get("docs", [])

    # One candidate per date, cap at 200
    seen_dates, candidates = {}, []
    for doc in docs:
        date_str = doc.get("date") or ""
        if isinstance(date_str, list): date_str = date_str[0] if date_str else ""
        date_str = date_str[:10]
        if not date_str or date_str in seen_dates: continue
        seen_dates[date_str] = True
        candidates.append(doc)
    candidates = candidates[:200]

    print(f"  {label}: {len(candidates)} candidate recordings — fetching metadata...", flush=True)

    pattern = re.compile(re.escape(label), re.IGNORECASE)

    def fetch_perf(doc):
        identifier = doc.get("identifier") or ""
        date_str   = doc.get("date") or ""
        if isinstance(date_str, list): date_str = date_str[0] if date_str else ""
        date_str = date_str[:10]
        if not date_str or not identifier: return None
        try:
            meta = requests.get(f"{ARCHIVE_METADATA}/{identifier}", timeout=8)
            meta.raise_for_status()
            files = meta.json().get("files", [])
        except Exception:
            return None
        matched_dur = None
        for f in files:
            fname = f.get("name", "")
            if not fname.lower().endswith((".mp3", ".flac", ".ogg")): continue
            title = f.get("title", "") or fname
            if pattern.search(title):
                dur = _parse_length(f.get("length"))
                if dur > 60:
                    matched_dur = dur
                    break
        if matched_dur is None: return None
        ident_lower = identifier.lower()
        if "sbd" in ident_lower or "soundboard" in ident_lower:    src = "SBD"
        elif "mtx" in ident_lower or "matrix" in ident_lower:      src = "MTX"
        elif "fob" in ident_lower:                                  src = "FOB"
        elif "aud" in ident_lower or "audience" in ident_lower:    src = "AUD"
        else: src = doc.get("source", "UNK") or "UNK"
        try:
            reviews = int(doc.get("num_reviews") or 0)
            rating  = float(doc.get("avg_rating") or 0)
        except (ValueError, TypeError):
            reviews, rating = 0, 0.0
        return {
            "date": date_str, "duration": round(matched_dur),
            "source": src, "reviews": reviews,
            "rating": round(rating, 1), "id": identifier,
        }

    performances = []
    with ThreadPoolExecutor(max_workers=12) as pool:
        futures = {pool.submit(fetch_perf, doc): doc for doc in candidates}
        for future in as_completed(futures):
            r = future.result()
            if r: performances.append(r)

    performances.sort(key=lambda x: x["date"])

    doc = {
        "song":         label,
        "song_id":      song_id,
        "songs":        OBS_SONGS,
        "performances": performances,
        "fetched_at":   time.time(),
        "query_version": QUERY_VERSION,
        "ts":           datetime.now(timezone.utc),
    }
    _obs_col.update_one({"song_id": song_id}, {"$set": doc}, upsert=True)
    print(f"  {label}: {len(performances)} performances cached ✓")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="Re-fetch even if already cached")
    parser.add_argument("--song", help="Seed a single song by id (e.g. 'dark star')")
    args = parser.parse_args()

    songs = OBS_SONGS
    if args.song:
        songs = [s for s in OBS_SONGS if s["id"] == args.song.lower().strip()]
        if not songs:
            print(f"Unknown song id '{args.song}'. Valid ids:", file=sys.stderr)
            for s in OBS_SONGS: print(f"  {s['id']}", file=sys.stderr)
            sys.exit(1)

    print(f"Seeding Observatory scatter cache for {len(songs)} song(s)...")
    for i, song in enumerate(songs, 1):
        print(f"\n[{i}/{len(songs)}]")
        fetch_song(song, force=args.force)
        if i < len(songs):
            time.sleep(2)  # be polite to Archive.org

    print("\nDone. Observatory cache is fully seeded.")
