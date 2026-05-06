"""
Seed the house_music_cache MongoDB collection with a curated pool of
live jazz tracks from Archive.org's etree collection. Run locally —
Render's IP is blocked by Archive.org so this can't run there.

Usage:
    MONGO_URI="mongodb+srv://..." python seed_house_music.py

Options:
    --workers N     Parallel metadata fetches (default 8)
    --tracks N      How many full tracks to extract (default 200)

Each pool entry includes a fully-resolved download URL so the
/api/house-music endpoint on Render can serve directly from MongoDB
without any Archive.org calls at request time.
"""
import os, sys, time, argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import requests
from pymongo import MongoClient

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

MONGO_URI = os.environ.get("MONGO_URI") or input("Paste your MONGO_URI: ").strip()
_mongo = MongoClient(MONGO_URI)
_db    = _mongo.get_default_database() if "?" in MONGO_URI or MONGO_URI.count("/") >= 3 else _mongo["ship_of_fools"]
_col   = _db["house_music_cache"]

ARCHIVE_SEARCH   = "https://archive.org/advancedsearch.php"
ARCHIVE_METADATA = "https://archive.org/metadata"
ARCHIVE_DOWNLOAD = "https://archive.org/download"

JAZZ_ARTISTS = [
    "John Coltrane", "Miles Davis", "Pharoah Sanders",
    "Alice Coltrane", "Sun Ra", "Albert Ayler",
    "Charles Mingus", "Ornette Coleman", "Eric Dolphy",
    "McCoy Tyner", "Yusef Lateef", "Don Cherry",
    "Bill Evans", "Thelonious Monk",
]

def parse_length(val):
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

def search_jazz(rows=400):
    creator_clause = " OR ".join(f'"{a}"' for a in JAZZ_ARTISTS)
    params = {
        "q": f'collection:etree AND mediatype:audio AND ({creator_clause})',
        "fl[]": "identifier,title,creator,date,downloads",
        "output": "json",
        "rows": rows,
        "sort[]": "downloads desc",
    }
    r = requests.get(ARCHIVE_SEARCH, params=params, timeout=30)
    r.raise_for_status()
    return r.json().get("response", {}).get("docs", [])

def extract_track(doc):
    """Pick one mid-set mp3 from this item and return a fully-resolved entry."""
    ident = doc.get("identifier")
    if not ident: return None
    creator = doc.get("creator")
    if isinstance(creator, list): creator = creator[0] if creator else ""
    title = doc.get("title") or ident
    if isinstance(title, list): title = title[0] if title else ident
    date = (doc.get("date") or "")[:10] if isinstance(doc.get("date"), str) else ""
    try:
        r = requests.get(f"{ARCHIVE_METADATA}/{ident}", timeout=15)
        r.raise_for_status()
        meta = r.json()
    except Exception:
        return None
    files = meta.get("files", [])
    mp3s = [f for f in files
            if f.get("format") in ("VBR MP3", "MP3", "128Kbps MP3", "64Kbps MP3")
            and f.get("name")]
    if not mp3s:
        return None
    # Pick mid-set track (avoid intros/outros). Filter out very short tracks (<60s).
    playable = [f for f in mp3s if parse_length(f.get("length")) >= 60]
    if not playable:
        playable = mp3s
    track = playable[len(playable) // 2] if len(playable) > 2 else playable[0]
    fname = track["name"]
    url = f"{ARCHIVE_DOWNLOAD}/{ident}/{requests.utils.quote(fname)}"
    return {
        "url":        url,
        "title":      track.get("title") or fname,
        "artist":     creator or "Unknown",
        "show_title": title,
        "date":       date,
        "duration":   round(parse_length(track.get("length"))),
        "identifier": ident,
    }

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--tracks", type=int, default=200, help="Target pool size")
    args = parser.parse_args()

    print(f"Searching Archive.org etree collection for jazz live recordings...")
    docs = search_jazz(rows=args.tracks * 2)  # over-fetch since some will fail
    print(f"  Found {len(docs)} candidate items.")

    if not docs:
        print("Search returned nothing. Check your network connection.", file=sys.stderr)
        return

    print(f"\nFetching metadata for each (target {args.tracks} usable tracks)...")
    pool = []
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(extract_track, d): d for d in docs}
        for i, future in enumerate(as_completed(futures), 1):
            try:
                t = future.result()
            except Exception:
                t = None
            if t:
                pool.append(t)
                if i % 20 == 0:
                    print(f"  [{i}/{len(docs)}] {len(pool)} usable tracks so far")
            if len(pool) >= args.tracks:
                break

    print(f"\nResolved {len(pool)} playable tracks.")
    if not pool:
        print("No usable tracks. Aborting.", file=sys.stderr)
        return

    # Wipe and rewrite as a single doc with the entire pool
    print("Writing pool to MongoDB...")
    _col.delete_many({})
    _col.update_one(
        {"_id": "pool"},
        {"$set": {"data": pool, "ts": datetime.now(timezone.utc)}},
        upsert=True,
    )
    print(f"Done. {len(pool)} tracks seeded into house_music_cache.")
    print("\nSample artists:")
    artists = {}
    for t in pool:
        artists[t["artist"]] = artists.get(t["artist"], 0) + 1
    for a, c in sorted(artists.items(), key=lambda x: -x[1])[:10]:
        print(f"  {c:3d}× {a}")

if __name__ == "__main__":
    main()
