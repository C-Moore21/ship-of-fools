"""
Backfill setlist_cache for every Grateful Dead show by scraping Archive.org.
Run locally — Render's IP is blocked by Archive.org; your machine isn't.

Usage:
    MONGO_URI="mongodb+srv://..." python seed_setlists.py

Options:
    --year YYYY    Seed only one year (e.g. --year 1977)
    --force        Re-seed even shows that already have rows
    --workers N    Parallel metadata fetches (default 12)

For each show: picks the best canonical source (highest-rated SBD with most
reviews, falls back to first source), fetches its track list, and writes one
row per track to setlist_cache with {song, date, position, source_id}.

Songs can repeat within a show (Drums in both sets, reprises) so the index
on (song, date) is non-unique.

Filters out non-musical tracks: tuning, crowd, intros, anything < 30s.
"""
import os, sys, re, time, argparse
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
_setlist_col   = _db["setlist_cache"]
_shows_year_col = _db["shows_year_cache"]

ARCHIVE_SEARCH   = "https://archive.org/advancedsearch.php"
ARCHIVE_METADATA = "https://archive.org/metadata"
COLLECTION       = "GratefulDead"

# Track titles that aren't songs — filter these out
NON_MUSICAL = re.compile(
    r'^(tun(e|ing)|crowd|audience|intro|outro|banter|talk|chat|'
    r'sound\s*check|silence|gap|//|--|---|applause|stage|'
    r'announcement|tape\s*flip|reel\s*change)\b',
    re.IGNORECASE
)

_ALIASES = {
    "gdtrfb": "going down the road feeling bad",
    "going down the road feelin bad": "going down the road feeling bad",
    "goin down the road feeling bad": "going down the road feeling bad",
    "goin down the road feelin bad":  "going down the road feeling bad",
    "nfa": "not fade away", "pitb": "playing in the band",
    "tleo": "the other one", "st stephen": "saint stephen",
    "lovelight": "turn on your lovelight",
    "turn on your love light": "turn on your lovelight",
    "sugar mag": "sugar magnolia", "truckin": "truckin",
    "me  my uncle": "me and my uncle", "iko": "iko iko",
    "china cat": "china cat sunflower", "cc rider": "cc rider",
}

def norm_song(title):
    t = (title or "").lower().strip()
    t = re.sub(r'^[\d\s\.\-]+', '', t)
    t = re.sub(r'\s*[-=>]+\s*.*$', '', t)
    t = re.sub(r"[''`&]", '', t)
    t = re.sub(r'[^\w\s]', '', t)
    t = re.sub(r'\s+', ' ', t).strip()
    return _ALIASES.get(t, t)

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

def archive_search(params):
    r = requests.get(ARCHIVE_SEARCH, params=params, timeout=20)
    r.raise_for_status()
    return r.json()

def archive_metadata(identifier):
    r = requests.get(f"{ARCHIVE_METADATA}/{identifier}", timeout=15)
    r.raise_for_status()
    return r.json()

def pick_best_source(date):
    """Return the best source identifier for a show date — prefer SBD with most reviews."""
    try:
        data = archive_search({
            "q": f'collection:{COLLECTION} AND date:{date}',
            "fl[]": "identifier,avg_rating,num_reviews",
            "output": "json", "rows": 50,
            "sort[]": "num_reviews desc",
        })
    except Exception as e:
        return None
    docs = data.get("response", {}).get("docs", [])
    if not docs:
        return None

    def score(d):
        ident = (d.get("identifier") or "").lower()
        is_sbd = "sbd" in ident or "soundboard" in ident
        is_mtx = "mtx" in ident or "matrix" in ident
        type_pref = 3 if is_sbd else 2 if is_mtx else 1
        reviews = int(d.get("num_reviews") or 0)
        return (type_pref, reviews)

    docs.sort(key=score, reverse=True)
    return docs[0].get("identifier")

def fetch_setlist(date, force=False):
    """Fetch the setlist for one show. Returns list of rows or None."""
    if not force:
        existing = _setlist_col.count_documents({"date": date}, limit=1)
        if existing:
            return "skipped"
    src_id = pick_best_source(date)
    if not src_id:
        return None
    try:
        meta = archive_metadata(src_id)
    except Exception:
        return None
    files = meta.get("files", [])
    mp3s = [f for f in files if f.get("format") in ("VBR MP3", "MP3", "128Kbps MP3", "64Kbps MP3")]
    # Sort by track number
    def tnum(f):
        try: return int(f.get("track") or 0)
        except (ValueError, TypeError): return 0
    mp3s.sort(key=tnum)

    rows = []
    pos = 0
    for f in mp3s:
        title = (f.get("title") or "").strip()
        if not title:
            continue
        if NON_MUSICAL.match(title):
            continue
        dur = parse_length(f.get("length"))
        if dur > 0 and dur < 30:  # skip tuning/intro fragments
            continue
        clean = title.rstrip('>').strip()
        n = norm_song(clean)
        if not n or len(n) <= 2:
            continue
        rows.append({
            "song": n,
            "date": date,
            "position": pos,
            "source_id": src_id,
            "duration": round(dur),
        })
        pos += 1
    return rows

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, help="Only seed this year")
    parser.add_argument("--force", action="store_true", help="Re-seed shows that already have rows")
    parser.add_argument("--missing", action="store_true", help="Only retry dates with zero setlist rows (fast)")
    parser.add_argument("--workers", type=int, default=12, help="Parallel workers")
    args = parser.parse_args()

    # Wipe + recreate index (drop unique constraint if it exists)
    print("Migrating setlist_cache schema...")
    try:
        _setlist_col.drop_index("song_1_date_1")
        print("  Dropped old unique index.")
    except Exception:
        pass
    _setlist_col.create_index([("song", 1), ("date", 1)])
    _setlist_col.create_index("date")
    print("  Created non-unique compound index.")

    if args.force:
        print("  --force: wiping all existing rows.")
        _setlist_col.delete_many({})

    # Build list of show dates from shows_year_cache
    print("\nLoading show dates from shows_year_cache...")
    if args.year:
        years = [args.year]
    else:
        years = list(range(1965, 1996))
    all_dates = []
    for y in years:
        doc = _shows_year_col.find_one({"_id": str(y)})
        if not doc or not doc.get("data"):
            print(f"  {y}: no shows cached — run seed_cache.py first", file=sys.stderr)
            continue
        for s in doc["data"]:
            d = s.get("display_date") or s.get("id")
            if d and re.match(r'^\d{4}-\d{2}-\d{2}$', d):
                all_dates.append(d)
    all_dates = sorted(set(all_dates))
    print(f"  Found {len(all_dates)} distinct show dates across {len(years)} year(s).")

    if not all_dates:
        print("Nothing to seed. Run seed_cache.py first to populate shows_year_cache.")
        return

    # --missing: filter to only dates with zero setlist rows (one fast query)
    if args.missing:
        print("\nFinding missing dates (this is one MongoDB query, much faster than iterating)...")
        seeded = set(_setlist_col.distinct("date"))
        all_dates = [d for d in all_dates if d not in seeded]
        print(f"  {len(all_dates)} dates have no setlist data — retrying just those.")
        if not all_dates:
            print("Nothing missing. All shows have setlists.")
            return

    # Process in parallel
    seeded = skipped = failed = 0
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(fetch_setlist, d, args.force): d for d in all_dates}
        for i, future in enumerate(as_completed(futures), 1):
            date = futures[future]
            try:
                rows = future.result()
            except Exception as e:
                print(f"  [{i}/{len(all_dates)}] {date}: ERROR {e}", file=sys.stderr)
                failed += 1
                continue
            if rows == "skipped":
                skipped += 1
                if i % 50 == 0:
                    print(f"  [{i}/{len(all_dates)}] (skipped — already has data)")
                continue
            if rows is None:
                print(f"  [{i}/{len(all_dates)}] {date}: no source found", file=sys.stderr)
                failed += 1
                continue
            if rows:
                try:
                    _setlist_col.insert_many(rows, ordered=False)
                    seeded += 1
                    print(f"  [{i}/{len(all_dates)}] {date}: {len(rows)} songs", flush=True)
                except Exception as e:
                    print(f"  [{i}/{len(all_dates)}] {date}: insert failed — {e}", file=sys.stderr)
                    failed += 1
            time.sleep(0.05)  # small per-task delay

    print(f"\nDone. Seeded: {seeded}  Skipped: {skipped}  Failed: {failed}")
    print(f"Total rows in setlist_cache: {_setlist_col.estimated_document_count()}")

if __name__ == "__main__":
    main()
