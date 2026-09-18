"""
Warm tracks_cache for the top-ranked source of every Grateful Dead show.
Run locally — Render's IP is blocked by Archive.org; your machine isn't.

Usage:
    MONGO_URI="mongodb+srv://..." python seed_tracks.py

Options:
    --year YYYY    Seed only one year (e.g. --year 1977)
    --force        Re-fetch even sources already cached
    --workers N    Parallel metadata fetches (default 8)
    --dry-run      Report coverage without writing

WHY THIS EXISTS
---------------
`/api/shows/<date>?include=tracks` folds the top source's tracklist into the
show-detail response so opening a show is one round trip instead of two. It is
deliberately cache-only: it reads tracks_cache and, on a miss, simply omits the
key so the client falls back to /api/sources/<id>/tracks. It must never call
Archive.org itself — that is the most-clicked endpoint, and Archive.org blocks
Render's IP regardless.

So the optimisation is only as good as tracks_cache coverage. That cache fills
on demand as people open shows, which measured at **10%** of shows right after
the feature shipped, and it carries a 30-day TTL (see the expireAfterSeconds
index in app.py), so it decays back down. This script pre-warms it.

RANKING MUST MATCH THE SERVER
-----------------------------
show_detail looks up the tracklist of `sources[0]` after `_enrich_and_sort_sources`
has sorted by `_composite_score`. If this script seeded a source chosen by any
other rule, the cache would be warm for the wrong identifier and the endpoint
would still miss. The scoring below is copied from app.py and must be kept in
step with it.

Re-run monthly alongside seed_cache.py / seed_setlists.py.
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
_db = _mongo.get_default_database() if "?" in MONGO_URI or MONGO_URI.count("/") >= 3 else _mongo["ship_of_fools"]
_tracks_col = _db["tracks_cache"]
_shows_year_col = _db["shows_year_cache"]

ARCHIVE_SEARCH = "https://archive.org/advancedsearch.php"
ARCHIVE_META = "https://archive.org/metadata"
ARCHIVE_DOWNLOAD = "https://archive.org/download"
COLLECTION = "GratefulDead"

# ── Scoring — mirrors app.py. Keep in step. ──────────────────────────────────
_SOURCE_MULTIPLIER = {"SBD": 1.00, "MTX": 0.90, "FOB": 0.80, "AUD": 0.70, "UNK": 0.65}
_BAYES_C = 10
_GLOBAL_MEAN = 3.8


def _composite_score(avg_rating, num_reviews, source_type):
    R = avg_rating if avg_rating is not None else _GLOBAL_MEAN
    n = num_reviews or 0
    bayesian = (_BAYES_C * _GLOBAL_MEAN + n * R) / (_BAYES_C + n)
    return round(bayesian * _SOURCE_MULTIPLIER.get(source_type, 0.80) * 2, 2)


def _parse_source_type(identifier):
    parts = set(identifier.lower().replace("-", ".").split("."))
    if "sbd" in parts or "soundboard" in parts:
        return "SBD"
    if "mtx" in parts or "matrix" in parts:
        return "MTX"
    if "fob" in parts:
        return "FOB"
    if "aud" in parts or "audience" in parts:
        return "AUD"
    return "UNK"


def _parse_duration(raw):
    try:
        s = str(raw or "0")
        if ":" in s:
            parts = [float(p) for p in s.split(":")]
            secs = 0.0
            for p in parts:
                secs = secs * 60 + p
            return int(secs)
        return int(float(s))
    except (ValueError, TypeError):
        return 0


def _norm_album_name(name):
    return re.sub(r'[^a-z0-9]+', '', (name or '').lower())


def _build_tracks_doc(identifier, meta):
    """Produce exactly the shape /api/sources/<id>/tracks stores, so the cached
    entry is indistinguishable from one written by a live request."""
    item_meta = meta.get("metadata", {}) or {}
    files = meta.get("files", []) or []
    mp3s = [f for f in files if f.get("format") in ("VBR MP3", "MP3", "128Kbps MP3", "64Kbps MP3")]
    if not mp3s:
        return None

    discs, disc_display = {}, {}
    for ord_idx, f in enumerate(mp3s):
        album = f.get("album") or "Set 1"
        norm = _norm_album_name(album)
        if norm not in discs:
            discs[norm] = []
            disc_display[norm] = album
        try:
            track_num = int(f.get("track") or 0)
        except (ValueError, TypeError):
            track_num = 0
        discs[norm].append({
            "id": f["name"],
            "title": f.get("title") or f["name"],
            "duration": _parse_duration(f.get("length")),
            "mp3_url": f"{ARCHIVE_DOWNLOAD}/{identifier}/{requests.utils.quote(f['name'])}",
            "track": track_num,
            "_ord": ord_idx,
        })

    # Un-numbered files go last, keeping file order — some tapers number only
    # one set, and sorting those to position 0 puts Set 2 before Set 1.
    for disc in discs.values():
        disc.sort(key=lambda t: (1 if t["track"] == 0 else 0, t["track"], t.get("_ord", 0)))
        for t in disc:
            t.pop("_ord", None)

    sets = sorted(
        [{"name": disc_display[k], "tracks": v} for k, v in discs.items()],
        key=lambda s: min((t["track"] for t in s["tracks"]), default=999),
    )
    return {
        "sets": sets,
        "lineage": item_meta.get("source") or item_meta.get("lineage") or "",
        "taper": item_meta.get("taper") or "",
        "transferer": item_meta.get("transferer") or "",
    }


def top_source_for(date, session):
    """The identifier show_detail will look up: highest composite score."""
    r = session.get(ARCHIVE_SEARCH, params={
        "q": f"collection:{COLLECTION} AND date:{date}*",
        "fl[]": "identifier,avg_rating,num_reviews",
        "output": "json",
        "rows": 100,
    }, timeout=20)
    r.raise_for_status()
    docs = r.json().get("response", {}).get("docs", []) or []
    best, best_score = None, -1.0
    for d in docs:
        ident = d.get("identifier")
        if not ident:
            continue
        try:
            rating = float(d["avg_rating"]) if d.get("avg_rating") is not None else None
        except (ValueError, TypeError):
            rating = None
        try:
            reviews = int(d.get("num_reviews") or 0)
        except (ValueError, TypeError):
            reviews = 0
        score = _composite_score(rating, reviews, _parse_source_type(ident))
        if score > best_score:
            best, best_score = ident, score
    return best


def all_dates(year_filter=None):
    dates = []
    for row in _shows_year_col.find({}, {"_id": 1, "data": 1}):
        year = str(row.get("_id"))
        if year_filter and year != str(year_filter):
            continue
        for sh in (row.get("data") or []):
            d = sh.get("id") or sh.get("display_date")
            if d and re.match(r'^\d{4}-\d{2}-\d{2}$', str(d)):
                dates.append(str(d))
    return sorted(set(dates))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    dates = all_dates(args.year)
    if not dates:
        print("No dates in shows_year_cache — run seed_cache.py first.")
        return 1
    print(f"{len(dates)} shows to check")

    session = requests.Session()
    session.headers["User-Agent"] = "ship-of-fools-seed/1.0"

    stats = {"cached": 0, "written": 0, "no_source": 0, "no_tracks": 0, "failed": 0}
    lock_print = time.time()

    def work(date):
        try:
            ident = top_source_for(date, session)
            if not ident:
                return ("no_source", date, None)
            if not args.force and _tracks_col.find_one({"_id": ident}, {"_id": 1}):
                return ("cached", date, ident)
            if args.dry_run:
                return ("no_tracks", date, ident)
            m = session.get(f"{ARCHIVE_META}/{ident}", timeout=30)
            m.raise_for_status()
            doc = _build_tracks_doc(ident, m.json())
            if not doc:
                return ("no_tracks", date, ident)
            _tracks_col.update_one(
                {"_id": ident},
                {"$set": {"data": doc, "ts": datetime.now(timezone.utc)}},
                upsert=True,
            )
            return ("written", date, ident)
        except Exception as e:
            return ("failed", date, str(e)[:80])

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(work, d) for d in dates]
        for i, fut in enumerate(as_completed(futures), 1):
            kind, date, extra = fut.result()
            stats[kind] += 1
            if kind == "failed":
                print(f"  ! {date}: {extra}")
            if time.time() - lock_print > 5:
                lock_print = time.time()
                print(f"  {i}/{len(dates)}  written={stats['written']} cached={stats['cached']} "
                      f"no_tracks={stats['no_tracks']} failed={stats['failed']}")

    print("\n" + "─" * 60)
    for k, v in stats.items():
        print(f"  {k:12} {v}")
    covered = stats["cached"] + stats["written"]
    print(f"\n  coverage: {covered}/{len(dates)} = {round(covered / len(dates) * 100)}%")
    if args.dry_run:
        print("  (dry run — nothing written)")
    else:
        print("\n  tracks_cache has a 30-day TTL — re-run monthly with the other seeds.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
