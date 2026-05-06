"""
Backfill segue_cache by re-fetching the canonical source per show
(the one already chosen by seed_setlists.py) and detecting ">" segues.

Usage:
    MONGO_URI="mongodb+srv://..." python seed_segues.py

Options:
    --workers N     Parallel metadata fetches (default 12)
    --keep          Merge with existing segue counts instead of wiping
                    (default: wipe and rewrite — user-derived counts replaced)

Why this is needed: seed_setlists.py strips ">" characters when normalizing
track titles (so song names match across spellings), so the > info is lost
in setlist_cache. This script re-fetches Archive.org metadata for the
exact source_id stored alongside each row and counts ">" pairs.

Reuses each show's already-chosen source_id so we don't waste bandwidth
re-discovering "best source" — that decision was already made.
"""
import os, sys, re, time, argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
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
_setlist_col = _db["setlist_cache"]
_segue_col   = _db["segue_cache"]

ARCHIVE_METADATA = "https://archive.org/metadata"

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

NON_MUSICAL = re.compile(
    r'^(tun(e|ing)|crowd|audience|intro|outro|banter|talk|chat|'
    r'sound\s*check|silence|gap|//|--|---|applause|stage|'
    r'announcement|tape\s*flip|reel\s*change)\b',
    re.IGNORECASE
)

def archive_metadata(identifier):
    r = requests.get(f"{ARCHIVE_METADATA}/{identifier}", timeout=15)
    r.raise_for_status()
    return r.json()

def extract_segues(source_id):
    """Fetch metadata for one source and return a list of (from, to) segue pairs."""
    try:
        meta = archive_metadata(source_id)
    except Exception:
        return None
    files = meta.get("files", [])
    mp3s = [f for f in files if f.get("format") in ("VBR MP3", "MP3", "128Kbps MP3", "64Kbps MP3")]
    def tnum(f):
        try: return int(f.get("track") or 0)
        except (ValueError, TypeError): return 0
    mp3s.sort(key=tnum)

    # Build ordered list of (raw_title, normalized) for non-musical filtering
    norms = []
    for f in mp3s:
        title = (f.get("title") or "").strip()
        if not title or NON_MUSICAL.match(title):
            continue
        clean = title.rstrip('>').strip()
        n = norm_song(clean)
        if not n or len(n) <= 2:
            continue
        norms.append((title, n))

    pairs = []
    for i, (raw, norm_a) in enumerate(norms):
        if raw.endswith('>') and i + 1 < len(norms):
            norm_b = norms[i + 1][1]
            if norm_a and norm_b and norm_a != norm_b:
                pairs.append((norm_a, norm_b))
    return pairs

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=12, help="Parallel workers")
    parser.add_argument("--keep", action="store_true",
                        help="Merge with existing counts instead of wiping")
    args = parser.parse_args()

    # Get all distinct (date, source_id) pairs from setlist_cache
    print("Loading canonical sources from setlist_cache...")
    pipeline = [
        {"$group": {"_id": {"date": "$date", "source_id": "$source_id"}}},
        {"$project": {"_id": 0, "date": "$_id.date", "source_id": "$_id.source_id"}},
    ]
    sources = [d for d in _setlist_col.aggregate(pipeline) if d.get("source_id")]
    # If multiple sources per date (from user listens), keep one — prefer SBD
    by_date = {}
    for s in sources:
        d = s["date"]
        sid = s["source_id"]
        if d not in by_date or ("sbd" in sid.lower() and "sbd" not in by_date[d].lower()):
            by_date[d] = sid
    sources = [{"date": d, "source_id": sid} for d, sid in by_date.items()]
    print(f"  {len(sources)} unique (date, source) pairs to process.")

    if not sources:
        print("No sources found in setlist_cache. Run seed_setlists.py first.")
        return

    if not args.keep:
        print(f"\nWiping segue_cache ({_segue_col.estimated_document_count()} existing pairs)...")
        _segue_col.delete_many({})

    # Process in parallel
    seeded = failed = total_pairs = 0
    pair_counts = {}  # (from, to) -> count, accumulated locally then bulk-written

    print(f"\nFetching metadata + extracting segues with {args.workers} workers...")
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(extract_segues, s["source_id"]): s for s in sources}
        for i, future in enumerate(as_completed(futures), 1):
            src = futures[future]
            try:
                pairs = future.result()
            except Exception as e:
                print(f"  [{i}/{len(sources)}] {src['date']}: ERROR {e}", file=sys.stderr)
                failed += 1
                continue
            if pairs is None:
                failed += 1
                continue
            seeded += 1
            for (a, b) in pairs:
                pair_counts[(a, b)] = pair_counts.get((a, b), 0) + 1
                total_pairs += 1
            if i % 50 == 0:
                print(f"  [{i}/{len(sources)}] processed · {total_pairs} segue pairs accumulated")

    print(f"\nWriting {len(pair_counts)} unique pairs to MongoDB...")
    bulk_ops = []
    for (a, b), count in pair_counts.items():
        pair_id = f"{a}||{b}"
        bulk_ops.append({
            "_id": pair_id,
            "from": a,
            "to":   b,
            "count": count,
        })
    if bulk_ops:
        # Wipe-and-write was already done above (unless --keep), so just upsert
        from pymongo import UpdateOne
        ops = [
            UpdateOne(
                {"_id": doc["_id"]},
                {"$inc": {"count": doc["count"]},
                 "$setOnInsert": {"from": doc["from"], "to": doc["to"]}},
                upsert=True,
            )
            for doc in bulk_ops
        ]
        # Bulk in batches of 500
        for i in range(0, len(ops), 500):
            _segue_col.bulk_write(ops[i:i+500], ordered=False)

    print(f"\nDone.")
    print(f"  Sources processed: {seeded}")
    print(f"  Sources failed:    {failed}")
    print(f"  Total segues:      {total_pairs}")
    print(f"  Unique pairs:      {len(pair_counts)}")
    if pair_counts:
        # Show top 10
        top = sorted(pair_counts.items(), key=lambda x: -x[1])[:10]
        print("\nTop 10 segue pairs:")
        for (a, b), c in top:
            print(f"  {c:4d}× {a} > {b}")

if __name__ == "__main__":
    main()
