"""
Run this locally to pre-seed the MongoDB show/source/today caches.
Render's IP is blocked by Archive.org; your machine isn't.

Usage:
    MONGO_URI="mongodb+srv://..." python seed_cache.py

Or set MONGO_URI in a .env file and run with:
    python seed_cache.py
"""
import os, time, sys
from datetime import datetime, timezone
import requests
from pymongo import MongoClient

MONGO_URI = os.environ.get("MONGO_URI") or input("Paste your MONGO_URI: ").strip()
COLLECTION = "GratefulDead"
ARCHIVE_SEARCH = "https://archive.org/advancedsearch.php"

_mongo = MongoClient(MONGO_URI)
_db = _mongo.get_default_database() if "?" in MONGO_URI or MONGO_URI.count("/") >= 3 else _mongo["ship_of_fools"]
_shows_year_cache = _db["shows_year_cache"]
_today_cache      = _db["today_cache"]

SOURCE_TYPE_ORDER = {"SBD": 0, "MTX": 1, "FOB": 2, "AUD": 3, "UNK": 4}

def parse_source_type(identifier):
    parts = set(identifier.lower().replace("-", ".").split("."))
    if "sbd" in parts or "soundboard" in parts: return "SBD"
    if "mtx" in parts or "matrix" in parts:     return "MTX"
    if "fob" in parts:                           return "FOB"
    if "aud" in parts or "audience" in parts:    return "AUD"
    return "UNK"

def composite_score(avg_rating, num_reviews, source_type):
    m, C = 3.8, 10
    R = avg_rating if avg_rating is not None else m
    n = num_reviews or 0
    bayesian = (C * m + n * R) / (C + n)
    mult = {"SBD":1.00,"MTX":0.90,"FOB":0.80,"AUD":0.70,"UNK":0.65}.get(source_type, 0.80)
    return round(bayesian * mult * 2, 2)

def mcache_set(col, key, data):
    col.update_one({"_id": key}, {"$set": {"data": data, "ts": datetime.now(timezone.utc)}}, upsert=True)

def search(params):
    r = requests.get(ARCHIVE_SEARCH, params=params, timeout=20)
    r.raise_for_status()
    return r.json()

# ── Seed all 31 years of show listings ────────────────────────────────────────
years = list(range(1965, 1996))
print(f"Seeding {len(years)} years of shows...")
for year in years:
    existing = _shows_year_cache.find_one({"_id": str(year)}, {"_id": 1})
    if existing:
        print(f"  {year} already cached — skipping")
        continue
    try:
        data = search({
            "q": f"collection:{COLLECTION} AND year:{year}",
            "fl[]": "identifier,title,date,coverage",
            "output": "json", "rows": 1000, "sort[]": "date asc",
        })
        docs = data.get("response", {}).get("docs", [])
        seen, result = {}, []
        for doc in docs:
            d = doc.get("date") or ""
            if isinstance(d, list): d = d[0] if d else ""
            d = d[:10]
            if not d or d in seen: continue
            seen[d] = True
            title = doc.get("title", "")
            venue = (title.split(" at ", 1)[1].split(" on ")[0].strip()
                     if " at " in title and " on " in title else "")
            result.append({"id": d, "display_date": d,
                           "venue": {"name": venue or title[:60],
                                     "location": doc.get("coverage", "")},
                           "avg_rating": None})
        mcache_set(_shows_year_cache, str(year), result)
        print(f"  {year}: {len(result)} shows cached")
    except Exception as e:
        print(f"  {year}: FAILED — {e}", file=sys.stderr)
    time.sleep(1.5)

# ── Seed "today in history" for every day of the year ─────────────────────────
print("\nSeeding 'today in history' for all 365 days...")
for month in range(1, 13):
    days_in_month = [31,28,31,30,31,30,31,31,30,31,30,31][month-1]
    for day in range(1, days_in_month + 1):
        mm, dd = f"{month:02d}", f"{day:02d}"
        key = f"{mm}-{dd}"
        existing = _today_cache.find_one({"_id": key}, {"_id": 1})
        if existing:
            continue
        try:
            date_terms = " OR ".join(f"{y}-{mm}-{dd}" for y in range(1965, 1996))
            data = search({
                "q": f"collection:{COLLECTION} AND date:({date_terms})",
                "fl[]": "identifier,title,date,coverage,avg_rating,num_reviews",
                "output": "json", "rows": 500, "sort[]": "date asc",
            })
            docs = data.get("response", {}).get("docs", [])
            result = []
            for doc in docs:
                date_str = doc.get("date") or ""
                if isinstance(date_str, list): date_str = date_str[0] if date_str else ""
                date_str = date_str[:10]
                if not date_str: continue
                title = doc.get("title", "")
                venue = (title.split(" at ", 1)[1].split(" on ")[0].strip()
                         if " at " in title and " on " in title else "")
                identifier = doc["identifier"]
                source_type = parse_source_type(identifier)
                avg_rating = doc.get("avg_rating")
                num_reviews = doc.get("num_reviews", 0) or 0
                result.append({
                    "id": date_str, "identifier": identifier,
                    "display_date": date_str,
                    "venue": {"name": venue or title[:60], "location": doc.get("coverage", "")},
                    "source_type": source_type, "avg_rating": avg_rating,
                    "num_reviews": num_reviews,
                    "score": composite_score(avg_rating, num_reviews, source_type),
                    "community_listens": 0,
                })
            result.sort(key=lambda x: x["score"], reverse=True)
            mcache_set(_today_cache, key, result)
            print(f"  {mm}-{dd}: {len(result)} shows")
        except Exception as e:
            print(f"  {mm}-{dd}: FAILED — {e}", file=sys.stderr)
        time.sleep(1.5)

print("\nDone. MongoDB cache is fully seeded.")
