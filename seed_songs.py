"""
Scrape the deaddisc.com canonical song list and seed canonical_songs collection.
This becomes the gate for DEBUT / DROUGHT BREAKER badges and the Song Archive
timeline — non-songs (tuning, soundcheck-only, rehearsal-only) won't appear.

Usage:
    MONGO_URI="mongodb+srv://..." python seed_songs.py
"""
import os, sys, re
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
_col   = _db["canonical_songs"]

URL = "https://www.deaddisc.com/GDFD_Songs_Perf.htm"

# <tr><td width="380"><a href="songs/X.htm"><img...></a> SONG TITLE<br></td>
# <td...>Yes/-</td><td...>Yes/-</td><td...>NOTES</td></tr>
ROW_RE = re.compile(
    r'<tr>\s*<td[^>]*width="380"[^>]*>\s*<a[^>]*href="(songs/[^"]+)"[^>]*>'
    r'<img[^>]*></a>\s*([^<]+?)(?:<br>)?\s*</td>\s*'
    r'<td[^>]*>([^<]+)</td>\s*'
    r'<td[^>]*>([^<]+)</td>\s*'
    r'<td[^>]*>([^<]+)</td>\s*</tr>',
    re.IGNORECASE | re.DOTALL,
)

# Same normalization as app.py _norm_song
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

# Patterns in title or notes that disqualify an entry from being a "song"
NON_SONG_TITLE_PAT = re.compile(r'\btuning\b|\bjam tease\b|\bsolo tease\b', re.IGNORECASE)
NON_SONG_NOTES_PAT = re.compile(
    r'^(?:\s*(?:tuning|soundcheck only|rehearsal only|studio outtake|'
    r'unknown or misnamed|unidentified|unreleased)\b)',
    re.IGNORECASE
)

def parse_yes(cell):
    return 'yes' in cell.strip().lower()

def main():
    print(f"Fetching {URL}...")
    r = requests.get(URL, timeout=30)
    r.raise_for_status()
    html = r.text

    rows = ROW_RE.findall(html)
    print(f"Found {len(rows)} rows on the page.")

    kept = []
    skipped_not_live = 0
    skipped_tuning = 0
    skipped_notes = 0
    skipped_short = 0

    for link, title_raw, live_cell, released_cell, notes_cell in rows:
        title = re.sub(r'\s+', ' ', title_raw).strip()
        notes = re.sub(r'\s+', ' ', notes_cell).replace('&nbsp;', '').strip().lstrip('-').strip()
        live = parse_yes(live_cell)
        released = parse_yes(released_cell)

        if not live:
            skipped_not_live += 1
            continue
        if NON_SONG_TITLE_PAT.search(title):
            skipped_tuning += 1
            continue
        if NON_SONG_NOTES_PAT.match(notes):
            skipped_notes += 1
            continue
        n = norm_song(title)
        if not n or len(n) <= 2:
            skipped_short += 1
            continue

        kept.append({
            "_id":           n,
            "official_name": title,
            "released":      released,
            "notes":         notes if notes else None,
            "url":           f"https://www.deaddisc.com/{link}",
        })

    print(f"\n  Kept (canonical songs):       {len(kept)}")
    print(f"  Skipped (not played live):     {skipped_not_live}")
    print(f"  Skipped (tuning in title):     {skipped_tuning}")
    print(f"  Skipped (soundcheck/rehearsal/etc): {skipped_notes}")
    print(f"  Skipped (too-short normalized): {skipped_short}")

    # Wipe + rewrite — small dataset (~600 docs)
    print("\nWriting to MongoDB...")
    _col.delete_many({})
    if kept:
        # De-dupe by _id (different aliases may collapse to same normalized name)
        seen = {}
        for doc in kept:
            seen[doc["_id"]] = doc  # last wins; small enough not to matter
        now = datetime.now(timezone.utc)
        for d in seen.values():
            d["ts"] = now
        _col.insert_many(list(seen.values()))
        print(f"Done. {len(seen)} canonical songs seeded.")
    else:
        print("Nothing to seed — check the page format hasn't changed.")

if __name__ == "__main__":
    main()
