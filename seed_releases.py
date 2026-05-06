"""
Scrape deaddisc.com to find shows officially released by the Grateful Dead.
Run locally to seed the MongoDB releases_cache collection.

Usage:
    MONGO_URI="mongodb+srv://..." python seed_releases.py

Or with the env var pre-set:
    python seed_releases.py

Stores only FULL show releases (entries marked "Live") — partial / track-level
releases on compilations are intentionally skipped.
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
_mongo    = MongoClient(MONGO_URI)
_db       = _mongo.get_default_database() if "?" in MONGO_URI or MONGO_URI.count("/") >= 3 else _mongo["ship_of_fools"]
_col      = _db["releases_cache"]

URL = "https://www.deaddisc.com/GDFD_Dead_By_Date.htm"

MONTHS = {
    "jan":1, "january":1, "feb":2, "february":2, "mar":3, "march":3,
    "apr":4, "april":4, "may":5, "jun":6, "june":6, "jul":7, "july":7,
    "aug":8, "august":8, "sep":9, "sept":9, "september":9,
    "oct":10, "october":10, "nov":11, "november":11, "dec":12, "december":12,
}

# <tr><td>DATE</td><td>TYPE</td><td><a href="LINK">TITLE, ARTIST, YEAR</a></td></tr>
ROW_RE = re.compile(
    r'<tr>\s*<td[^>]*>([^<]+)</td>\s*<td[^>]*>([^<]+)</td>\s*'
    r'<td[^>]*><a[^>]*href="([^"]+)"[^>]*>([^<]+)</a></td>\s*</tr>',
    re.IGNORECASE | re.DOTALL,
)

def parse_dates(date_str):
    """Return list of YYYY-MM-DD dates from a deaddisc date cell.
    Handles 'May 8, 1977', 'Feb 22-24, 1968', 'May 5-7, 1977'.
    Skips 'Unknown date X' entries."""
    s = date_str.strip()
    if "unknown" in s.lower():
        return []
    # Try: "Month Day-Day, Year" range
    m = re.match(r'(\w+)\s+(\d+)\s*[-–]\s*(\d+)\s*,\s*(\d{4})', s)
    if m:
        month_name, d_start, d_end, year = m.group(1), int(m.group(2)), int(m.group(3)), int(m.group(4))
        mn = MONTHS.get(month_name.lower())
        if not mn: return []
        return [f"{year:04d}-{mn:02d}-{d:02d}" for d in range(d_start, d_end + 1)]
    # Try: "Month Day, Year"
    m = re.match(r'(\w+)\s+(\d+)\s*,\s*(\d{4})', s)
    if m:
        month_name, d, year = m.group(1), int(m.group(2)), int(m.group(3))
        mn = MONTHS.get(month_name.lower())
        if not mn: return []
        return [f"{year:04d}-{mn:02d}-{d:02d}"]
    return []

def parse_release_text(text):
    """deaddisc format: 'Title, Artist, Year' — split on last comma to get year."""
    text = re.sub(r'\s+', ' ', text).strip()
    m = re.match(r'^(.+),\s*(\d{4})\s*$', text)
    if m:
        title_with_artist = m.group(1).strip()
        year = m.group(2)
        # Strip trailing ", Grateful Dead" if present
        title = re.sub(r',\s*Grateful Dead\s*$', '', title_with_artist).strip()
        return title, year
    return text, None

def main():
    print(f"Fetching {URL}...")
    r = requests.get(URL, timeout=30)
    r.raise_for_status()
    html = r.text

    rows = ROW_RE.findall(html)
    print(f"Found {len(rows)} entries on the page.")

    # Group full-show releases by date
    by_date = {}  # date_str -> list of {name, year, url}
    skipped_partial = 0
    skipped_unknown = 0
    for date_cell, type_cell, link_url, link_text in rows:
        type_clean = type_cell.strip().lower()
        # Only keep FULL show releases (type == "Live"). Skip "X live tracks", "X songs", etc.
        if type_clean != "live":
            skipped_partial += 1
            continue
        dates = parse_dates(date_cell)
        if not dates:
            skipped_unknown += 1
            continue
        title, year = parse_release_text(link_text)
        for d in dates:
            by_date.setdefault(d, []).append({
                "name": title,
                "year": year,
                "url":  f"https://www.deaddisc.com/{link_url}" if not link_url.startswith("http") else link_url,
            })

    print(f"  Full-show releases kept:  {sum(len(v) for v in by_date.values())}")
    print(f"  Distinct show dates:       {len(by_date)}")
    print(f"  Skipped (partial/comp):    {skipped_partial}")
    print(f"  Skipped (unknown date):    {skipped_unknown}")

    # Wipe and rewrite — small dataset, simpler than diffing
    print("\nWriting to MongoDB...")
    _col.delete_many({})
    now = datetime.now(timezone.utc)
    docs = [{"_id": d, "releases": rels, "ts": now} for d, rels in sorted(by_date.items())]
    if docs:
        _col.insert_many(docs)
    print(f"Done. {len(docs)} dates seeded into releases_cache.")

if __name__ == "__main__":
    main()
