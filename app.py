from flask import Flask, render_template, jsonify, request, session
from pymongo import MongoClient
from werkzeug.security import generate_password_hash, check_password_hash
import requests
import os
import functools
import time

# ── Simple in-memory cache ────────────────────────────────────────────────────
_cache = {}
_CACHE_TTL = 300  # 5 minutes

def _cache_get(key):
    entry = _cache.get(key)
    if entry and time.time() - entry["ts"] < _CACHE_TTL:
        return entry["val"]
    return None

def _cache_set(key, val):
    _cache[key] = {"val": val, "ts": time.time()}

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-change-in-prod")

# ── Database ──────────────────────────────────────────────────────────────────
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017/ship_of_fools")
_mongo = MongoClient(MONGO_URI)
_db = _mongo.get_default_database() if "?" in MONGO_URI or MONGO_URI.count("/") >= 3 else _mongo["ship_of_fools"]
users_table      = _db["users"]
ratings_table    = _db["ratings"]
show_ratings_table = _db["show_ratings"]
listens_table    = _db["listens"]
notes_table      = _db["notes"]

# Indexes (no-op if they already exist)
users_table.create_index("username", unique=True)
ratings_table.create_index([("username", 1), ("track_id", 1)], unique=True)
show_ratings_table.create_index([("username", 1), ("show_id", 1)], unique=True)
listens_table.create_index([("username", 1), ("ts", 1)])
notes_table.create_index([("username", 1), ("show_id", 1)], unique=True)

# ── Archive.org API ───────────────────────────────────────────────────────────
ARCHIVE_SEARCH   = "https://archive.org/advancedsearch.php"
ARCHIVE_METADATA = "https://archive.org/metadata"
ARCHIVE_DOWNLOAD = "https://archive.org/download"
COLLECTION       = "GratefulDead"

def archive_search(params):
    r = requests.get(ARCHIVE_SEARCH, params=params, timeout=15)
    r.raise_for_status()
    return r.json()

def archive_metadata(identifier):
    r = requests.get(f"{ARCHIVE_METADATA}/{identifier}", timeout=15)
    r.raise_for_status()
    return r.json()

# ── Auth helpers ──────────────────────────────────────────────────────────────
def login_required(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if "username" not in session:
            return jsonify({"error": "Not logged in"}), 401
        return f(*args, **kwargs)
    return decorated

def current_user():
    return session.get("username")

# ── Auth routes ───────────────────────────────────────────────────────────────
@app.route("/api/auth/register", methods=["POST"])
def register():
    data = request.get_json()
    username = (data.get("username") or "").strip().lower()
    password = data.get("password") or ""
    if not username or not password:
        return jsonify({"error": "Username and password required"}), 400
    if len(username) < 3:
        return jsonify({"error": "Username must be at least 3 characters"}), 400
    if len(password) < 6:
        return jsonify({"error": "Password must be at least 6 characters"}), 400
    import re
    if not re.match(r'^[a-z0-9_-]+$', username):
        return jsonify({"error": "Username may only contain letters, numbers, _ and -"}), 400
    if users_table.find_one({"username": username}):
        return jsonify({"error": "Username already taken"}), 409
    users_table.insert_one({"username": username, "password_hash": generate_password_hash(password)})
    session["username"] = username
    return jsonify({"ok": True, "username": username})

@app.route("/api/auth/login", methods=["POST"])
def login():
    data = request.get_json()
    username = (data.get("username") or "").strip().lower()
    password = data.get("password") or ""
    user = users_table.find_one({"username": username})
    if not user or not check_password_hash(user["password_hash"], password):
        return jsonify({"error": "Invalid username or password"}), 401
    session["username"] = username
    return jsonify({"ok": True, "username": username})

@app.route("/api/auth/logout", methods=["POST"])
def logout():
    session.clear()
    return jsonify({"ok": True})

@app.route("/api/auth/me")
def me():
    if "username" in session:
        return jsonify({"username": session["username"]})
    return jsonify({"username": None})

# ── Ratings routes ────────────────────────────────────────────────────────────
@app.route("/api/ratings", methods=["POST"])
@login_required
def upsert_rating():
    data = request.get_json()
    track_id    = data.get("track_id")
    track_title = data.get("track_title", "")
    show_date   = data.get("show_date", "")
    source_id   = str(data.get("source_id", ""))
    try:
        stars = int(data.get("stars", 0))
    except (ValueError, TypeError):
        return jsonify({"error": "stars must be an integer"}), 400
    if not track_id or stars not in range(1, 6):
        return jsonify({"error": "track_id and stars (1-5) required"}), 400
    username = current_user()
    record = {"username": username, "track_id": track_id, "track_title": track_title,
              "show_date": show_date, "source_id": source_id, "stars": stars}
    ratings_table.update_one({"username": username, "track_id": track_id}, {"$set": record}, upsert=True)
    return jsonify({"ok": True, "stars": stars})

@app.route("/api/ratings", methods=["DELETE"])
@login_required
def delete_rating():
    data = request.get_json()
    track_id = data.get("track_id")
    if not track_id:
        return jsonify({"error": "track_id required"}), 400
    username = current_user()
    ratings_table.delete_one({"username": username, "track_id": track_id})
    return jsonify({"ok": True})

@app.route("/api/ratings/mine")
@login_required
def my_ratings():
    username = current_user()
    rows = list(ratings_table.find({"username": username}, {"_id": 0}).sort([("show_date", 1), ("track_title", 1)]))
    return jsonify(rows)

@app.route("/api/ratings/show")
@login_required
def show_ratings():
    source_id = request.args.get("source_id", "")
    username = current_user()
    rows = ratings_table.find({"username": username, "source_id": source_id}, {"_id": 0})
    return jsonify({r["track_id"]: r["stars"] for r in rows})

# ── Show ratings routes ───────────────────────────────────────────────────────
@app.route("/api/show-ratings", methods=["POST"])
@login_required
def upsert_show_rating():
    data = request.get_json()
    show_id   = data.get("show_id", "")
    venue     = data.get("venue", "")
    try:
        stars = int(data.get("stars", 0))
    except (ValueError, TypeError):
        return jsonify({"error": "stars must be an integer"}), 400
    if not show_id or stars not in range(1, 6):
        return jsonify({"error": "show_id and stars (1-5) required"}), 400
    username = current_user()
    record = {"username": username, "show_id": show_id, "venue": venue, "stars": stars}
    show_ratings_table.update_one({"username": username, "show_id": show_id}, {"$set": record}, upsert=True)
    return jsonify({"ok": True, "stars": stars})

@app.route("/api/show-ratings", methods=["DELETE"])
@login_required
def delete_show_rating():
    data = request.get_json()
    show_id = data.get("show_id", "")
    if not show_id:
        return jsonify({"error": "show_id required"}), 400
    username = current_user()
    show_ratings_table.delete_one({"username": username, "show_id": show_id})
    return jsonify({"ok": True})

@app.route("/api/show-ratings/mine")
@login_required
def my_show_ratings():
    username = current_user()
    rows = list(show_ratings_table.find({"username": username}, {"_id": 0}).sort("show_id", 1))
    return jsonify(rows)

@app.route("/api/show-ratings/lookup")
@login_required
def lookup_show_rating():
    show_id = request.args.get("show_id", "")
    username = current_user()
    row = show_ratings_table.find_one({"username": username, "show_id": show_id}, {"_id": 0})
    return jsonify({"stars": row["stars"] if row else 0})

# ── Archive.org proxy routes ──────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/today")
def today_in_history():
    from datetime import date
    today = date.today()
    mm = f"{today.month:02d}"
    dd = f"{today.day:02d}"
    date_terms = " OR ".join(f"{y}-{mm}-{dd}" for y in range(1965, 1996))
    try:
        data = archive_search({
            "q": f"collection:{COLLECTION} AND date:({date_terms})",
            "fl[]": "identifier,title,date,coverage,avg_rating,num_reviews",
            "output": "json",
            "rows": 500,
            "sort[]": "date asc",
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 502
    docs = data.get("response", {}).get("docs", [])
    result = []
    for doc in docs:
        date_str = doc.get("date") or ""
        if isinstance(date_str, list):
            date_str = date_str[0] if date_str else ""
        date_str = date_str[:10]
        if not date_str:
            continue
        title = doc.get("title", "")
        venue_name = ""
        if " at " in title and " on " in title:
            venue_name = title.split(" at ", 1)[1].split(" on ")[0].strip()
        identifier = doc["identifier"]
        source_type = _parse_source_type(identifier)
        avg_rating = doc.get("avg_rating")
        num_reviews = doc.get("num_reviews", 0) or 0
        score = _composite_score(avg_rating, num_reviews, source_type)
        result.append({
            "id": date_str,
            "identifier": identifier,
            "display_date": date_str,
            "venue": {
                "name": venue_name or title[:60],
                "location": doc.get("coverage", ""),
            },
            "source_type": source_type,
            "avg_rating": avg_rating,
            "num_reviews": num_reviews,
            "score": score,
        })
    result.sort(key=lambda x: x["score"], reverse=True)
    return jsonify(result)

@app.route("/api/years")
def years():
    # Grateful Dead active years
    return jsonify([{"year": str(y), "show_count": None} for y in range(1995, 1964, -1)])

@app.route("/api/years/<year>/shows")
def shows(year):
    cache_key = f"shows:{year}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return jsonify(cached)
    try:
        data = archive_search({
            "q": f"collection:{COLLECTION} AND year:{year}",
            "fl[]": "identifier,title,date,coverage",
            "output": "json",
            "rows": 1000,
            "sort[]": "date asc",
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 502
    docs = data.get("response", {}).get("docs", [])

    seen = {}
    result = []
    for doc in docs:
        date = doc.get("date") or ""
        if isinstance(date, list):
            date = date[0] if date else ""
        date = date[:10]
        if not date or date in seen:
            continue
        seen[date] = True
        title = doc.get("title", "")
        venue_name = ""
        if " at " in title and " on " in title:
            venue_name = title.split(" at ", 1)[1].split(" on ")[0].strip()
        result.append({
            "id": date,
            "display_date": date,
            "venue": {
                "name": venue_name or title[:60],
                "location": doc.get("coverage", ""),
            },
            "avg_rating": None,
        })
    _cache_set(cache_key, result)
    return jsonify(result)

_SOURCE_TYPE_ORDER = {"SBD": 0, "MTX": 1, "FOB": 2, "AUD": 3, "UNK": 4}

_SOURCE_MULTIPLIER = {"SBD": 1.00, "MTX": 0.90, "FOB": 0.80, "AUD": 0.70, "UNK": 0.65}
_BAYES_C = 10      # reviews needed to earn 50% weight over prior
_GLOBAL_MEAN = 3.8 # prior: reasonable default for archive.org GD ratings

def _composite_score(avg_rating, num_reviews, source_type):
    m = _GLOBAL_MEAN
    C = _BAYES_C
    R = avg_rating if avg_rating is not None else m
    n = num_reviews or 0
    bayesian = (C * m + n * R) / (C + n)
    multiplier = _SOURCE_MULTIPLIER.get(source_type, 0.80)
    return round(bayesian * multiplier * 2, 2)  # 0–10 display scale

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
            parts = s.split(":")
            if len(parts) == 3:
                return int(parts[0]) * 3600 + int(parts[1]) * 60 + float(parts[2])
            return int(parts[0]) * 60 + float(parts[1])
        return float(s)
    except (ValueError, TypeError):
        return 0

@app.route("/api/shows/<path:show_id>/sources")
def show_sources(show_id):
    import re
    if not re.match(r'^\d{4}-\d{2}-\d{2}', show_id):
        return jsonify({"error": "invalid show_id"}), 400
    cache_key = f"sources:{show_id}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return jsonify(cached)
    try:
        data = archive_search({
            "q": f"collection:{COLLECTION} AND date:{show_id}*",
            "fl[]": "identifier,title,avg_rating,num_reviews",
            "output": "json",
            "rows": 100,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 502
    docs = data.get("response", {}).get("docs", [])

    sources = []
    for doc in docs:
        identifier = doc["identifier"]
        source_type = _parse_source_type(identifier)
        sources.append({
            "id": identifier,
            "title": doc.get("title", identifier),
            "source_type": source_type,
            "archive_rating": doc.get("avg_rating"),
            "archive_reviews": doc.get("num_reviews", 0),
            "sets": None,
        })

    sources.sort(key=lambda s: _SOURCE_TYPE_ORDER.get(s["source_type"], 99))
    _cache_set(cache_key, sources)
    return jsonify(sources)

@app.route("/api/sources/<path:identifier>/tracks")
def source_tracks(identifier):
    cache_key = f"tracks:{identifier}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return jsonify(cached)
    try:
        meta = archive_metadata(identifier)
    except Exception as e:
        return jsonify({"error": str(e)}), 502

    item_meta = meta.get("metadata", {})
    files = meta.get("files", [])
    mp3s = [f for f in files if f.get("format") in ("VBR MP3", "MP3", "128Kbps MP3", "64Kbps MP3")]

    discs = {}
    for f in mp3s:
        album = f.get("album") or "Set 1"
        discs.setdefault(album, [])
        try:
            track_num = int(f.get("track") or 0)
        except (ValueError, TypeError):
            track_num = 0
        discs[album].append({
            "id": f["name"],
            "title": f.get("title") or f["name"],
            "duration": _parse_duration(f.get("length")),
            "mp3_url": f"{ARCHIVE_DOWNLOAD}/{identifier}/{requests.utils.quote(f['name'])}",
            "track": track_num,
        })

    for disc in discs.values():
        disc.sort(key=lambda t: t["track"])

    sets = [{"name": k, "tracks": v} for k, v in sorted(discs.items())]
    result = {
        "sets": sets,
        "lineage": item_meta.get("source") or item_meta.get("lineage") or "",
        "taper": item_meta.get("taper") or "",
        "transferer": item_meta.get("transferer") or "",
    }
    _cache_set(cache_key, result)
    return jsonify(result)

# ── Listen tracking ───────────────────────────────────────────────────────────
@app.route("/api/listens", methods=["POST"])
@login_required
def record_listen():
    from datetime import datetime, timezone
    data = request.get_json()
    seconds = int(data.get("seconds", 0))
    if seconds < 5:
        return jsonify({"ok": True})  # ignore tiny accidental plays
    listens_table.insert_one({
        "username":    current_user(),
        "track_id":    data.get("track_id", ""),
        "track_title": data.get("track_title", ""),
        "show_id":     data.get("show_id", ""),
        "show_date":   data.get("show_date", ""),
        "source_id":   data.get("source_id", ""),
        "seconds":     seconds,
        "ts":          datetime.now(timezone.utc).isoformat(),
    })
    return jsonify({"ok": True})

@app.route("/api/listens/stats")
@login_required
def listen_stats():
    from collections import defaultdict
    username = current_user()
    rows = list(listens_table.find({"username": username}, {"_id": 0}))

    total_seconds = sum(r["seconds"] for r in rows)

    by_show = defaultdict(lambda: {"seconds": 0, "show_date": ""})
    for r in rows:
        k = r.get("show_date") or r.get("show_id") or ""
        by_show[k]["seconds"]  += r["seconds"]
        by_show[k]["show_date"] = k

    by_track = defaultdict(lambda: {"seconds": 0, "track_title": "", "show_date": ""})
    for r in rows:
        k = r.get("track_id", r.get("track_title", ""))
        by_track[k]["seconds"]     += r["seconds"]
        by_track[k]["track_title"]  = r.get("track_title", k)
        by_track[k]["show_date"]    = r.get("show_date", "")

    top_shows  = sorted(by_show.values(),  key=lambda x: x["seconds"], reverse=True)[:10]
    top_tracks = sorted(by_track.values(), key=lambda x: x["seconds"], reverse=True)[:10]

    return jsonify({
        "total_seconds": total_seconds,
        "total_listens": len(rows),
        "top_shows":     top_shows,
        "top_tracks":    top_tracks,
    })

# ── Leaderboard ───────────────────────────────────────────────────────────────
@app.route("/api/leaderboard")
@login_required
def leaderboard():
    from collections import defaultdict
    rows = list(listens_table.find({}, {"_id": 0}))
    by_user = defaultdict(lambda: {"seconds": 0, "shows": set(), "tracks": 0})
    for r in rows:
        u = r.get("username", "")
        by_user[u]["seconds"]  += r.get("seconds", 0)
        by_user[u]["tracks"]   += 1
        sid = r.get("show_id") or r.get("show_date", "")
        if sid:
            by_user[u]["shows"].add(sid)
    result = sorted([
        {"username": u, "seconds": v["seconds"], "shows": len(v["shows"]), "tracks": v["tracks"]}
        for u, v in by_user.items()
    ], key=lambda x: x["seconds"], reverse=True)
    return jsonify(result)

# ── Notes ────────────────────────────────────────────────────────────────────
@app.route("/api/notes/<path:show_id>", methods=["GET"])
@login_required
def get_note(show_id):
    row = notes_table.find_one({"username": current_user(), "show_id": show_id}, {"_id": 0})
    return jsonify({"note": row["note"] if row else ""})

@app.route("/api/notes", methods=["POST"])
@login_required
def save_note():
    data = request.get_json()
    show_id = data.get("show_id", "")
    note = data.get("note", "").strip()
    if not show_id:
        return jsonify({"error": "show_id required"}), 400
    if note:
        notes_table.update_one(
            {"username": current_user(), "show_id": show_id},
            {"$set": {"username": current_user(), "show_id": show_id, "note": note}},
            upsert=True
        )
    else:
        notes_table.delete_one({"username": current_user(), "show_id": show_id})
    return jsonify({"ok": True})

# ── Listening history ─────────────────────────────────────────────────────────
@app.route("/api/listens/history")
@login_required
def listen_history():
    rows = list(listens_table.find(
        {"username": current_user()}, {"_id": 0}
    ).sort("ts", -1).limit(500))
    return jsonify(rows)

# ── On This Tour ──────────────────────────────────────────────────────────────
@app.route("/api/shows/<path:show_id>/tour")
def on_this_tour(show_id):
    import re
    if not re.match(r'^\d{4}-\d{2}-\d{2}$', show_id):
        return jsonify({"error": "invalid show_id"}), 400
    from datetime import date, timedelta
    try:
        d = date.fromisoformat(show_id)
    except ValueError:
        return jsonify({"error": "invalid show_id"}), 400
    # Fetch shows within 30 days either side — same year run
    start = (d - timedelta(days=30)).isoformat()
    end   = (d + timedelta(days=30)).isoformat()
    year  = show_id[:4]
    try:
        data = archive_search({
            "q": f"collection:{COLLECTION} AND year:{year} AND date:[{start} TO {end}]",
            "fl[]": "identifier,title,date,coverage",
            "output": "json",
            "rows": 60,
            "sort[]": "date asc",
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 502
    docs = data.get("response", {}).get("docs", [])
    seen = {}
    result = []
    for doc in docs:
        date_str = doc.get("date") or ""
        if isinstance(date_str, list):
            date_str = date_str[0] if date_str else ""
        date_str = date_str[:10]
        if not date_str or date_str in seen:
            continue
        seen[date_str] = True
        title = doc.get("title", "")
        venue_name = ""
        if " at " in title and " on " in title:
            venue_name = title.split(" at ", 1)[1].split(" on ")[0].strip()
        result.append({
            "id": date_str,
            "display_date": date_str,
            "is_current": date_str == show_id,
            "venue": {"name": venue_name or title[:50], "location": doc.get("coverage", "")},
        })
    return jsonify(result)

# ── Venue history ─────────────────────────────────────────────────────────────
@app.route("/api/venue")
def venue_history():
    venue = request.args.get("venue", "").strip()
    if not venue or len(venue) < 3:
        return jsonify({"error": "venue required"}), 400
    try:
        data = archive_search({
            "q": f'collection:{COLLECTION} AND coverage:"{venue}"',
            "fl[]": "identifier,title,date,coverage",
            "output": "json",
            "rows": 200,
            "sort[]": "date asc",
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 502
    docs = data.get("response", {}).get("docs", [])
    seen = {}
    result = []
    for doc in docs:
        date_str = doc.get("date") or ""
        if isinstance(date_str, list):
            date_str = date_str[0] if date_str else ""
        date_str = date_str[:10]
        if not date_str or date_str in seen:
            continue
        seen[date_str] = True
        result.append({"id": date_str, "display_date": date_str})
    return jsonify({"venue": venue, "shows": result})

# ── Search ────────────────────────────────────────────────────────────────────
@app.route("/api/search")
def search_shows():
    q = request.args.get("q", "").strip()
    if not q or len(q) < 2:
        return jsonify([])
    try:
        data = archive_search({
            "q": f"collection:{COLLECTION} AND ({q})",
            "fl[]": "identifier,title,date,coverage",
            "output": "json",
            "rows": 50,
            "sort[]": "date asc",
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 502
    docs = data.get("response", {}).get("docs", [])
    seen = {}
    result = []
    for doc in docs:
        date_str = doc.get("date") or ""
        if isinstance(date_str, list):
            date_str = date_str[0] if date_str else ""
        date_str = date_str[:10]
        if not date_str or date_str in seen:
            continue
        seen[date_str] = True
        title = doc.get("title", "")
        venue_name = ""
        if " at " in title and " on " in title:
            venue_name = title.split(" at ", 1)[1].split(" on ")[0].strip()
        result.append({
            "id": date_str,
            "display_date": date_str,
            "venue": {"name": venue_name or title[:60], "location": doc.get("coverage", "")},
        })
    return jsonify(result)

# ── Keep-alive (Render free tier) ────────────────────────────────────────────
_RENDER_URL = os.environ.get("RENDER_EXTERNAL_URL")
if _RENDER_URL:
    import threading, time as _time
    def _keep_alive():
        _time.sleep(60)  # wait for app to finish starting
        while True:
            try:
                requests.get(_RENDER_URL, timeout=10)
            except Exception:
                pass
            _time.sleep(14 * 60)  # ping every 14 minutes
    threading.Thread(target=_keep_alive, daemon=True).start()

if __name__ == "__main__":
    app.run(debug=True, port=5000)
