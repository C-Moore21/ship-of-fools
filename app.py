from flask import Flask, render_template, jsonify, request, session
from tinydb import TinyDB, Query
from werkzeug.security import generate_password_hash, check_password_hash
import requests
import os
import functools

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-change-in-prod")

# ── Database ──────────────────────────────────────────────────────────────────
# TinyDB: pure-Python NoSQL, stores everything as JSON in a local file.
# Great for small/hobby apps. Upgrade path: MongoDB Atlas free tier or Redis.
db = TinyDB("db.json")
users_table = db.table("users")
ratings_table = db.table("ratings")
show_ratings_table = db.table("show_ratings")
listens_table = db.table("listens")
User = Query()
Rating = Query()
ShowRating = Query()
Listen = Query()

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
    if users_table.get(User.username == username):
        return jsonify({"error": "Username already taken"}), 409
    users_table.insert({"username": username, "password_hash": generate_password_hash(password)})
    session["username"] = username
    return jsonify({"ok": True, "username": username})

@app.route("/api/auth/login", methods=["POST"])
def login():
    data = request.get_json()
    username = (data.get("username") or "").strip().lower()
    password = data.get("password") or ""
    user = users_table.get(User.username == username)
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
    stars       = int(data.get("stars", 0))
    if not track_id or stars not in range(1, 6):
        return jsonify({"error": "track_id and stars (1-5) required"}), 400
    username = current_user()
    record = {"username": username, "track_id": track_id, "track_title": track_title,
              "show_date": show_date, "source_id": source_id, "stars": stars}
    if ratings_table.get((Rating.username == username) & (Rating.track_id == track_id)):
        ratings_table.update(record, (Rating.username == username) & (Rating.track_id == track_id))
    else:
        ratings_table.insert(record)
    return jsonify({"ok": True, "stars": stars})

@app.route("/api/ratings", methods=["DELETE"])
@login_required
def delete_rating():
    data = request.get_json()
    track_id = data.get("track_id")
    username = current_user()
    ratings_table.remove((Rating.username == username) & (Rating.track_id == track_id))
    return jsonify({"ok": True})

@app.route("/api/ratings/mine")
@login_required
def my_ratings():
    username = current_user()
    rows = ratings_table.search(Rating.username == username)
    rows.sort(key=lambda r: (r.get("show_date", ""), r.get("track_title", "")))
    return jsonify(rows)

@app.route("/api/ratings/show")
@login_required
def show_ratings():
    source_id = request.args.get("source_id", "")
    username = current_user()
    rows = ratings_table.search((Rating.username == username) & (Rating.source_id == source_id))
    return jsonify({r["track_id"]: r["stars"] for r in rows})

# ── Show ratings routes ───────────────────────────────────────────────────────
@app.route("/api/show-ratings", methods=["POST"])
@login_required
def upsert_show_rating():
    data = request.get_json()
    show_id   = data.get("show_id", "")
    venue     = data.get("venue", "")
    stars     = int(data.get("stars", 0))
    if not show_id or stars not in range(1, 6):
        return jsonify({"error": "show_id and stars (1-5) required"}), 400
    username = current_user()
    record = {"username": username, "show_id": show_id, "venue": venue, "stars": stars}
    if show_ratings_table.get((ShowRating.username == username) & (ShowRating.show_id == show_id)):
        show_ratings_table.update(record, (ShowRating.username == username) & (ShowRating.show_id == show_id))
    else:
        show_ratings_table.insert(record)
    return jsonify({"ok": True, "stars": stars})

@app.route("/api/show-ratings", methods=["DELETE"])
@login_required
def delete_show_rating():
    data = request.get_json()
    show_id = data.get("show_id", "")
    username = current_user()
    show_ratings_table.remove((ShowRating.username == username) & (ShowRating.show_id == show_id))
    return jsonify({"ok": True})

@app.route("/api/show-ratings/mine")
@login_required
def my_show_ratings():
    username = current_user()
    rows = show_ratings_table.search(ShowRating.username == username)
    rows.sort(key=lambda r: r.get("show_id", ""))
    return jsonify(rows)

@app.route("/api/show-ratings/lookup")
@login_required
def lookup_show_rating():
    show_id = request.args.get("show_id", "")
    username = current_user()
    row = show_ratings_table.get((ShowRating.username == username) & (ShowRating.show_id == show_id))
    return jsonify({"stars": row["stars"] if row else 0})

# ── Archive.org proxy routes ──────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/years")
def years():
    # Grateful Dead active years
    return jsonify([{"year": str(y), "show_count": None} for y in range(1995, 1964, -1)])

@app.route("/api/years/<year>/shows")
def shows(year):
    data = archive_search({
        "q": f"collection:{COLLECTION} AND year:{year}",
        "fl[]": "identifier,title,date,coverage",
        "output": "json",
        "rows": 1000,
        "sort[]": "date asc",
    })
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
    return jsonify(result)

_SOURCE_TYPE_ORDER = {"SBD": 0, "MTX": 1, "FOB": 2, "AUD": 3, "UNK": 4}

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
            return int(parts[0]) * 60 + float(parts[1])
        return float(s)
    except (ValueError, TypeError):
        return 0

@app.route("/api/shows/<path:show_id>/sources")
def show_sources(show_id):
    data = archive_search({
        "q": f"collection:{COLLECTION} AND date:{show_id}*",
        "fl[]": "identifier,title,avg_rating,num_reviews",
        "output": "json",
        "rows": 100,
    })
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
    return jsonify(sources)

@app.route("/api/sources/<path:identifier>/tracks")
def source_tracks(identifier):
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
    return jsonify({
        "sets": sets,
        "lineage": item_meta.get("source") or item_meta.get("lineage") or "",
        "taper": item_meta.get("taper") or "",
        "transferer": item_meta.get("transferer") or "",
    })

# ── Listen tracking ───────────────────────────────────────────────────────────
@app.route("/api/listens", methods=["POST"])
@login_required
def record_listen():
    from datetime import datetime, timezone
    data = request.get_json()
    seconds = int(data.get("seconds", 0))
    if seconds < 5:
        return jsonify({"ok": True})  # ignore tiny accidental plays
    listens_table.insert({
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
    rows = listens_table.search(Listen.username == username)

    total_seconds = sum(r["seconds"] for r in rows)

    by_show = defaultdict(lambda: {"seconds": 0, "show_date": "", "show_id": ""})
    for r in rows:
        k = r.get("show_id") or r.get("show_date", "")
        by_show[k]["seconds"]   += r["seconds"]
        by_show[k]["show_date"]  = r.get("show_date", k)
        by_show[k]["show_id"]    = k

    by_track = defaultdict(lambda: {"seconds": 0, "track_title": "", "show_date": ""})
    for r in rows:
        k = r.get("track_id", r.get("track_title", ""))
        by_track[k]["seconds"]     += r["seconds"]
        by_track[k]["track_title"]  = r.get("track_title", k)
        by_track[k]["show_date"]    = r.get("show_date", "")

    top_shows  = sorted(by_show.values(),  key=lambda x: x["seconds"], reverse=True)[:10]
    top_tracks = sorted(by_track.values(), key=lambda x: x["seconds"], reverse=True)[:10]
    recent     = sorted(rows, key=lambda x: x.get("ts", ""), reverse=True)[:20]

    return jsonify({
        "total_seconds": total_seconds,
        "total_listens": len(rows),
        "top_shows":     top_shows,
        "top_tracks":    top_tracks,
        "recent":        recent,
    })

# ── Leaderboard ───────────────────────────────────────────────────────────────
@app.route("/api/leaderboard")
def leaderboard():
    from collections import defaultdict
    rows = listens_table.all()
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

if __name__ == "__main__":
    app.run(debug=True, port=5000)
