from flask import Flask, render_template, jsonify, request, session
from pymongo import MongoClient
from werkzeug.security import generate_password_hash, check_password_hash
import requests
import os
import functools
import time
import random
from collections import OrderedDict

class _LRUCache:
    def __init__(self, maxsize=500):
        self._d = OrderedDict()
        self._maxsize = maxsize
    def get(self, key, default=None):
        if key not in self._d: return default
        self._d.move_to_end(key)
        return self._d[key]
    def __contains__(self, key): return key in self._d
    def __getitem__(self, key):
        self._d.move_to_end(key)
        return self._d[key]
    def __setitem__(self, key, val):
        if key in self._d: self._d.move_to_end(key)
        self._d[key] = val
        if len(self._d) > self._maxsize:
            self._d.popitem(last=False)

# ── Simple in-memory cache ────────────────────────────────────────────────────
_cache = _LRUCache(maxsize=500)
_CACHE_TTL = 300  # 5 minutes

def _cache_get(key):
    entry = _cache.get(key)
    if entry and time.time() - entry["ts"] < _CACHE_TTL:
        return entry["val"]
    return None

def _cache_set(key, val):
    _cache[key] = {"val": val, "ts": time.time()}

# Each tour run uses date ranges; progress queries use $gte/$lte — no hardcoded show lists needed.
TOUR_ERAS = [
    {"id": "proto",    "name": "1965–1969 · Proto-Touring Era"},
    {"id": "classic",  "name": "1970–1974 · Classic Era"},
    {"id": "hiatus",   "name": "1975 · Hiatus Year"},
    {"id": "comeback", "name": "1976–1979 · Post-Hiatus"},
    {"id": "regular",  "name": "1980–1985 · Regular Cycle"},
    {"id": "brent",    "name": "1986–1990 · Brent Era"},
    {"id": "final",    "name": "1991–1995 · Final Era"},
]
TOUR_RUNS = [
    # 1965
    {"id":"1965-spring",    "era":"proto",    "name":"Spring '65",         "start":"1965-05-05","end":"1965-06-30"},
    {"id":"1965-summer",    "era":"proto",    "name":"Summer '65",         "start":"1965-07-01","end":"1965-08-31"},
    {"id":"1965-fall",      "era":"proto",    "name":"Fall/Winter '65",    "start":"1965-11-30","end":"1965-12-31"},
    # 1966
    {"id":"1966-winter",    "era":"proto",    "name":"Winter '66",         "start":"1966-01-04","end":"1966-02-28"},
    {"id":"1966-spring",    "era":"proto",    "name":"Spring '66",         "start":"1966-03-03","end":"1966-05-28"},
    {"id":"1966-summer",    "era":"proto",    "name":"Summer '66",         "start":"1966-06-01","end":"1966-08-31"},
    {"id":"1966-fall",      "era":"proto",    "name":"Fall '66",           "start":"1966-09-01","end":"1966-12-31"},
    # 1967
    {"id":"1967-winter",    "era":"proto",    "name":"Winter '67",         "start":"1967-01-01","end":"1967-02-28"},
    {"id":"1967-spring",    "era":"proto",    "name":"Spring '67",         "start":"1967-03-18","end":"1967-04-30"},
    {"id":"1967-summer",    "era":"proto",    "name":"Summer '67",         "start":"1967-06-01","end":"1967-08-31"},
    {"id":"1967-fall",      "era":"proto",    "name":"Fall '67",           "start":"1967-09-01","end":"1967-12-31"},
    # 1968
    {"id":"1968-winter",    "era":"proto",    "name":"Winter '68",         "start":"1968-01-01","end":"1968-02-29"},
    {"id":"1968-spring",    "era":"proto",    "name":"Spring '68",         "start":"1968-03-01","end":"1968-05-31"},
    {"id":"1968-summer",    "era":"proto",    "name":"Summer '68",         "start":"1968-06-01","end":"1968-08-31"},
    {"id":"1968-fall",      "era":"proto",    "name":"Fall '68",           "start":"1968-09-01","end":"1968-12-31"},
    # 1969
    {"id":"1969-winter",    "era":"proto",    "name":"Winter '69",         "start":"1969-01-01","end":"1969-03-02"},
    {"id":"1969-spring",    "era":"proto",    "name":"Spring '69",         "start":"1969-04-01","end":"1969-06-30"},
    {"id":"1969-summer",    "era":"proto",    "name":"Summer '69",         "start":"1969-07-01","end":"1969-08-31"},
    {"id":"1969-fall",      "era":"proto",    "name":"Fall/Winter '69",    "start":"1969-09-01","end":"1969-12-31"},
    # 1970
    {"id":"1970-winter",    "era":"classic",  "name":"Winter '70",         "start":"1970-01-02","end":"1970-02-14"},
    {"id":"1970-spring",    "era":"classic",  "name":"Spring '70",         "start":"1970-03-20","end":"1970-05-24"},
    {"id":"1970-summer",    "era":"classic",  "name":"Summer '70",         "start":"1970-06-04","end":"1970-08-30"},
    {"id":"1970-fall",      "era":"classic",  "name":"Fall '70",           "start":"1970-09-17","end":"1970-12-31"},
    # 1971
    {"id":"1971-winter",    "era":"classic",  "name":"Winter '71",         "start":"1971-01-21","end":"1971-02-24"},
    {"id":"1971-spring",    "era":"classic",  "name":"Spring '71",         "start":"1971-04-04","end":"1971-04-29"},
    {"id":"1971-summer",    "era":"classic",  "name":"Summer '71",         "start":"1971-07-02","end":"1971-08-24"},
    {"id":"1971-fall",      "era":"classic",  "name":"Fall '71",           "start":"1971-10-19","end":"1971-12-31"},
    # 1972
    {"id":"1972-europe",    "era":"classic",  "name":"Europe '72",         "start":"1972-04-07","end":"1972-05-26"},
    {"id":"1972-summer",    "era":"classic",  "name":"Summer '72",         "start":"1972-07-18","end":"1972-08-27"},
    {"id":"1972-fall",      "era":"classic",  "name":"Fall '72",           "start":"1972-09-17","end":"1972-11-26"},
    # 1973
    {"id":"1973-winter",    "era":"classic",  "name":"Winter/Spring '73",  "start":"1973-02-09","end":"1973-04-02"},
    {"id":"1973-summer",    "era":"classic",  "name":"Summer '73",         "start":"1973-06-10","end":"1973-07-31"},
    {"id":"1973-fall",      "era":"classic",  "name":"Fall '73",           "start":"1973-10-19","end":"1973-12-19"},
    # 1974
    {"id":"1974-spring",    "era":"classic",  "name":"Spring '74",         "start":"1974-02-23","end":"1974-02-24"},
    {"id":"1974-summer",    "era":"classic",  "name":"Summer '74",         "start":"1974-06-08","end":"1974-08-06"},
    {"id":"1974-fall",      "era":"classic",  "name":"Fall '74 (pre-hiatus)","start":"1974-10-16","end":"1974-10-20"},
    # 1975
    {"id":"1975-only",      "era":"hiatus",   "name":"'75 (Hiatus Year)",  "start":"1975-06-17","end":"1975-09-28"},
    # 1976
    {"id":"1976-summer",    "era":"comeback", "name":"Summer Comeback '76","start":"1976-06-03","end":"1976-06-29"},
    {"id":"1976-fall",      "era":"comeback", "name":"Fall '76",           "start":"1976-09-23","end":"1976-10-15"},
    # 1977
    {"id":"1977-winter",    "era":"comeback", "name":"Winter '77",         "start":"1977-02-26","end":"1977-03-20"},
    {"id":"1977-spring",    "era":"comeback", "name":"Spring '77",         "start":"1977-04-22","end":"1977-05-22"},
    {"id":"1977-fall",      "era":"comeback", "name":"Fall '77",           "start":"1977-09-03","end":"1977-10-30"},
    # 1978
    {"id":"1978-winter",    "era":"comeback", "name":"Winter '78",         "start":"1978-01-06","end":"1978-02-05"},
    {"id":"1978-spring",    "era":"comeback", "name":"Spring '78",         "start":"1978-04-06","end":"1978-04-15"},
    {"id":"1978-summer",    "era":"comeback", "name":"Summer '78",         "start":"1978-07-01","end":"1978-09-16"},
    {"id":"1978-fall",      "era":"comeback", "name":"Fall/Winter '78",    "start":"1978-10-17","end":"1978-12-31"},
    # 1979
    {"id":"1979-winter",    "era":"comeback", "name":"Winter '79",         "start":"1979-01-10","end":"1979-01-20"},
    {"id":"1979-spring",    "era":"comeback", "name":"Spring '79",         "start":"1979-04-15","end":"1979-04-22"},
    {"id":"1979-summer",    "era":"comeback", "name":"Summer '79",         "start":"1979-08-04","end":"1979-09-01"},
    {"id":"1979-fall",      "era":"comeback", "name":"Fall '79",           "start":"1979-10-24","end":"1979-12-28"},
    # 1980
    {"id":"1980-spring",    "era":"regular",  "name":"Spring '80",         "start":"1980-03-29","end":"1980-05-16"},
    {"id":"1980-summer",    "era":"regular",  "name":"Summer '80",         "start":"1980-08-30","end":"1980-09-06"},
    {"id":"1980-fall",      "era":"regular",  "name":"Fall '80",           "start":"1980-09-25","end":"1980-10-31"},
    # 1981
    {"id":"1981-spring",    "era":"regular",  "name":"Spring '81",         "start":"1981-02-26","end":"1981-03-14"},
    {"id":"1981-summer",    "era":"regular",  "name":"Summer '81",         "start":"1981-07-04","end":"1981-07-13"},
    {"id":"1981-fall",      "era":"regular",  "name":"Fall '81",           "start":"1981-08-28","end":"1981-10-19"},
    # 1982
    {"id":"1982-spring",    "era":"regular",  "name":"Spring '82",         "start":"1982-04-06","end":"1982-04-19"},
    {"id":"1982-summer",    "era":"regular",  "name":"Summer '82",         "start":"1982-07-17","end":"1982-08-10"},
    {"id":"1982-fall",      "era":"regular",  "name":"Fall '82",           "start":"1982-09-15","end":"1982-10-17"},
    # 1983
    {"id":"1983-spring",    "era":"regular",  "name":"Spring '83",         "start":"1983-04-09","end":"1983-04-17"},
    {"id":"1983-summer",    "era":"regular",  "name":"Summer '83",         "start":"1983-06-18","end":"1983-07-31"},
    {"id":"1983-fall",      "era":"regular",  "name":"Fall '83",           "start":"1983-09-02","end":"1983-10-17"},
    # 1984
    {"id":"1984-spring",    "era":"regular",  "name":"Spring '84",         "start":"1984-04-19","end":"1984-05-07"},
    {"id":"1984-summer",    "era":"regular",  "name":"Summer '84",         "start":"1984-06-21","end":"1984-07-22"},
    {"id":"1984-fall",      "era":"regular",  "name":"Fall '84",           "start":"1984-10-05","end":"1984-10-20"},
    # 1985
    {"id":"1985-spring",    "era":"regular",  "name":"Spring '85",         "start":"1985-03-27","end":"1985-04-14"},
    {"id":"1985-summer",    "era":"regular",  "name":"Summer '85",         "start":"1985-06-14","end":"1985-07-13"},
    {"id":"1985-fall",      "era":"regular",  "name":"Fall '85",           "start":"1985-10-25","end":"1985-11-10"},
    # 1986
    {"id":"1986-spring",    "era":"brent",    "name":"Spring '86",         "start":"1986-03-19","end":"1986-04-13"},
    {"id":"1986-summer",    "era":"brent",    "name":"Summer '86",         "start":"1986-06-20","end":"1986-07-04"},
    {"id":"1986-fall",      "era":"brent",    "name":"Fall '86",           "start":"1986-12-15","end":"1986-12-31"},
    # 1987
    {"id":"1987-spring",    "era":"brent",    "name":"Spring '87",         "start":"1987-03-22","end":"1987-04-19"},
    {"id":"1987-summer",    "era":"brent",    "name":"Summer '87 (Dylan)", "start":"1987-07-04","end":"1987-07-26"},
    {"id":"1987-fall",      "era":"brent",    "name":"Fall '87",           "start":"1987-09-18","end":"1987-10-04"},
    # 1988
    {"id":"1988-spring",    "era":"brent",    "name":"Spring '88",         "start":"1988-03-27","end":"1988-04-11"},
    {"id":"1988-summer",    "era":"brent",    "name":"Summer '88",         "start":"1988-06-30","end":"1988-07-17"},
    {"id":"1988-fall",      "era":"brent",    "name":"Fall '88",           "start":"1988-09-02","end":"1988-09-24"},
    # 1989
    {"id":"1989-spring",    "era":"brent",    "name":"Spring '89",         "start":"1989-03-27","end":"1989-04-15"},
    {"id":"1989-summer",    "era":"brent",    "name":"Summer '89",         "start":"1989-07-02","end":"1989-07-19"},
    {"id":"1989-fall",      "era":"brent",    "name":"Fall '89",           "start":"1989-10-08","end":"1989-10-26"},
    # 1990
    {"id":"1990-spring",    "era":"brent",    "name":"Spring '90",         "start":"1990-03-14","end":"1990-04-03"},
    {"id":"1990-summer",    "era":"brent",    "name":"Summer '90",         "start":"1990-07-08","end":"1990-07-19"},
    {"id":"1990-fall",      "era":"brent",    "name":"Fall '90",           "start":"1990-09-07","end":"1990-09-20"},
    # 1991
    {"id":"1991-spring",    "era":"final",    "name":"Spring '91",         "start":"1991-03-17","end":"1991-04-01"},
    {"id":"1991-summer",    "era":"final",    "name":"Summer '91",         "start":"1991-06-01","end":"1991-06-17"},
    {"id":"1991-fall",      "era":"final",    "name":"Fall '91",           "start":"1991-09-06","end":"1991-09-16"},
    # 1992
    {"id":"1992-spring",    "era":"final",    "name":"Spring '92",         "start":"1992-03-09","end":"1992-03-24"},
    {"id":"1992-summer",    "era":"final",    "name":"Summer '92",         "start":"1992-06-06","end":"1992-06-25"},
    {"id":"1992-fall",      "era":"final",    "name":"Fall '92",           "start":"1992-12-06","end":"1992-12-16"},
    # 1993
    {"id":"1993-spring",    "era":"final",    "name":"Spring '93",         "start":"1993-02-21","end":"1993-03-17"},
    {"id":"1993-summer",    "era":"final",    "name":"Summer '93",         "start":"1993-06-11","end":"1993-06-26"},
    {"id":"1993-fall",      "era":"final",    "name":"Fall '93",           "start":"1993-09-22","end":"1993-10-05"},
    # 1994
    {"id":"1994-spring",    "era":"final",    "name":"Spring '94",         "start":"1994-03-21","end":"1994-04-04"},
    {"id":"1994-summer",    "era":"final",    "name":"Summer '94",         "start":"1994-06-13","end":"1994-07-19"},
    {"id":"1994-fall",      "era":"final",    "name":"Fall '94",           "start":"1994-10-01","end":"1994-10-14"},
    # 1995
    {"id":"1995-spring",    "era":"final",    "name":"Spring '95",         "start":"1995-02-19","end":"1995-03-26"},
    {"id":"1995-summer",    "era":"final",    "name":"Summer '95 (Final)", "start":"1995-06-02","end":"1995-07-09"},
]

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
listens_table.create_index([("username", 1), ("show_date", 1)])
try:
    listens_table.create_index([("username", 1), ("session_id", 1)], unique=True, sparse=True)
except Exception:
    app.logger.warning("listens_table: could not create unique (username, session_id) index — pre-existing duplicates detected, falling back to non-unique")
    listens_table.create_index([("username", 1), ("session_id", 1)], sparse=True)
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

@app.route("/api/admin/rename-user", methods=["POST"])
@login_required
def rename_user():
    """Rename a user across all collections. Only the logged-in user can rename themselves,
    or any user can rename another if they know the old username (admin-style for small deployments)."""
    data = request.get_json()
    old_name = (data.get("old_username") or "").strip().lower()
    new_name = (data.get("new_username") or "").strip().lower()
    if not old_name or not new_name:
        return jsonify({"error": "old_username and new_username required"}), 400
    import re
    if not re.match(r'^[a-z0-9_-]+$', new_name):
        return jsonify({"error": "new_username may only contain letters, numbers, _ and -"}), 400
    if users_table.find_one({"username": new_name}):
        return jsonify({"error": "Username already taken"}), 409
    if not users_table.find_one({"username": old_name}):
        return jsonify({"error": "User not found"}), 404
    users_table.update_one({"username": old_name}, {"$set": {"username": new_name}})
    for col in [listens_table, ratings_table, show_ratings_table, notes_table]:
        col.update_many({"username": old_name}, {"$set": {"username": new_name}})
    if session.get("username") == old_name:
        session["username"] = new_name
    return jsonify({"ok": True, "old_username": old_name, "new_username": new_name})

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
        stars = float(data.get("stars", 0))
        stars = round(stars * 2) / 2  # round to nearest 0.5
    except (ValueError, TypeError):
        return jsonify({"error": "stars must be a number"}), 400
    if not track_id or not (0.5 <= stars <= 5):
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
        stars = float(data.get("stars", 0))
        stars = round(stars * 2) / 2
    except (ValueError, TypeError):
        return jsonify({"error": "stars must be a number"}), 400
    if not show_id or not (0.5 <= stars <= 5):
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

@app.route("/api/show-ratings/user/<username>")
def user_show_ratings(username):
    rows = list(show_ratings_table.find({"username": username}, {"_id": 0, "username": 0}).sort("show_id", 1))
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
    except requests.exceptions.Timeout:
        return jsonify({"error": "Archive.org timed out"}), 502
    except requests.exceptions.RequestException as e:
        return jsonify({"error": "Archive.org unavailable"}), 502
    except Exception as e:
        return jsonify({"error": "Unexpected error"}), 500
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
    except requests.exceptions.Timeout:
        return jsonify({"error": "Archive.org timed out"}), 502
    except requests.exceptions.RequestException as e:
        return jsonify({"error": "Archive.org unavailable"}), 502
    except Exception as e:
        return jsonify({"error": "Unexpected error"}), 500
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
    except requests.exceptions.Timeout:
        return jsonify({"error": "Archive.org timed out"}), 502
    except requests.exceptions.RequestException as e:
        return jsonify({"error": "Archive.org unavailable"}), 502
    except Exception as e:
        return jsonify({"error": "Unexpected error"}), 500
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
    except requests.exceptions.Timeout:
        return jsonify({"error": "Archive.org timed out"}), 502
    except requests.exceptions.RequestException as e:
        return jsonify({"error": "Archive.org unavailable"}), 502
    except Exception as e:
        return jsonify({"error": "Unexpected error"}), 500

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
    session_id = data.get("session_id", "")
    username = current_user()
    now = datetime.now(timezone.utc).isoformat()
    if session_id:
        # Upsert: update seconds on existing session, insert if new
        listens_table.update_one(
            {"username": username, "session_id": session_id},
            {"$set": {
                "username":    username,
                "session_id":  session_id,
                "track_id":    data.get("track_id", ""),
                "track_title": data.get("track_title", ""),
                "show_id":     data.get("show_id", ""),
                "show_date":   data.get("show_date", ""),
                "source_id":   data.get("source_id", ""),
                "seconds":     seconds,
                "ts":          now,
            }},
            upsert=True
        )
    else:
        listens_table.insert_one({
            "username":    username,
            "track_id":    data.get("track_id", ""),
            "track_title": data.get("track_title", ""),
            "show_id":     data.get("show_id", ""),
            "show_date":   data.get("show_date", ""),
            "source_id":   data.get("source_id", ""),
            "seconds":     seconds,
            "ts":          now,
        })
    return jsonify({"ok": True})

@app.route("/api/listens/stats")
@login_required
def listen_stats():
    import re as _re
    from collections import defaultdict
    username = current_user()
    year     = request.args.get("year", "")  # optional e.g. "2024"

    query = {"username": username}
    if year:
        query["show_date"] = {"$regex": f"^{year}-"}

    rows = list(listens_table.find(query, {"seconds": 1, "show_date": 1, "show_id": 1, "track_id": 1, "track_title": 1, "_id": 0}))

    total_seconds = sum(r["seconds"] for r in rows)

    all_years_set = set()
    by_show = defaultdict(lambda: {"seconds": 0, "show_date": ""})
    by_track = defaultdict(lambda: {"seconds": 0, "track_title": "", "show_date": ""})

    for r in rows:
        show_date = r.get("show_date") or r.get("show_id") or ""
        yr = show_date[:4]
        if yr.isdigit():
            all_years_set.add(yr)

        # by_show
        by_show[show_date]["seconds"] += r["seconds"]
        by_show[show_date]["show_date"] = show_date

        # by_track
        k = r.get("track_id", r.get("track_title", ""))
        by_track[k]["seconds"] += r["seconds"]
        by_track[k]["track_title"] = r.get("track_title", k)
        by_track[k]["show_date"] = show_date

    all_years = sorted(all_years_set, reverse=True)

    # Top songs: group by normalized title across all shows
    # Keys and values are both in normalized (no-punctuation) form so the lookup
    # works after stripping apostrophes/punctuation from the raw title.
    _ALIASES = {
        "gdtrfb":                         "going down the road feeling bad",
        "going down the road feelin bad": "going down the road feeling bad",
        "goin down the road feeling bad": "going down the road feeling bad",
        "goin down the road feelin bad":  "going down the road feeling bad",
        "nfa":                            "not fade away",
        "pitb":                           "playing in the band",
        "tleo":                           "the other one",
        "st stephen":                     "saint stephen",
        "lovelight":                      "turn on your lovelight",
        "turn on your love light":        "turn on your lovelight",
        "sugar mag":                      "sugar magnolia",
        "truckin":                        "truckin",
        "me  my uncle":                   "me and my uncle",   # "me & my uncle" after stripping &
        "iko":                            "iko iko",
        "china cat":                      "china cat sunflower",
        "its all over now":               "its all over now",
        "death dont have no mercy":       "death dont have no mercy",
        "deaths dont have no mercy":      "death dont have no mercy",
        "death don t have no mercy":      "death dont have no mercy",
    }

    def _norm(title):
        t = (title or "").lower().strip()
        t = _re.sub(r'^[\d\s\.\-]+', '', t)       # strip leading track numbers
        t = _re.sub(r'\s*[-=>]+\s*.*$', '', t)    # strip transitions e.g. "-> Eyes", "> China"
        t = _re.sub(r"[''`&]", '', t)              # remove apostrophes/ampersands
        t = _re.sub(r'[^\w\s]', '', t)             # strip remaining punctuation
        t = _re.sub(r'\s+', ' ', t).strip()
        return _ALIASES.get(t, t)

    by_song = defaultdict(lambda: {"seconds": 0, "shows": set(), "title": ""})
    for r in rows:
        raw = r.get("track_title", "")
        norm = _norm(raw)
        if not norm:
            continue
        show = r.get("show_date") or r.get("show_id") or ""
        by_song[norm]["seconds"] += r["seconds"]
        by_song[norm]["shows"].add(show)
        by_song[norm]["title"] = by_song[norm]["title"] or raw  # keep first seen display title

    top_songs = sorted(
        [{"title": v["title"], "norm": k, "seconds": v["seconds"], "show_count": len(v["shows"])}
         for k, v in by_song.items()],
        key=lambda x: x["show_count"],
        reverse=True,
    )[:20]

    top_shows  = sorted(by_show.values(),  key=lambda x: x["seconds"], reverse=True)[:10]
    top_tracks = sorted(by_track.values(), key=lambda x: x["seconds"], reverse=True)[:10]

    # Available years the user has listens for
    all_years = sorted({
        r.get("show_date", "")[:4]
        for r in listens_table.find({"username": username}, {"show_date": 1, "_id": 0})
        if r.get("show_date", "")[:4].isdigit()
    }, reverse=True)

    # Era bucketing
    ERAS = [
        ("Primal Dead",   1965, 1969),
        ("Anthem Era",    1970, 1972),
        ("Wall of Sound", 1973, 1974),
        ("Comeback",      1976, 1979),
        ("Brent Years",   1980, 1990),
        ("Final Years",   1991, 1995),
    ]
    era_seconds = {name: 0 for name, _, _ in ERAS}
    for r in rows:
        show_date = r.get("show_date") or r.get("show_id") or ""
        yr_str = show_date[:4]
        if not yr_str.isdigit():
            continue
        yr = int(yr_str)
        for name, start, end in ERAS:
            if start <= yr <= end:
                era_seconds[name] += r["seconds"]
                break
    by_era = [{"era": name, "seconds": era_seconds[name]} for name, _, _ in ERAS if era_seconds[name] > 0]

    return jsonify({
        "total_seconds": total_seconds,
        "total_listens": len(rows),
        "top_shows":     top_shows,
        "top_tracks":    top_tracks,
        "top_songs":     top_songs,
        "years":         all_years,
        "year":          year,
        "by_era":        by_era,
    })

# ── Leaderboard ───────────────────────────────────────────────────────────────
@app.route("/api/leaderboard")
@login_required
def leaderboard():
    from collections import defaultdict
    from datetime import datetime, timezone, timedelta
    period = request.args.get("period", "week")
    query = {}
    if period == "week":
        since = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
        query["ts"] = {"$gte": since}
    elif period == "month":
        since = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
        query["ts"] = {"$gte": since}
    rows = list(listens_table.find(query, {"_id": 0}))
    by_user = defaultdict(lambda: {"seconds": 0, "shows": set()})
    for r in rows:
        u = r.get("username", "")
        by_user[u]["seconds"] += r.get("seconds", 0)
        sid = r.get("show_id") or r.get("show_date", "")
        if sid:
            by_user[u]["shows"].add(sid)
    usernames = list(by_user.keys())
    user_docs = {d["username"]: d for d in users_table.find(
        {"username": {"$in": usernames}}, {"username": 1, "display_name": 1, "_id": 0}
    )}
    result = sorted([
        {
            "username": u,
            "display_name": user_docs.get(u, {}).get("display_name") or u,
            "seconds": v["seconds"],
            "shows": len(v["shows"]),
        }
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
    except requests.exceptions.Timeout:
        return jsonify({"error": "Archive.org timed out"}), 502
    except requests.exceptions.RequestException as e:
        return jsonify({"error": "Archive.org unavailable"}), 502
    except Exception as e:
        return jsonify({"error": "Unexpected error"}), 500
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
    except requests.exceptions.Timeout:
        return jsonify({"error": "Archive.org timed out"}), 502
    except requests.exceptions.RequestException as e:
        return jsonify({"error": "Archive.org unavailable"}), 502
    except Exception as e:
        return jsonify({"error": "Unexpected error"}), 500
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
    except requests.exceptions.Timeout:
        return jsonify({"error": "Archive.org timed out"}), 502
    except requests.exceptions.RequestException as e:
        return jsonify({"error": "Archive.org unavailable"}), 502
    except Exception as e:
        return jsonify({"error": "Unexpected error"}), 500
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

# ── Random Show (The Gambler) ─────────────────────────────────────────────────
@app.route("/api/random-show")
def random_show():
    year = request.args.get("year", "").strip()
    cache_key = f"gambler:{year or 'all'}"
    cached = _cache_get(cache_key)
    if cached is not None:
        pool = cached
    else:
        if year:
            q = f"collection:{COLLECTION} AND year:{year}"
        else:
            # Spread across all active GD years
            q = f"collection:{COLLECTION} AND year:[1965 TO 1995]"
        try:
            data = archive_search({
                "q": q,
                "fl[]": "identifier,title,date,coverage,avg_rating,num_reviews",
                "output": "json",
                "rows": 1000,
                "sort[]": "date asc",
            })
        except requests.exceptions.Timeout:
            return jsonify({"error": "Archive.org timed out"}), 502
        except requests.exceptions.RequestException:
            return jsonify({"error": "Archive.org unavailable"}), 502
        except Exception:
            return jsonify({"error": "Unexpected error"}), 500

        docs = data.get("response", {}).get("docs", [])
        seen = {}
        pool = []
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
            avg_rating = doc.get("avg_rating")
            num_reviews = doc.get("num_reviews", 0) or 0
            pool.append({
                "show_id": date_str,
                "show_date": date_str,
                "venue": venue_name or title[:60],
                "location": doc.get("coverage", ""),
                "avg_rating": avg_rating,
                "num_reviews": num_reviews,
            })
        if not pool:
            return jsonify({"error": "No shows found"}), 404
        _cache_set(cache_key, pool)

    # Weighted random pick: use avg_rating as weight if available, else uniform
    weights = []
    for show in pool:
        r = show.get("avg_rating")
        weights.append(float(r) if r is not None else 3.5)

    chosen = random.choices(pool, weights=weights, k=1)[0]
    return jsonify({
        "show_id": chosen["show_id"],
        "show_date": chosen["show_date"],
        "venue": chosen["venue"],
        "location": chosen["location"],
    })

# ── Tour Runs ────────────────────────────────────────────────────────────────
@app.route("/api/tours")
def list_tours():
    runs_by_era = {}
    for t in TOUR_RUNS:
        runs_by_era.setdefault(t["era"], []).append({
            "id": t["id"],
            "name": t["name"],
            "start": t["start"],
            "end": t["end"],
        })
    eras = [
        {"id": e["id"], "name": e["name"], "runs": runs_by_era.get(e["id"], [])}
        for e in TOUR_ERAS
        if e["id"] in runs_by_era
    ]
    return jsonify({"eras": eras})

@app.route("/api/tours/<tour_id>/progress")
@login_required
def tour_progress(tour_id):
    tour = next((t for t in TOUR_RUNS if t["id"] == tour_id), None)
    if not tour:
        return jsonify({"error": "Tour not found"}), 404
    username = current_user()
    date_filter = {"$gte": tour["start"], "$lte": tour["end"]}

    # My progress: distinct show_dates this user has listened to within this tour
    my_heard = listens_table.distinct("show_date", {"username": username, "show_date": date_filter})
    my_heard_set = set(my_heard)

    # Cohort: all users who have at least 1 listen in this tour
    pipeline = [
        {"$match": {"show_date": date_filter}},
        {"$group": {
            "_id": "$username",
            "completed_count": {"$addToSet": "$show_date"},
            "last_show": {"$max": "$show_date"},
        }},
        {"$project": {
            "_id": 1,
            "completed_count": {"$size": "$completed_count"},
            "last_show": 1,
        }},
        {"$sort": {"completed_count": -1}},
        {"$limit": 20},
    ]
    cohort_raw = list(listens_table.aggregate(pipeline))
    usernames_in_cohort = [r["_id"] for r in cohort_raw]
    user_docs = {d["username"]: d for d in users_table.find(
        {"username": {"$in": usernames_in_cohort}},
        {"username": 1, "display_name": 1, "_id": 0}
    )}
    cohort = [
        {
            "display_name": user_docs.get(r["_id"], {}).get("display_name") or r["_id"],
            "completed_count": r["completed_count"],
            "last_show": r["last_show"],
        }
        for r in cohort_raw
    ]

    # Fetch shows in the date range from Archive.org (via per-year cache)
    start_year = int(tour["start"][:4])
    end_year   = int(tour["end"][:4])
    tour_shows = []
    for yr in range(start_year, end_year + 1):
        cache_key = f"shows:{yr}"
        yr_shows = _cache_get(cache_key)
        if yr_shows is None:
            try:
                data = archive_search({
                    "q": f"collection:{COLLECTION} AND year:{yr}",
                    "fl[]": "identifier,title,date,coverage",
                    "output": "json",
                    "rows": 1000,
                    "sort[]": "date asc",
                })
                docs = data.get("response", {}).get("docs", [])
                seen = {}
                yr_shows = []
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
                    yr_shows.append({
                        "id": date,
                        "venue": venue_name or title[:60],
                        "location": doc.get("coverage", ""),
                    })
                _cache_set(cache_key, yr_shows)
            except Exception:
                yr_shows = []
        # yr_shows may be the full shows-endpoint format (dicts with "venue" key as dict)
        # or the compact format above; normalise to date + label
        for s in (yr_shows or []):
            date = s.get("id") or s.get("display_date") or ""
            if date < tour["start"] or date > tour["end"]:
                continue
            if isinstance(s.get("venue"), dict):
                venue = s["venue"].get("name", "")
                location = s["venue"].get("location", "")
            else:
                venue = s.get("venue", "")
                location = s.get("location", "")
            tour_shows.append({"date": date, "venue": venue, "location": location})
    tour_shows.sort(key=lambda x: x["date"])

    return jsonify({
        "tour": {"id": tour["id"], "name": tour["name"], "start": tour["start"], "end": tour["end"]},
        "shows": tour_shows,
        "my_progress": list(my_heard_set),
        "cohort": cohort,
    })

@app.route("/api/tours/<tour_id>/join", methods=["POST"])
@login_required
def join_tour(tour_id):
    tour = next((t for t in TOUR_RUNS if t["id"] == tour_id), None)
    if not tour:
        return jsonify({"error": "Tour not found"}), 404
    return jsonify({"ok": True})

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
