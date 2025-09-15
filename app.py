import os, time, json
from flask import Flask, request, jsonify
from flask_cors import CORS
import psycopg2
import psycopg2.extras as extras

app = Flask(__name__)
# If you still access it from the browser directly, set CORS_ORIGIN to your Render site.
CORS(app, resources={r"/*": {"origins": os.getenv("CORS_ORIGIN", "*")}})

DB = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME", "postgres"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "")
}
API_KEY = os.getenv("API_KEY")  # must match ET_API_KEY in PHP

def conn():
    # Supabase usually requires SSL
    return psycopg2.connect(
        host=DB["host"],
        port=DB["port"],
        dbname=DB["dbname"],
        user=DB["user"],
        password=DB["password"],
        sslmode=os.getenv("DB_SSLMODE", "require"),
        connect_timeout=10
    )

def require_api_key():
    if not API_KEY:  # allow if not set (dev)
        return None
    key = request.headers.get("X-API-KEY")
    if key != API_KEY:
        return jsonify({"error": "Unauthorized"}), 401
    return None

# Tiny in-memory cache
_cache = {}
def cache_get(k, ttl):
    v = _cache.get(k)
    if not v: return None
    data, ts = v
    if time.time() - ts > ttl: return None
    return data
def cache_set(k, data): _cache[k] = (data, time.time())

@app.route("/health")
def health():
    return jsonify({"ok": True})

# =========================
# Core endpoints
# =========================

@app.route("/top-books")
def top_books():
    auth = require_api_key()
    if auth: return auth
    cached = cache_get("top-books", 60)
    if cached: return jsonify(cached)

    q = """
    SELECT i.id, i.name, COUNT(b.id) AS borrow_count
    FROM borrowings b
    JOIN items i ON i.id = b.item_id
    GROUP BY i.id, i.name
    ORDER BY borrow_count DESC
    LIMIT 5;
    """
    with conn() as c, c.cursor(cursor_factory=extras.DictCursor) as cur:
        cur.execute(q)
        rows = [dict(r) for r in cur.fetchall()]

    cache_set("top-books", rows)
    return jsonify(rows)

@app.route("/recs")
def recs():
    auth = require_api_key()
    if auth: return auth

    user_id = request.args.get("user_id", type=int)
    if not user_id:
        return jsonify({"error": "user_id is required"}), 400

    key = f"recs:{user_id}"
    cached = cache_get(key, 60)
    if cached: return jsonify(cached)

    q_user_items = """
      SELECT DISTINCT b.item_id
      FROM borrowings b
      WHERE b.user_id = %s
    """
    q_recs = """
    WITH user_items AS (
      SELECT DISTINCT b.item_id FROM borrowings b WHERE b.user_id = %(uid)s
    ),
    co AS (
      SELECT b2.item_id AS rec_item, b1.item_id AS reason_item, COUNT(*) AS co_count
      FROM borrowings b1
      JOIN borrowings b2
        ON b1.user_id = b2.user_id
       AND b1.item_id <> b2.item_id
      WHERE b1.item_id IN (SELECT item_id FROM user_items)
      GROUP BY rec_item, reason_item
    ),
    scores AS (
      SELECT rec_item, SUM(co_count) AS score
      FROM co
      GROUP BY rec_item
    ),
    filtered AS (
      SELECT s.rec_item, s.score
      FROM scores s
      WHERE s.rec_item NOT IN (SELECT item_id FROM user_items)
      ORDER BY s.score DESC
      LIMIT 10
    )
    SELECT f.rec_item AS id,
           i.name AS name,
           f.score::int AS score,
           json_agg(
             json_build_object('item_id', c.reason_item, 'title', i2.name, 'count', c.co_count)
             ORDER BY c.co_count DESC
           ) AS reasons
    FROM filtered f
    JOIN co c ON c.rec_item = f.rec_item
    JOIN items i ON i.id = f.rec_item
    JOIN items i2 ON i2.id = c.reason_item
    GROUP BY f.rec_item, i.name, f.score
    ORDER BY f.score DESC
    LIMIT 5;
    """

    with conn() as c, c.cursor(cursor_factory=extras.DictCursor) as cur:
        cur.execute(q_user_items, (user_id,))
        if not cur.fetchall():
            return jsonify([])  # no history → no recs
        cur.execute(q_recs, {"uid": user_id})
        rows = [dict(r) for r in cur.fetchall()]

    # ensure reasons is a list, not JSON text
    for r in rows:
        if isinstance(r.get("reasons"), str):
            r["reasons"] = json.loads(r["reasons"])
        rs = r.get("reasons") or []
        r["because"] = [x.get("title") for x in rs[:2] if isinstance(x, dict)]

    cache_set(key, rows)
    return jsonify(rows)

# =========================
# New report endpoints
# =========================

@app.route("/borrowings-trend")
def borrowings_trend():
    auth = require_api_key()
    if auth: return auth
    days = request.args.get("days", default=30, type=int)
    cache_key = f"borrowings-trend:{days}"
    cached = cache_get(cache_key, 120)
    if cached: return jsonify(cached)

    # Uses approval_date as the "borrowed on" date
    q = """
      SELECT date_trunc('day', approval_date)::date::text AS day,
             COUNT(*)::int AS count
      FROM borrowings
      WHERE approval_date IS NOT NULL
        AND approval_date >= NOW() - make_interval(days => %s)
      GROUP BY day
      ORDER BY day ASC;
    """
    with conn() as c, c.cursor(cursor_factory=extras.DictCursor) as cur:
        cur.execute(q, (days,))
        rows = [dict(r) for r in cur.fetchall()]

    cache_set(cache_key, rows)
    return jsonify(rows)

@app.route("/top-categories")
def top_categories():
    auth = require_api_key()
    if auth: return auth
    cached = cache_get("top-categories", 300)
    if cached: return jsonify(cached)

    q = """
      SELECT COALESCE(NULLIF(TRIM(i.category), ''), 'Uncategorised') AS category,
             COUNT(b.id)::int AS count
      FROM borrowings b
      JOIN items i ON i.id = b.item_id
      GROUP BY category
      ORDER BY count DESC
      LIMIT 6;
    """
    with conn() as c, c.cursor(cursor_factory=extras.DictCursor) as cur:
        cur.execute(q)
        rows = [dict(r) for r in cur.fetchall()]

    cache_set("top-categories", rows)
    return jsonify(rows)

@app.route("/overdue-stats")
def overdue_stats():
    auth = require_api_key()
    if auth: return auth
    cached = cache_get("overdue-stats", 60)
    if cached: return jsonify(cached)

    q = """
      SELECT
        SUM(CASE WHEN b.return_date IS NULL AND b.due_date < NOW() THEN 1 ELSE 0 END)::int AS overdue_now,
        SUM(CASE WHEN b.return_date IS NULL THEN 1 ELSE 0 END)::int AS borrowed_now,
        SUM(CASE WHEN b.return_date >= date_trunc('month', NOW()) THEN 1 ELSE 0 END)::int AS returned_this_month
      AS stats
      FROM borrowings b;
    """
    # The above aliasing style can vary by PG versions; fetch as three columns instead:
    q = """
      SELECT
        SUM(CASE WHEN b.return_date IS NULL AND b.due_date < NOW() THEN 1 ELSE 0 END)::int AS overdue_now,
        SUM(CASE WHEN b.return_date IS NULL THEN 1 ELSE 0 END)::int AS borrowed_now,
        SUM(CASE WHEN b.return_date >= date_trunc('month', NOW()) THEN 1 ELSE 0 END)::int AS returned_this_month
      FROM borrowings b;
    """
    with conn() as c, c.cursor(cursor_factory=extras.DictCursor) as cur:
        cur.execute(q)
        row = cur.fetchone()
        out = dict(row) if row else {"overdue_now": 0, "borrowed_now": 0, "returned_this_month": 0}

    cache_set("overdue-stats", out)
    return jsonify(out)

@app.route("/")
def root():
    return jsonify({
        "service": "LMS Emerging-Tech API",
        "endpoints": [
            "/health",
            "/top-books",
            "/recs?user_id=",
            "/borrowings-trend?days=30",
            "/top-categories",
            "/overdue-stats"
        ]
    })
