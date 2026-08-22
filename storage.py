import sqlite3
from contextlib import contextmanager

import run_clock

from config import DB_PATH


SCHEMA = """
CREATE TABLE IF NOT EXISTS hourly_traffic (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    collected_at TEXT NOT NULL,
    bucket TEXT NOT NULL,
    country TEXT,
    http_method TEXT,
    status_code INTEGER,
    content_type TEXT,
    request_count INTEGER NOT NULL DEFAULT 0,
    bytes_total INTEGER NOT NULL DEFAULT 0,
    threats INTEGER NOT NULL DEFAULT 0,
    unique_visitors INTEGER NOT NULL DEFAULT 0,
    UNIQUE(bucket, country, http_method, status_code)
);

CREATE TABLE IF NOT EXISTS firewall_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    collected_at TEXT NOT NULL,
    event_datetime TEXT NOT NULL,
    action TEXT,
    client_ip TEXT,
    country TEXT,
    host TEXT,
    http_method TEXT,
    request_path TEXT,
    user_agent TEXT,
    rule_id TEXT,
    source TEXT,
    ray_name TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS ml_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_at TEXT NOT NULL,
    model_name TEXT NOT NULL,
    result_type TEXT NOT NULL,
    result_json TEXT NOT NULL
);

-- Compact per-run snapshot of the headline metrics, written every run.
-- Separate from ml_results because the longitudinal layer queries it on every
-- run to compute percentiles, streaks and change detection; keeping it narrow
-- and typed avoids re-parsing large result blobs to answer "what changed".
CREATE TABLE IF NOT EXISTS run_facts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_at TEXT NOT NULL,
    -- Volume of the run: hours evaluated, and how many broke the interval.
    total_buckets INTEGER,
    anomalies_found INTEGER,
    -- Where the database came from this run. A silent cache eviction resets
    -- every longitudinal series here; recording it makes that detectable.
    db_source TEXT,
    -- Build-time gate results, so a regression is visible in the data rather
    -- than only in a job log that ages out.
    gates TEXT,
    -- Scorecard: claimed vs achieved coverage, and mean absolute error.
    coverage_p90 REAL,
    coverage_p99 REAL,
    mae REAL
);

CREATE INDEX IF NOT EXISTS idx_run_facts_run_at ON run_facts(run_at);
CREATE INDEX IF NOT EXISTS idx_hourly_bucket ON hourly_traffic(bucket);
CREATE INDEX IF NOT EXISTS idx_hourly_country ON hourly_traffic(country);
CREATE INDEX IF NOT EXISTS idx_fw_datetime ON firewall_events(event_datetime);
CREATE INDEX IF NOT EXISTS idx_fw_action ON firewall_events(action);
CREATE INDEX IF NOT EXISTS idx_ml_model ON ml_results(model_name, run_at);
"""


@contextmanager
def get_db():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db():
    with get_db() as conn:
        conn.executescript(SCHEMA)
        migrate_run_facts(conn)
        _deduplicate_hourly_traffic(conn)


def _deduplicate_hourly_traffic(conn):
    """Remove duplicate rows caused by NULL values in the old UNIQUE constraint."""
    dupes = conn.execute("""
        SELECT bucket, country, COUNT(*) as cnt
        FROM hourly_traffic
        GROUP BY bucket, COALESCE(country, ''), COALESCE(http_method, ''), COALESCE(status_code, 0)
        HAVING cnt > 1
    """).fetchall()
    if not dupes:
        return
    conn.execute("""
        DELETE FROM hourly_traffic
        WHERE id NOT IN (
            SELECT MIN(id)
            FROM hourly_traffic
            GROUP BY bucket, COALESCE(country, ''), COALESCE(http_method, ''), COALESCE(status_code, 0)
        )
    """)
    conn.execute("""
        UPDATE hourly_traffic
        SET http_method = COALESCE(http_method, ''),
            status_code = COALESCE(status_code, 0),
            country = COALESCE(country, 'Unknown'),
            content_type = COALESCE(content_type, '')
        WHERE http_method IS NULL OR status_code IS NULL OR country IS NULL OR content_type IS NULL
    """)


def insert_hourly_traffic(conn, rows: list[dict]):
    conn.executemany(
        """INSERT OR REPLACE INTO hourly_traffic
           (collected_at, bucket, country, http_method, status_code,
            content_type, request_count, bytes_total, threats, unique_visitors)
           VALUES (:collected_at, :bucket, :country, :http_method, :status_code,
                   :content_type, :request_count, :bytes_total, :threats, :unique_visitors)
        """,
        rows,
    )


def insert_firewall_events(conn, rows: list[dict]):
    conn.executemany(
        """INSERT OR IGNORE INTO firewall_events
           (collected_at, event_datetime, action, client_ip, country,
            host, http_method, request_path, user_agent, rule_id, source, ray_name)
           VALUES (:collected_at, :event_datetime, :action, :client_ip, :country,
                   :host, :http_method, :request_path, :user_agent, :rule_id, :source, :ray_name)
        """,
        rows,
    )


def insert_ml_result(conn, model_name: str, result_type: str, result_json: str):
    conn.execute(
        """INSERT INTO ml_results (run_at, model_name, result_type, result_json)
           VALUES (?, ?, ?, ?)""",
        (run_clock.stamp(), model_name, result_type, result_json),
    )


# Exactly what the pipeline writes. insert_run_facts builds its INSERT from this
# tuple with facts.get(), so a name here that nothing populates is a column of
# permanent NULLs — which is what twenty-six columns from a retired descriptive
# pipeline had become.
RUN_FACTS_COLUMNS = (
    "total_buckets", "anomalies_found", "db_source",
    "gates", "coverage_p90", "coverage_p99", "mae",
)


def migrate_run_facts(conn):
    """Add run_facts columns introduced after the table first shipped.

    The production database is restored from a cache/artifact written by an
    older revision, so CREATE TABLE IF NOT EXISTS alone will not add columns to
    an existing table. Without this, the first post-deploy run would fail on an
    INSERT naming columns the restored table does not have.
    """
    existing = {r[1] for r in conn.execute("PRAGMA table_info(run_facts)")}
    if not existing:
        return
    # Only the live columns. A database written by an older revision keeps its
    # retired columns — SQLite makes dropping one expensive and there is no
    # reason to: nothing names them in an INSERT, so they sit inert. This adds
    # what is missing; it does not try to reshape what is already there.
    coltypes = {
        "total_buckets": "INTEGER", "anomalies_found": "INTEGER",
        "db_source": "TEXT", "gates": "TEXT",
        "coverage_p90": "REAL", "coverage_p99": "REAL", "mae": "REAL",
    }
    for col, typ in coltypes.items():
        if col not in existing:
            conn.execute(f"ALTER TABLE run_facts ADD COLUMN {col} {typ}")


def insert_run_facts(conn, facts: dict):
    """Append this run's headline metrics to run_facts.

    Tolerates missing keys so a partial run — one where the interval built but
    archetypes had too few entities to cluster — still records what it computed
    rather than losing the row.
    """
    cols = ["run_at"] + list(RUN_FACTS_COLUMNS)
    values = [run_clock.stamp()] + [
        facts.get(c) for c in RUN_FACTS_COLUMNS
    ]
    conn.execute(
        f"INSERT INTO run_facts ({', '.join(cols)}) "
        f"VALUES ({', '.join('?' * len(cols))})",
        values,
    )


def query_prior_run_facts(conn, limit: int = 200) -> list[dict]:
    """Prior runs, newest first, for the longitudinal layer."""
    try:
        rows = conn.execute(
            "SELECT * FROM run_facts ORDER BY run_at DESC LIMIT ?", (limit,)
        ).fetchall()
    except sqlite3.OperationalError:
        return []  # table not created yet on a pre-migration database
    return [dict(r) for r in rows]


def query_all(conn, sql: str, params: tuple = ()) -> list[dict]:
    rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]
