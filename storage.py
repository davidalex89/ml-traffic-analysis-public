import sqlite3
from contextlib import contextmanager
from datetime import datetime

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

CREATE TABLE IF NOT EXISTS request_details (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    collected_at TEXT NOT NULL,
    bucket TEXT NOT NULL,
    country TEXT,
    http_method TEXT,
    status_code INTEGER,
    path TEXT,
    request_count INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS ml_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_at TEXT NOT NULL,
    model_name TEXT NOT NULL,
    result_type TEXT NOT NULL,
    result_json TEXT NOT NULL
);

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


def insert_request_details(conn, rows: list[dict]):
    conn.executemany(
        """INSERT OR REPLACE INTO request_details
           (collected_at, bucket, country, http_method, status_code, path, request_count)
           VALUES (:collected_at, :bucket, :country, :http_method, :status_code, :path, :request_count)
        """,
        rows,
    )


def insert_ml_result(conn, model_name: str, result_type: str, result_json: str):
    conn.execute(
        """INSERT INTO ml_results (run_at, model_name, result_type, result_json)
           VALUES (?, ?, ?, ?)""",
        (datetime.utcnow().isoformat(), model_name, result_type, result_json),
    )


def query_all(conn, sql: str, params: tuple = ()) -> list[dict]:
    rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]
