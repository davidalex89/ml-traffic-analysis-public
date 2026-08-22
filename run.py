#!/usr/bin/env python3
"""
ML Traffic Analysis Pipeline
Collects Cloudflare traffic data, runs ML models, generates dashboard.

Usage:
    python run.py              # Full pipeline: collect + analyze + dashboard
    python run.py collect      # Only collect data
    python run.py analyze      # Only run ML analysis
    python run.py dashboard    # Only regenerate dashboard
    python run.py verify       # Verify API token works
"""
import logging
import sys
from datetime import datetime, timezone

from collector import CloudflareCollector
from storage import (
    init_db, get_db, insert_hourly_traffic, insert_firewall_events,
    insert_ml_result,
)
import pipeline_v2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("pipeline")


def collect():
    log.info("=== Data Collection ===")
    init_db()
    c = CloudflareCollector()

    log.info("Fetching hourly traffic...")
    hourly = c.collect_hourly_traffic(hours_back=25)

    log.info("Fetching firewall events...")
    fw_events = c.collect_firewall_events(hours_back=24)

    with get_db() as conn:
        if hourly:
            insert_hourly_traffic(conn, hourly)
            log.info("Stored %d hourly traffic rows", len(hourly))
        if fw_events:
            insert_firewall_events(conn, fw_events)
            log.info("Stored %d firewall events", len(fw_events))

    total = len(hourly) + len(fw_events)
    log.info("Collection complete: %d total rows", total)
    return total


def analyze():
    """Predictive analysis + render, in one pass.

    pipeline_v2 computes the expectation interval, archetypes, calibration and
    backtest, then renders. An earlier descriptive pipeline (K-Means two-pass,
    ARI, hourly Isolation Forest) has been retired and removed.
    """
    init_db()
    log.info("=== Predictive Analysis ===")
    path = pipeline_v2.run()
    log.info("Analysis complete: %s", path)
    return path


def dashboard():
    """Retained for the CLI; rendering now happens inside analyze()."""
    init_db()
    return pipeline_v2.run()


def verify():
    """Check the credentials before scheduling anything.

    The zone id is optional here: a token can be valid while the zone is unset,
    and saying which one is missing is more useful than failing on the pair.
    """
    try:
        c = CloudflareCollector(require_zone=True)
    except RuntimeError:
        c = CloudflareCollector(require_zone=False)
    if c.verify_token():
        print("Token is valid.")
        return True
    print("Token verification FAILED — the token was rejected by Cloudflare.")
    return False


def _record_collection_status(success: bool, error: str | None, rows_collected: int | None):
    """Persist collection outcome so the dashboard can show a visible
    "data may be stale" notice instead of failures being silently absorbed
    and only visible in CI logs."""
    import json
    status = {
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "success": success,
        "error": error,
        "rows_collected": rows_collected,
    }
    with get_db() as conn:
        insert_ml_result(conn, "collection_status", "health_check", json.dumps(status))


def full_pipeline():
    log.info("Starting full pipeline")
    try:
        total_rows = collect()
        _record_collection_status(success=True, error=None, rows_collected=total_rows)
    except Exception as e:
        log.warning("Collection failed (%s), proceeding with cached data", e)
        _record_collection_status(success=False, error=str(e), rows_collected=None)
    path = analyze()
    log.info("Pipeline complete. Dashboard: %s", path)
    return path


COMMANDS = {
    "collect": collect,
    "analyze": analyze,
    "dashboard": dashboard,
    "verify": verify,
}

if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else None
    if cmd is not None and cmd not in COMMANDS:
        print(f"Unknown command: {cmd}")
        print(__doc__)
        sys.exit(1)
    try:
        COMMANDS[cmd]() if cmd else full_pipeline()
    except RuntimeError as e:
        # Missing credentials reach here. `verify` in particular exists to tell
        # someone whether their setup is right, so answering with a stack trace
        # is the wrong output for the one command whose job is to report.
        print(f"\n{e}\n\nSee the Quick start in README.md for how to obtain a "
              f"token and zone id.", file=sys.stderr)
        sys.exit(1)
