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

from collector import CloudflareCollector
from storage import init_db, get_db, insert_hourly_traffic, insert_firewall_events, insert_request_details
from ml_analysis import TrafficAnalyzer
from dashboard import generate_dashboard

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

    log.info("Fetching adaptive request details...")
    details = c.collect_adaptive_requests(hours_back=25)

    with get_db() as conn:
        if hourly:
            insert_hourly_traffic(conn, hourly)
            log.info("Stored %d hourly traffic rows", len(hourly))
        if fw_events:
            insert_firewall_events(conn, fw_events)
            log.info("Stored %d firewall events", len(fw_events))
        if details:
            insert_request_details(conn, details)
            log.info("Stored %d request detail rows", len(details))

    total = len(hourly) + len(fw_events) + len(details)
    log.info("Collection complete: %d total rows", total)
    return total


def analyze():
    log.info("=== ML Analysis ===")
    init_db()
    analyzer = TrafficAnalyzer(min_rows=5)
    analyzer.run_all()
    log.info("Analysis complete")


def dashboard():
    log.info("=== Dashboard Generation ===")
    path = generate_dashboard()
    log.info("Dashboard ready: %s", path)
    return path


def verify():
    try:
        c = CloudflareCollector(require_zone=True)
    except RuntimeError:
        c = CloudflareCollector(require_zone=False)
    if c.verify_token():
        print("Token is valid!")
        return True
    else:
        print("Token verification FAILED")
        return False


def full_pipeline():
    log.info("Starting full pipeline")
    collect()
    analyze()
    path = dashboard()
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
    if cmd in COMMANDS:
        COMMANDS[cmd]()
    elif cmd is None:
        full_pipeline()
    else:
        print(f"Unknown command: {cmd}")
        print(__doc__)
        sys.exit(1)
