#!/usr/bin/env python3
"""Populate data/traffic.db with synthetic traffic so the pipeline can run.

Two uses:

  python seed_demo_db.py     # then: python run.py dashboard

CI has no Cloudflare credentials, and `pipeline_v2.run()` returns early without
writing anything when there is no traffic to analyse — so a renderer smoke test
needs data to exist first. The same seeder lets anyone who clones this repo see
a working dashboard before deciding whether to wire up a real zone.

Everything here is generated, not sampled. Client addresses come from the
RFC 5737 documentation ranges (192.0.2.0/24, 198.51.100.0/24, 203.0.113.0/24),
which are reserved for exactly this purpose and route nowhere. No real traffic,
no real addresses.

Deterministic: a seeded RNG and a hardcoded end timestamp (see build()), so CI
produces the same database every run and a rendering diff means the renderer
changed rather than the data.
"""

import argparse
import random
from datetime import datetime, timedelta, timezone

from config import DB_PATH
from storage import (
    init_db, get_db, insert_hourly_traffic, insert_firewall_events,
)

# predictive.py needs MIN_HISTORY_HOURS (50) for an interval and twice that for
# the backtest; the quantile window is 14 days. 21 days clears all three with
# room for the display window to look populated rather than truncated.
DAYS = 21
SEED = 20260816

COUNTRIES = ["US", "DE", "CN", "GB", "NL", "SG", "BR", "FR"]
METHODS = ["GET", "GET", "GET", "POST", "HEAD"]
STATUSES = [200, 200, 200, 301, 404, 403, 503]

PROBE_PATHS = [
    "/wp-admin/install.php", "/wp-login.php", "/.env", "/.git/config",
    "/admin/", "/phpmyadmin/", "/xmlrpc.php", "/config.json",
    "/wordpress/wp-admin/setup-config.php", "/.aws/credentials",
]
NORMAL_PATHS = ["/", "/about", "/blog/", "/robots.txt", "/sitemap.xml", "/contact"]

UAS = [
    "Mozilla/5.0 (compatible; ExampleBot/1.0)",
    "python-requests/2.31.0",
    "curl/8.4.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "",  # empty UA is a real and common scanner signature
]


def _docaddr(rng, octet_pool):
    """An address from an RFC 5737 documentation range. Never routable."""
    net = rng.choice(["192.0.2", "198.51.100", "203.0.113"])
    return f"{net}.{rng.choice(octet_pool)}"


def build(days=DAYS, seed=SEED):
    rng = random.Random(seed)
    # Anchor to a fixed point so repeated runs are byte-identical.
    end = datetime(2026, 8, 16, 0, 0, 0, tzinfo=timezone.utc)
    start = end - timedelta(days=days)
    collected = end.strftime("%Y-%m-%dT%H:%M:%SZ")

    hourly, events = [], []

    # A handful of noisy addresses so entity clustering has something with
    # ENTITY_MIN_HITS (20) or more; the rest are long-tail singletons.
    heavy = [_docaddr(rng, range(10, 40)) for _ in range(14)]

    hours = int((end - start).total_seconds() // 3600)
    for h in range(hours):
        t = start + timedelta(hours=h)
        bucket = t.strftime("%Y-%m-%dT%H:%M:%SZ")

        # Diurnal shape plus noise, so the expectation interval has a real
        # pattern to learn rather than flat noise.
        hour_factor = 1.0 + 0.6 * (1 if 8 <= t.hour <= 22 else -0.5)
        base = int(120 * hour_factor * rng.uniform(0.75, 1.25))

        # Occasional burst — this is what produces breaches worth rendering.
        burst = rng.random() < 0.03
        if burst:
            base = int(base * rng.uniform(3.0, 6.0))

        for country in rng.sample(COUNTRIES, k=rng.randint(3, 6)):
            reqs = max(1, int(base * rng.uniform(0.05, 0.35)))
            threats = int(reqs * (rng.uniform(0.5, 0.9) if burst else rng.uniform(0.0, 0.25)))
            hourly.append({
                "collected_at": collected, "bucket": bucket, "country": country,
                "http_method": rng.choice(METHODS), "status_code": rng.choice(STATUSES),
                "content_type": "text/html", "request_count": reqs,
                "bytes_total": reqs * rng.randint(400, 9000), "threats": threats,
                "unique_visitors": max(1, reqs // rng.randint(2, 8)),
            })

        n_events = rng.randint(8, 22) if burst else rng.randint(1, 6)
        for i in range(n_events):
            ip = rng.choice(heavy) if rng.random() < 0.75 else _docaddr(rng, range(40, 250))
            events.append({
                "collected_at": collected,
                "event_datetime": bucket,
                "action": rng.choice(["block", "managed_challenge", "block", "skip"]),
                "client_ip": ip,
                "country": rng.choice(COUNTRIES),
                "host": "example.com",
                "http_method": rng.choice(METHODS),
                "request_path": rng.choice(PROBE_PATHS + NORMAL_PATHS),
                "user_agent": rng.choice(UAS),
                # Managed features report a literal; custom rules report a
                # per-zone id. Both shapes appear so consumers handle each.
                "rule_id": rng.choice(["bot_fight_mode", "waf", "e" * 8 + f"{i:024d}"]),
                "source": rng.choice(["firewallCustom", "botFight", "waf"]),
                "ray_name": f"seed{h:05d}{i:03d}",
            })

    return hourly, events


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--days", type=int, default=DAYS)
    ap.add_argument("--seed", type=int, default=SEED)
    ap.add_argument("--force", action="store_true",
                    help="seed even if the database already holds traffic")
    args = ap.parse_args()

    init_db()
    with get_db() as conn:
        existing = conn.execute("SELECT COUNT(*) FROM hourly_traffic").fetchone()[0]
        if existing and not args.force:
            print(f"traffic.db already has {existing} hourly rows — refusing to seed. "
                  f"Use --force to add synthetic data anyway.")
            return 0

        hourly, events = build(days=args.days, seed=args.seed)
        insert_hourly_traffic(conn, hourly)
        insert_firewall_events(conn, events)

    print(f"seeded {DB_PATH}: {len(hourly)} hourly rows, "
          f"{len(events)} firewall events "
          f"({args.days} days, seed {args.seed})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
