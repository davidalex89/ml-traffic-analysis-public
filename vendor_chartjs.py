#!/usr/bin/env python3
"""Download Chart.js into lib/ so the dashboard can be served without a CDN.

    python vendor_chartjs.py

Chart.js is not committed to this repository. It is MIT-licensed and could be,
but vendoring a 200 KB minified bundle means carrying someone else's release
history in our diffs, and it goes stale silently. Fetching it at setup keeps the
version explicit and the upstream project the source of truth:

    https://github.com/chartjs/Chart.js

The page still loads it from your own origin at runtime — this only moves the
copy step from "committed" to "fetched once". Nothing is requested from a third
party when a visitor loads the dashboard.

Run this once after cloning, and again when you change CHART_JS_VERSION. CI runs
it before the render smoke test.
"""

import hashlib
import re
import sys
import urllib.error
import urllib.request
from pathlib import Path

SOURCEMAP_RE = re.compile(rb"\n//# sourceMappingURL=\S+\s*\Z")

CHART_JS_VERSION = "4.5.1"

LIB_DIR = Path(__file__).resolve().parent / "lib"

# jsDelivr serves npm packages verbatim and supports exact-version pinning, so
# the bytes are the published release rather than a moving "latest".
URL = (f"https://cdn.jsdelivr.net/npm/chart.js@{CHART_JS_VERSION}"
       f"/dist/chart.umd.min.js")
TARGET = "chart.umd.min.js"

# Sanity bounds, not integrity. A published Chart.js build is ~200 KB; anything
# far outside that is a captive-portal page or an error body saved as a script,
# which would otherwise sit in lib/ and fail confusingly at runtime.
MIN_BYTES = 100_000
MAX_BYTES = 1_000_000


def main():
    LIB_DIR.mkdir(exist_ok=True)
    dest = LIB_DIR / TARGET

    if dest.exists():
        print(f"{dest.relative_to(Path.cwd())} already present "
              f"({dest.stat().st_size:,} bytes) — delete it to re-fetch")
        return 0

    print(f"fetching Chart.js {CHART_JS_VERSION}")
    print(f"  {URL}")
    try:
        with urllib.request.urlopen(URL, timeout=30) as r:
            body = r.read()
    except urllib.error.URLError as e:
        print(f"\nfailed: {e}\n\n"
              f"Download it manually instead and save it as lib/{TARGET}:\n"
              f"  {URL}\n"
              f"Releases: https://github.com/chartjs/Chart.js/releases/tag/v{CHART_JS_VERSION}",
              file=sys.stderr)
        return 1

    if not (MIN_BYTES <= len(body) <= MAX_BYTES):
        print(f"\nrefusing to save: got {len(body):,} bytes, expected roughly "
              f"{MIN_BYTES:,}–{MAX_BYTES:,}. That is not a Chart.js build.",
              file=sys.stderr)
        return 1
    if b"Chart.js" not in body[:600]:
        print("\nrefusing to save: the file does not carry a Chart.js banner.",
              file=sys.stderr)
        return 1

    # Strip the trailing //# sourceMappingURL comment. The published bundle
    # points at chart.umd.min.js.map, which is not part of the dist file and is
    # not fetched here, so leaving it in means every visitor who opens devtools
    # gets a 404 on a file that was never going to exist. Shipping the map
    # instead would add several hundred KB of debugging data for a minified
    # dependency nobody steps through.
    before = len(body)
    body = SOURCEMAP_RE.sub(b"\n", body)
    if len(body) != before:
        print(f"  stripped sourceMappingURL comment ({before - len(body)} bytes)")

    dest.write_bytes(body)
    print(f"  saved {dest.relative_to(Path.cwd())}  {len(body):,} bytes")
    print(f"  sha256 {hashlib.sha256(body).hexdigest()[:16]}…")
    print("\nChart.js is MIT licensed; its notice is preserved in the file header.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
