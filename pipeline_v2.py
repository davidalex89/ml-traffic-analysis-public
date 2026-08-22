"""Pipeline entry point: models -> render.

The page reports measurements and the models' scores against them. It does not
interpret them, so there is no narrative layer here — every sentence on the
output is a fixed caption describing method, with computed values substituted in
and checked by the provenance gate at build time.
"""

import json
import logging
import os
import statistics as st
from pathlib import Path

import run_clock
import dashboard_v2 as V
import predictive as P
from config import OUTPUT_DIR, BASE_DIR, RUN_HISTORY_PATH
from storage import get_db, insert_run_facts

log = logging.getLogger(__name__)


def run():
    with get_db() as conn:
        series = P._hourly_series(conn)
        if not series:
            log.error("No hourly traffic; nothing to analyse")
            return None

        expectation = P.build_expectation(conn)
        calibration = P.score_calibration(conn)
        backtest = P.run_backtest(conn)
        archetypes = P.build_archetypes(conn)

        median_hour = int(st.median([q for _, q, _ in series]))

        tiers = {t["tier"]: t for t in (calibration.get("tiers") or [])}
        breaches = expectation.get("breaches", [])

        runs = [dict(r) for r in conn.execute(
            "SELECT * FROM run_facts ORDER BY run_at")]
        path = V.render(
            expectation=expectation, calibration=calibration, backtest=backtest,
            archetypes=archetypes, run_series=runs, median_hour=median_hour,
            out_dir=Path(OUTPUT_DIR), asset_src=Path(BASE_DIR))

        insert_run_facts(conn, {
            "db_source": os.environ.get("ML_DB_SOURCE", "unknown"),
            "total_buckets": calibration.get("n_evaluated"),
            "anomalies_found": len(breaches),
            "mae": calibration.get("mae"),
            "coverage_p90": tiers.get("p90", {}).get("empirical"),
            "coverage_p99": tiers.get("p99", {}).get("empirical"),
            "gates": json.dumps(V.LAST_GATES),
        })
        # Git-tracked per-run record; see RUN_HISTORY_PATH in config.py for why
        # it is committed rather than left in the cache.
        with open(RUN_HISTORY_PATH, "a", encoding="utf-8") as fh:
            fh.write(json.dumps({
                # Same clock as the run_facts row above. Calling datetime.now()
                # here instead meant one run wrote two different timestamps,
                # which is the thing run_clock exists to prevent.
                "run_at": run_clock.now().isoformat(timespec="seconds"),
                "coverage_p90": tiers.get("p90", {}).get("empirical"),
                "coverage_p99": tiers.get("p99", {}).get("empirical"),
                "mae": calibration.get("mae"),
                "p99_breaches": len(breaches),
                "p90_only": expectation.get("tier95_only"),
                "median_hour": median_hour,
                "archetype_k": archetypes.get("k"),
            }) + "\n")
        log.info("RUN_HISTORY_OK: appended to %s", RUN_HISTORY_PATH)

        log.info("Pipeline complete: %d breach(es), MAE %s, coverage p90 %s / p99 %s",
                 len(breaches), calibration.get("mae"),
                 tiers.get("p90", {}).get("empirical"),
                 tiers.get("p99", {}).get("empirical"))
        return path
