"""Analysis layer for the traffic dashboard.

Produces what Cloudflare's own dashboard does not: a calibrated expectation
interval, breaches measured against it, behavioral archetypes learned from
per-IP feature vectors, and the model's own scored accuracy.

The page states a calibrated range rather than drawing a forecast line. Which
estimator earns that range is not assumed — `run_backtest()` scores several
candidates against held-out history on every run and reports which won, so
whether a given zone's traffic has learnable structure is answered from that
zone's own data rather than decided here.
"""

import json
import logging
import math
import os
import statistics as st
from collections import Counter, defaultdict
from datetime import datetime, timedelta

import numpy as np
from sklearn.cluster import KMeans
from sklearn.metrics import (adjusted_rand_score, normalized_mutual_info_score,
                             silhouette_score)
from sklearn.preprocessing import StandardScaler

from storage import insert_ml_result, query_all

log = logging.getLogger(__name__)

# --- Expectation interval -------------------------------------------------

# Trailing window the quantiles are computed over. 14 days measured best
# calibration without lagging real level shifts.
QUANTILE_WINDOW_DAYS = 14
MIN_HISTORY_HOURS = 50          # below this the interval is not trustworthy
DISPLAY_WINDOW_DAYS = 7         # observed history shown on the hero chart
HORIZON_HOURS = 24              # forward extent of the expected range

TIERS = (("p90", 0.05, 0.95), ("p99", 0.005, 0.995))

# --- Entity archetypes ----------------------------------------------------

ENTITY_MIN_HITS = 20            # below this a per-IP feature vector is noise
# k adapts to the data but will not move on noise.
#
# Choosing k by plain argmax silhouette can decide the cluster count on a margin
# far smaller than the spread between candidates, which means a handful of new
# addresses flips it and the section reports a structural change that did not
# happen. So k still comes from the data each run, but the incumbent only gives
# way when a rival beats it by ENTITY_K_MARGIN.
#
# 0.03 was chosen by replaying the sweep across several hundred refits of one
# zone's history and comparing margins on how often k changed against worst-case
# membership stability. It is a starting value, not a derived constant: the right
# margin depends on how many entities a zone sees and how stable they are. If
# your cluster count flaps between runs, raise it; if a real structural change
# takes several runs to show up, lower it. The run that moves k says so on the
# page, which is the signal to tune this.
ENTITY_K_RANGE = (2, 6)
ENTITY_K_MARGIN = 0.03
# A cluster this small is not a behavior, it is a handful of addresses. When the
# fit produces one, those addresses are removed and the model is refitted once
# without them; they are then reported by name rather than dressed up as a group.
# A guard rather than a routine step — it should fire rarely, and firing often is
# a sign the feature vector has an unscaled dimension one address can dominate.
# One pass only: a second sub-floor cluster after the refit is accepted, so the
# loop cannot chase its own tail. On a low-volume zone the guard declines to act
# at all rather than strip the data down below what the cluster count needs.
ENTITY_MIN_CLUSTER = 10
# Addresses to drop before clustering — typically your own, since an operator
# browsing their own site accumulates enough hits to rank as a top entity and
# crowd out a real one. Configure via ML_EXCLUDED_IPS as a comma-separated list;
# empty by default. Deliberately not hardcoded: an IP here would be published
# with the source, and it is personal infrastructure rather than configuration.
EXCLUDED_IPS = {
    ip.strip() for ip in os.environ.get("ML_EXCLUDED_IPS", "").split(",") if ip.strip()
}

PROBE_FAMILIES = (
    ("WordPress", ("wp-", "wordpress/")),
    ("env/secrets", (".env", ".aws", "secrets", "credentials")),
    ("config", ("config", "settings", ".ini", "properties")),
    ("vcs", (".git", ".svn", ".hg")),
    ("admin", ("admin", "phpmyadmin", "cpanel", "webmail")),
    ("rpc", ("xmlrpc", "jsonrpc")),
)


def _mask_ip(ip: str) -> str:
    """Drop the final octet/group of an address.

    The exemplar exists to make an archetype concrete — "this many paths in one
    hour" — and a masked address does that just as well. IP addresses are
    personal data under GDPR, so if you publish the dashboard, this is what keeps
    an individual visitor from being identifiable in it. Removing the mask would
    make that untrue.
    """
    if not ip:
        return ip
    if ":" in ip:                       # IPv6 — mask the last group
        head, _, _ = ip.rpartition(":")
        return f"{head}:x" if head else "x"
    parts = ip.split(".")
    return ".".join(parts[:-1] + ["x"]) if len(parts) == 4 else ip


def _fmt_hour(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _hourly_series(conn):
    rows = conn.execute("""
        SELECT bucket, SUM(request_count) AS req, SUM(threats) AS thr
        FROM hourly_traffic GROUP BY bucket ORDER BY bucket
    """).fetchall()
    return [(datetime.strptime(r["bucket"], "%Y-%m-%dT%H:%M:%SZ"),
             int(r["req"] or 0), int(r["thr"] or 0)) for r in rows]


def build_expectation(conn):
    """Rolling-quantile expectation interval, breaches, and forward range.

    Quantiles come from the trailing window of OBSERVED hours only. Missing
    hours are skipped rather than zero-filled. Collection gaps are normal — a
    failed run, an expired cache, a rate limit — and imputing zero for them
    drags the lower bound down and manufactures breaches that never happened.
    """
    series = _hourly_series(conn)
    if len(series) < MIN_HISTORY_HOURS:
        return {"available": False, "reason": f"only {len(series)} observed hours"}

    by_hour = {dt: (q, t) for dt, q, t in series}
    last = series[-1][0]
    display_start = last - timedelta(days=DISPLAY_WINDOW_DAYS)

    points, breaches = [], []
    tier95_count = 0
    widths = []

    cur = display_start
    while cur <= last:
        obs = by_hour.get(cur)
        window = [q for dt, q, _ in series
                  if cur - timedelta(days=QUANTILE_WINDOW_DAYS) <= dt < cur]
        if len(window) >= MIN_HISTORY_HOURS:
            centre = float(np.median(window))
            bounds = {name: (float(np.quantile(window, lo)), float(np.quantile(window, hi)))
                      for name, lo, hi in TIERS}
            widths.append(bounds["p90"][1] - bounds["p90"][0])
            rec = {
                "t": _fmt_hour(cur),
                "expected": round(centre, 1),
                "lo90": round(bounds["p90"][0], 1), "hi90": round(bounds["p90"][1], 1),
                "lo99": round(bounds["p99"][0], 1), "hi99": round(bounds["p99"][1], 1),
                "observed": obs[0] if obs else None,
                "threats": obs[1] if obs else None,
            }
            if obs:
                o = obs[0]
                if o > bounds["p99"][1] or o < bounds["p99"][0]:
                    rec["breach"] = "p99"
                    breaches.append({
                        "t": rec["t"], "observed": o, "expected": rec["expected"],
                        "lo": rec["lo99"], "hi": rec["hi99"], "tier": "p99",
                        "threats": obs[1],
                        "threat_pct": round(100 * obs[1] / o, 1) if o else 0.0,
                    })
                elif o > bounds["p90"][1] or o < bounds["p90"][0]:
                    rec["breach"] = "p90"
                    tier95_count += 1
            points.append(rec)
        cur += timedelta(hours=1)

    # Forward expected range. Flat by construction: with no seasonality there
    # is nothing to vary it by, and pretending otherwise would be decoration.
    recent = [q for dt, q, _ in series if dt >= last - timedelta(days=QUANTILE_WINDOW_DAYS)]
    forward = []
    if len(recent) >= MIN_HISTORY_HOURS:
        centre = float(np.median(recent))
        b = {n: (float(np.quantile(recent, lo)), float(np.quantile(recent, hi)))
             for n, lo, hi in TIERS}
        for h in range(1, HORIZON_HOURS + 1):
            forward.append({
                "t": _fmt_hour(last + timedelta(hours=h)),
                "expected": round(centre, 1),
                "lo90": round(b["p90"][0], 1), "hi90": round(b["p90"][1], 1),
                "lo99": round(b["p99"][0], 1), "hi99": round(b["p99"][1], 1),
                "observed": None,
            })

    breaches.sort(key=lambda b: b["t"], reverse=True)
    return {
        "available": True,
        "points": points,
        "forward": forward,
        "now": _fmt_hour(last),
        "breaches": breaches,
        "tier95_only": tier95_count,
        "median_width90": round(float(np.median(widths)), 0) if widths else None,
        "window_days": QUANTILE_WINDOW_DAYS,
        "display_days": DISPLAY_WINDOW_DAYS,
        "horizon_hours": HORIZON_HOURS,
    }


def score_calibration(conn, days=30):
    """Empirical coverage against nominal, plus MAE. The credibility feature.

    Coverage is the honest metric here; MAPE is not usable when the median hour
    carries 9 requests, because near-zero denominators dominate it.
    """
    series = _hourly_series(conn)
    if len(series) < MIN_HISTORY_HOURS * 2:
        return {"available": False}
    last = series[-1][0]
    start = last - timedelta(days=days)

    hits = {n: 0 for n, _, _ in TIERS}
    n_eval = 0
    abs_err = []
    for i, (dt, q, _) in enumerate(series):
        if dt < start:
            continue
        window = [v for d, v, _ in series
                  if dt - timedelta(days=QUANTILE_WINDOW_DAYS) <= d < dt]
        if len(window) < MIN_HISTORY_HOURS:
            continue
        n_eval += 1
        abs_err.append(abs(q - float(np.median(window))))
        for name, lo, hi in TIERS:
            if np.quantile(window, lo) <= q <= np.quantile(window, hi):
                hits[name] += 1

    if not n_eval:
        return {"available": False}
    tiers = []
    for name, lo, hi in TIERS:
        nominal = (hi - lo) * 100
        empirical = 100 * hits[name] / n_eval
        tiers.append({
            "tier": name, "nominal": round(nominal, 1),
            "empirical": round(empirical, 1),
            "delta": round(empirical - nominal, 1),
            "within_2pp": abs(empirical - nominal) <= 2.0,
        })
    return {
        "available": True, "tiers": tiers, "n_evaluated": n_eval,
        "mae": round(float(np.mean(abs_err)), 1), "days": days,
    }


def run_backtest(conn, days=30, include_ml=True):
    """Rolling-origin comparison of candidate methods. Evidence for T5.

    Runs every run, not once. The page asserts that seasonal and ML candidates
    lose to a rolling median; that verdict has to be re-earned against current
    data or it is just a stale opinion in a table.

    Cost on the production series (~2,900 observed hours, 700 scored): the
    median candidates are ~0.5 s total. The gradient-boosting candidate adds
    ~2-4 s because it refits once on the pre-test span rather than per step —
    a genuine one-step-ahead refit would be ~700 fits and is not worth the
    runtime for a candidate that is being tested for rejection. Pass
    include_ml=False to skip it if the runner ever becomes a constraint.
    """
    series = _hourly_series(conn)
    if len(series) < 200:
        return {"available": False}
    last = series[-1][0]
    start = last - timedelta(days=days)
    idx = {dt: i for i, (dt, _, _) in enumerate(series)}

    def window_before(i, d):
        cutoff = series[i][0] - timedelta(days=d)
        return [v for dt, v, _ in series[:i] if dt >= cutoff]

    cands = {
        "Rolling median, 14d": lambda i: st.median(window_before(i, 14)) if window_before(i, 14) else None,
        "Hour-of-day median": None,
        "Hour x day-of-week median": None,
        "Naive (previous hour)": lambda i: series[i - 1][1] if i else None,
    }
    errs = defaultdict(list)
    for i, (dt, actual, _) in enumerate(series):
        if dt < start or i < 24 * 14:
            continue
        hist = [(d, v) for d, v, _ in series[:i] if d >= dt - timedelta(days=28)]
        if len(hist) < MIN_HISTORY_HOURS:
            continue
        preds = {
            "Rolling median, 14d": st.median([v for d, v in hist if d >= dt - timedelta(days=14)] or [0]),
            "Hour-of-day median": st.median([v for d, v in hist if d.hour == dt.hour] or [0]),
            "Hour x day-of-week median": st.median(
                [v for d, v in hist if d.hour == dt.hour and d.weekday() == dt.weekday()] or [0]),
            "Naive (previous hour)": series[i - 1][1],
        }
        for k, p in preds.items():
            errs[k].append(abs(actual - p))

    if not errs:
        return {"available": False}

    # Gradient boosting over lag features. Included so the page's claim that it
    # loses is a measured result each run rather than a remembered one.
    if include_ml:
        try:
            from sklearn.ensemble import HistGradientBoostingRegressor
            L = [1, 2, 3, 24, 168]
            X, y, T = [], [], []
            for i, (dt, v, _) in enumerate(series):
                if i < 168:
                    continue
                lags = [series[i - k][1] for k in L]
                X.append([math.log1p(z) for z in lags] + [dt.hour, dt.weekday()])
                y.append(math.log1p(v)); T.append(dt)
            X, y, T = np.array(X), np.array(y), np.array(T)
            mask = T >= start
            if mask.sum() > 20 and (~mask).sum() > 100:
                m = HistGradientBoostingRegressor(max_iter=200, random_state=42)
                m.fit(X[~mask], y[~mask])
                pred = np.expm1(m.predict(X[mask]))
                errs["Gradient boosting (lags + hour + dow)"] = list(
                    np.abs(np.expm1(y[mask]) - pred))
        except Exception as exc:  # noqa: BLE001 — a failed candidate must not fail the run
            log.warning("Backtest: GBM candidate skipped (%s)", exc)

    n = max(len(v) for v in errs.values())
    rows = [{"method": k, "mae": round(float(np.mean(v)), 1), "n": len(v)}
            for k, v in errs.items()]
    rows.sort(key=lambda r: r["mae"])
    best = rows[0]["mae"]
    # The winner is whichever candidate scored lowest on THIS zone's data.
    #
    # This used to be a hardcoded method name, with the others labelled against
    # it as an "incumbent". That was a conclusion carried over from one specific
    # site: on any other traffic the section could rank a candidate first and
    # then mark a worse one selected, which is the opposite of what a section
    # titled "model selection evidence" should do.
    #
    # No hysteresis, deliberately. The flag is a label on a table — the interval
    # itself is always empirical quantiles and does not consult this — so there
    # is no decision to stabilise. Runs are twelve hours apart against a 28-day
    # window and share almost all their data, so ranks only swap when two
    # candidates sit within noise of each other, and in that case which one
    # carries the badge does not matter because they perform the same.
    for i, r in enumerate(rows):
        r["selected"] = (i == 0)
        if r["selected"]:
            r["verdict"] = "selected"
            r["reason"] = f"lowest error over {n:,} scored hours"
        else:
            r["verdict"] = "rejected"
            r["reason"] = (f"{r['mae']} against {best} for the selected "
                           f"candidate (+{round(r['mae'] - best, 1)})")
    return {"available": True, "rows": rows, "n_evaluated": n, "days": days,
            "window_days": 28, "included_ml": include_ml}


# --- Behavioral archetypes ----------------------------------------------

def _family_of(path):
    p = (path or "").lower()
    for name, needles in PROBE_FAMILIES:
        if any(nd in p for nd in needles):
            return name
    return "other"


def build_archetypes(conn):
    """K-means over per-IP behavioral feature vectors.

    Features are chosen to separate how an actor behaves, not how much it does:
    request count, path diversity, temporal span, and probe-family concentration.
    Validation claim is that known structure (single-day broad scanners vs
    persistent narrow pollers) is recovered without being told it exists.
    """
    rows = conn.execute("""
        SELECT client_ip, request_path, event_datetime
        FROM firewall_events WHERE client_ip IS NOT NULL
    """).fetchall()
    per = defaultdict(lambda: {"paths": set(), "times": [], "fam": Counter(), "n": 0})
    for r in rows:
        ip = r["client_ip"]
        if ip in EXCLUDED_IPS:
            continue
        e = per[ip]
        e["n"] += 1
        e["paths"].add(r["request_path"])
        e["fam"][_family_of(r["request_path"])] += 1
        try:
            e["times"].append(datetime.strptime(r["event_datetime"][:19], "%Y-%m-%dT%H:%M:%S"))
        except (ValueError, TypeError):
            pass

    ips, feats, meta = [], [], []
    for ip, e in per.items():
        if e["n"] < ENTITY_MIN_HITS or not e["times"]:
            continue
        span_h = max((max(e["times"]) - min(e["times"])).total_seconds() / 3600.0, 1.0)
        diversity = len(e["paths"]) / e["n"]
        probe = 1.0 - (e["fam"].get("other", 0) / e["n"])
        ips.append(ip)
        # Four features, all on comparable scales. A fifth — the raw request
        # rate, hits/span_h — used to sit here and was removed. Unlogged beside
        # two logged magnitudes it was skewed enough that, after standardising, a
        # large share of its variance sat in a single address. K-means minimizes
        # squared Euclidean distance, so one point that far out pulls a centroid
        # onto itself, which is what produced the tiny clusters the sub-floor
        # filter below kept having to clear.
        # It is dropped rather than logged because log(hits/span) is exactly
        # log(hits) - log(span), so a logged rate carries no information the
        # first and third features do not already span.
        feats.append([math.log1p(e["n"]), diversity, math.log1p(span_h), probe])
        meta.append({"ip": ip, "hits": e["n"], "paths": len(e["paths"]),
                     "span_h": span_h, "diversity": diversity, "probe": probe,
                     "fam": e["fam"].most_common(3),
                     "first": min(e["times"]), "last": max(e["times"])})
    if len(ips) < 12:
        return {"available": False, "reason": f"only {len(ips)} IPs above the {ENTITY_MIN_HITS}-hit floor"}

    X = StandardScaler().fit_transform(np.array(feats))
    sweep = {}
    for k in range(ENTITY_K_RANGE[0], min(ENTITY_K_RANGE[1], len(ips) - 1) + 1):
        lab = KMeans(n_clusters=k, random_state=42, n_init=10).fit_predict(X)
        if len(set(lab)) > 1:
            sweep[k] = (float(silhouette_score(X, lab)), lab)
    if not sweep:
        return {"available": False, "reason": "no usable clustering"}

    def _fit(Z, k):
        lab = KMeans(n_clusters=k, random_state=42, n_init=10).fit_predict(Z)
        return lab, (float(silhouette_score(Z, lab)) if len(set(lab)) > 1 else 0.0)

    leader = max(sweep, key=lambda k: sweep[k][0])
    incumbent = _previous_k(conn)
    if incumbent is None or incumbent not in sweep:
        best_k, k_moved = leader, False
    elif sweep[leader][0] - sweep[incumbent][0] > ENTITY_K_MARGIN:
        best_k, k_moved = leader, leader != incumbent
    else:
        best_k, k_moved = incumbent, False
    best_s, best_lab = sweep[best_k]
    k_margin = round(sweep[leader][0] - sweep[best_k][0], 4)

    # Drop addresses that landed in a sub-floor cluster and refit once.
    outliers = []
    counts = Counter(int(l) for l in best_lab)
    strays = {c for c, n in counts.items() if n < ENTITY_MIN_CLUSTER}
    if strays and len(ips) - sum(counts[c] for c in strays) > best_k * ENTITY_MIN_CLUSTER:
        drop = {i for i, l in enumerate(best_lab) if int(l) in strays}
        outliers = [{"ip": meta[i]["ip"], "hits": meta[i]["hits"],
                     "paths": meta[i]["paths"], "span_h": round(meta[i]["span_h"], 1)}
                    for i in sorted(drop, key=lambda i: -meta[i]["hits"])]
        ips = [v for i, v in enumerate(ips) if i not in drop]
        meta = [v for i, v in enumerate(meta) if i not in drop]
        X = StandardScaler().fit_transform(
            np.array([f for i, f in enumerate(feats) if i not in drop]))
        best_k = min(best_k, len(ips) - 1)
        best_lab, best_s = _fit(X, best_k)

    groups = defaultdict(list)
    for m, l in zip(meta, best_lab):
        groups[int(l)].append(m)

    archetypes = []
    for cid, members in groups.items():
        med_div = st.median([m["diversity"] for m in members])
        med_span = st.median([m["span_h"] for m in members])
        med_hits = st.median([m["hits"] for m in members])
        med_probe = st.median([m["probe"] for m in members])
        fam = Counter()
        for m in members:
            for f, c in m["fam"]:
                fam[f] += c
        top_fam = [f for f, _ in fam.most_common(3) if f != "other"] or ["unclassified"]

        # Name composed from the three axes that actually separate the
        # clusters. A single coarse rule collapsed six distinct groups into
        # three repeated labels, which tells the reader nothing.
        breadth = ("broad" if med_div > 0.6 else
                   "narrow" if med_div < 0.2 else "mixed-path")
        tempo = ("single-pass" if med_span < 48 else
                 "persistent" if med_span > 24 * 14 else "intermittent")
        intent = ("probe" if med_probe > 0.5 else
                  "crawl" if med_probe < 0.2 else "mixed")
        noun = {"probe": "prober", "crawl": "crawler", "mixed": "requester"}[intent]
        name = f"{breadth.capitalize()} {tempo} {noun}"
        exemplar = max(members, key=lambda m: m["hits"])
        archetypes.append({
            "cluster": cid, "name": name, "n_ips": len(members),
            "median_diversity": round(med_div, 3),
            "median_span_hours": round(med_span, 1),
            "median_hits": int(med_hits),
            "probe_share": round(med_probe, 3),
            "families": top_fam,
            "exemplar": {"ip": _mask_ip(exemplar["ip"]), "hits": exemplar["hits"],
                         "paths": exemplar["paths"],
                         "span_h": round(exemplar["span_h"], 1)},
        })
    archetypes.sort(key=lambda a: -a["n_ips"])
    # Guarantee uniqueness even if two clusters land on the same three axes,
    # qualifying by the feature that still separates them.
    #
    # That qualifier used to be a band — high-volume above 150 median requests,
    # low-volume below 60 — with a bare "#1"/"#2" for anything in between. Two
    # clusters at 132 and 34 requests therefore published as "(#1)" and
    # "(low-volume)": one label meaningless, the pair not comparable. The volume
    # that separates two otherwise identical clusters is whatever it happens to
    # be on the day, so the qualifier is now the measured value rather than a
    # band it has to fall into. Rank is the last resort, for medians that tie
    # exactly and so genuinely have nothing else to separate them.
    seen = Counter(a["name"] for a in archetypes)
    for name, count in seen.items():
        if count < 2:
            continue
        tied = sorted((a for a in archetypes if a["name"] == name),
                      key=lambda a: -a["median_hits"])
        distinct = len({a["median_hits"] for a in tied}) == len(tied)
        for rank, a in enumerate(tied, 1):
            qual = (f"median {a['median_hits']:,} requests" if distinct
                    else f"{rank} of {count} by volume")
            a["name"] = f"{name} ({qual})"

    # Stable accent per archetype, assigned by rank so colors do not shuffle
    # between runs when cluster ids change.
    # Four accents, cycled. Amber and red are reserved for caution and breach
    # states elsewhere on the page; reusing them here would imply an archetype
    # is a warning, which none of them are.
    palette = ["#8f7fe8", "#2bcbba", "#4fa3f7", "#9b8bc4"]
    for i, a in enumerate(archetypes):
        a["accent"] = palette[i % len(palette)]

    # ── Scorecard: grade the clustering the way T4 grades the interval model ──
    # k no longer moves, so the question is whether the *membership* holds and
    # whether the fitted structure is telling us anything the WAF did not.
    membership = {ip: int(l) for ip, l in zip(ips, best_lab)}
    # Total hits over the clustered population. If neither this nor the address
    # set moved between runs, the fit is deterministic and ARI is 1.0 by
    # construction — a number the scorecard must not present as a result.
    fingerprint = sum(m["hits"] for m in meta)
    stability = _archetype_stability(conn, membership, fingerprint)
    waf = _archetype_vs_waf(conn, ips, best_lab)
    _save_archetype_membership(conn, membership, fingerprint)

    return {"available": True, "archetypes": archetypes, "k": best_k,
            "silhouette": round(float(best_s), 3), "n_ips": len(ips),
            "min_hits": ENTITY_MIN_HITS, "excluded": sorted(EXCLUDED_IPS),
            "smallest": min(a["n_ips"] for a in archetypes),
            "outliers": outliers, "min_cluster": ENTITY_MIN_CLUSTER,
            "k_margin": k_margin, "k_moved": k_moved, "k_incumbent": incumbent,
            "k_threshold": ENTITY_K_MARGIN,
            "k_sweep": {k: round(v[0], 3) for k, v in sorted(sweep.items())},
            "stability": stability, "waf": waf}


def _previous_k(conn):
    """The cluster count the last run settled on, or None on a cold start."""
    rows = query_all(conn, """SELECT result_json FROM ml_results
                              WHERE model_name = 'archetype_membership'
                              ORDER BY run_at DESC LIMIT 1""")
    if not rows:
        return None
    try:
        raw = json.loads(rows[0]["result_json"])
    except (ValueError, TypeError):
        return None
    prev = _membership_of(raw)[0]
    return len(set(prev.values())) or None


def _membership_of(raw):
    """(labels, fingerprint) from a stored row, in either storage format.

    Rows written before the fingerprint was added are a flat ip -> label map and
    carry no fingerprint, so the no-op check simply does not fire against them.
    """
    if isinstance(raw, dict) and "labels" in raw:
        return raw["labels"], raw.get("fingerprint")
    return raw, None


def _save_archetype_membership(conn, membership, fingerprint):
    """Persist this run's assignment so the next run can measure churn."""
    insert_ml_result(conn, "archetype_membership", "membership",
                     json.dumps({"labels": membership, "fingerprint": fingerprint}))


def _archetype_stability(conn, membership, fingerprint):
    """Membership churn against the previous run (ARI over shared addresses).

    ARI is permutation-invariant, so it measures whether the same addresses stay
    grouped together — not whether they kept the same cluster id.

    Guarded against reporting a tautology: when no new addresses arrived and the
    hit total is unchanged, the model is being refitted on identical input and
    scores 1.0 because k-means is seeded deterministically. That is a property of
    the pipeline, not evidence the grouping is stable, so it is reported as such.
    """
    rows = query_all(conn, """SELECT result_json FROM ml_results
                              WHERE model_name = 'archetype_membership'
                              ORDER BY run_at DESC LIMIT 1""")
    if not rows:
        return {"available": False, "reason": "no previous run to compare"}
    try:
        raw = json.loads(rows[0]["result_json"])
    except (ValueError, TypeError):
        return {"available": False, "reason": "previous membership unreadable"}
    prev, prev_fp = _membership_of(raw)
    shared = sorted(set(prev) & set(membership))
    if len(shared) < 10:
        return {"available": False, "reason": f"only {len(shared)} shared addresses"}
    # Compared by ARI only. A raw "how many changed cluster id" count looks
    # precise and is not: k-means ids are arbitrary, so a run that groups every
    # address identically but numbers the clusters differently would report
    # 100% moved. ARI is invariant to that relabelling.
    ari = adjusted_rand_score([prev[i] for i in shared], [membership[i] for i in shared])
    new = len(set(membership) - set(prev))
    unchanged = new == 0 and prev_fp is not None and prev_fp == fingerprint
    return {"available": True, "ari": round(float(ari), 3), "shared": len(shared),
            "new": new, "informative": not unchanged}


def _archetype_vs_waf(conn, ips, labels):
    """Independent validation: do the behavioral clusters reproduce the WAF?

    The recovery test asks whether the fit contains profiles we described in
    advance, so it can only ever confirm our own thresholds. This compares the
    clustering against a label produced by a different system entirely. A LOW
    score is the good outcome — it means the behavior model is finding
    structure the rules engine is not already encoding.
    """
    rows = query_all(conn, """SELECT client_ip, action FROM firewall_events
                              WHERE client_ip IS NOT NULL AND action IS NOT NULL""")
    acts = defaultdict(Counter)
    for r in rows:
        acts[r["client_ip"]][r["action"]] += 1
    dom = {ip: (acts[ip].most_common(1)[0][0] if acts.get(ip) else None) for ip in ips}
    pairs = [(i, d) for i, d in enumerate(dom.values()) if d]
    if len(pairs) < 20:
        return {"available": False}
    vocab = sorted({d for _, d in pairs})
    idx = [i for i, _ in pairs]
    y = [vocab.index(d) for _, d in pairs]
    lab = [int(labels[i]) for i in idx]
    ari = adjusted_rand_score(y, lab)
    nmi = normalized_mutual_info_score(y, lab)

    # Behavioral twins: same cluster, different WAF action from their peers.
    by_cluster = defaultdict(list)
    for i in idx:
        by_cluster[int(labels[i])].append(list(dom)[i])
    twins, purities = 0, []
    for members in by_cluster.values():
        c = Counter(dom[ip] for ip in members)
        top = c.most_common(1)[0]
        purities.append(top[1] / len(members))
        twins += len(members) - top[1]
    return {"available": True, "ari": round(float(ari), 3), "nmi": round(float(nmi), 3),
            "twins": twins, "labeled": len(pairs),
            "mean_purity": round(sum(purities) / len(purities), 2),
            "actions": len(vocab)}
