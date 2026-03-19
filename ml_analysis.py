import json
import logging
from collections import Counter

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler, LabelEncoder

from storage import get_db, query_all, insert_ml_result

log = logging.getLogger(__name__)


class TrafficAnalyzer:
    """SEC595-style ML analysis on accumulated Cloudflare traffic data."""

    def __init__(self, min_rows: int = 10):
        self.min_rows = min_rows

    def run_all(self):
        """Run every available analysis and store results."""
        with get_db() as conn:
            self._cluster_traffic_patterns(conn)
            self._detect_anomalies(conn)
            self._analyze_country_distribution(conn)
            self._analyze_firewall_events(conn)
            self._compute_summary_stats(conn)

    def _cluster_traffic_patterns(self, conn):
        """K-means clustering on hourly traffic features to find visitor 'personas'."""
        rows = query_all(conn, """
            SELECT bucket,
                   country,
                   COALESCE(request_count, 0) as request_count,
                   COALESCE(bytes_total, 0) as bytes_total,
                   COALESCE(threats, 0) as threats
            FROM hourly_traffic
            WHERE country IS NOT NULL
            ORDER BY bucket
        """)
        if len(rows) < self.min_rows:
            log.info("Not enough traffic rows for clustering (%d)", len(rows))
            return

        df = pd.DataFrame(rows)

        le_country = LabelEncoder()
        df["country_enc"] = le_country.fit_transform(df["country"].fillna("Unknown"))

        features = df[["country_enc", "request_count", "bytes_total", "threats"]].values
        scaler = StandardScaler()
        features_scaled = scaler.fit_transform(features)

        n_clusters = min(max(2, len(df) // 20), 6)
        kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
        df["cluster"] = kmeans.fit_predict(features_scaled)

        cluster_profiles = []
        for c in range(n_clusters):
            mask = df["cluster"] == c
            subset = df[mask]
            top_countries = subset["country"].value_counts().head(3).to_dict()
            profile = {
                "cluster_id": int(c),
                "size": int(mask.sum()),
                "avg_requests": float(subset["request_count"].mean()),
                "avg_bytes": float(subset["bytes_total"].mean()),
                "total_threats": int(subset["threats"].sum()),
                "top_countries": top_countries,
                "threat_ratio": float(
                    subset["threats"].sum() / max(subset["request_count"].sum(), 1)
                ),
            }
            cluster_profiles.append(profile)

        cluster_profiles.sort(key=lambda x: x["avg_requests"], reverse=True)
        for i, p in enumerate(cluster_profiles):
            if p["threat_ratio"] > 0.1:
                p["label"] = "Suspicious Traffic"
            elif p["avg_requests"] > df["request_count"].quantile(0.75):
                p["label"] = "High-Volume Visitors"
            elif len(p["top_countries"]) == 1:
                p["label"] = f"Concentrated ({list(p['top_countries'].keys())[0]})"
            else:
                p["label"] = f"Cluster {i + 1}"

        result = {
            "n_clusters": n_clusters,
            "total_rows_analyzed": len(df),
            "inertia": float(kmeans.inertia_),
            "profiles": cluster_profiles,
        }
        insert_ml_result(conn, "kmeans_traffic", "cluster_profiles", json.dumps(result))
        log.info("Clustering complete: %d clusters from %d rows", n_clusters, len(df))

    def _detect_anomalies(self, conn):
        """Isolation Forest on hourly request volumes to flag unusual traffic."""
        rows = query_all(conn, """
            SELECT bucket,
                   SUM(request_count) as total_requests,
                   SUM(bytes_total) as total_bytes,
                   SUM(threats) as total_threats,
                   COUNT(DISTINCT country) as unique_countries
            FROM hourly_traffic
            GROUP BY bucket
            ORDER BY bucket
        """)
        if len(rows) < self.min_rows:
            log.info("Not enough hourly buckets for anomaly detection (%d)", len(rows))
            return

        df = pd.DataFrame(rows)
        features = df[["total_requests", "total_bytes", "total_threats", "unique_countries"]].values

        scaler = StandardScaler()
        features_scaled = scaler.fit_transform(features)

        contamination = min(0.1, 2.0 / len(df))
        iso_forest = IsolationForest(
            contamination=contamination, random_state=42, n_estimators=100
        )
        df["anomaly_score"] = iso_forest.fit_predict(features_scaled)
        df["anomaly_raw_score"] = iso_forest.decision_function(features_scaled)

        anomalies = df[df["anomaly_score"] == -1].copy()
        anomaly_list = []
        for _, row in anomalies.iterrows():
            anomaly_list.append({
                "bucket": row["bucket"],
                "total_requests": int(row["total_requests"]),
                "total_bytes": int(row["total_bytes"]),
                "total_threats": int(row["total_threats"]),
                "unique_countries": int(row["unique_countries"]),
                "anomaly_score": float(row["anomaly_raw_score"]),
            })

        mean_requests = float(df["total_requests"].mean())
        std_requests = float(df["total_requests"].std()) if len(df) > 1 else 0.0

        result = {
            "total_buckets_analyzed": len(df),
            "anomalies_found": len(anomaly_list),
            "contamination_rate": float(contamination),
            "baseline_mean_requests": mean_requests,
            "baseline_std_requests": std_requests,
            "anomalies": anomaly_list,
        }
        insert_ml_result(conn, "isolation_forest", "anomaly_detection", json.dumps(result))
        log.info("Anomaly detection complete: %d anomalies from %d buckets", len(anomaly_list), len(df))

    def _analyze_country_distribution(self, conn):
        """Country-level traffic breakdown for geographic visualization."""
        rows = query_all(conn, """
            SELECT country,
                   SUM(request_count) as total_requests,
                   SUM(bytes_total) as total_bytes,
                   SUM(threats) as total_threats
            FROM hourly_traffic
            WHERE country IS NOT NULL
            GROUP BY country
            ORDER BY total_requests DESC
        """)
        if not rows:
            return

        total = sum(r["total_requests"] for r in rows)
        country_data = []
        for r in rows:
            country_data.append({
                "country": r["country"],
                "requests": int(r["total_requests"]),
                "bytes": int(r["total_bytes"]),
                "threats": int(r["total_threats"]),
                "pct": round(r["total_requests"] / max(total, 1) * 100, 2),
                "threat_ratio": round(
                    r["total_threats"] / max(r["total_requests"], 1) * 100, 4
                ),
            })

        result = {
            "total_countries": len(country_data),
            "total_requests": int(total),
            "countries": country_data[:50],
        }
        insert_ml_result(conn, "geo_analysis", "country_distribution", json.dumps(result))
        log.info("Country analysis complete: %d countries", len(country_data))

    def _analyze_firewall_events(self, conn):
        """Breakdown of firewall events by action, source, country, and path."""
        rows = query_all(conn, """
            SELECT action, country, source, request_path, http_method, user_agent
            FROM firewall_events
            ORDER BY event_datetime DESC
        """)
        if not rows:
            log.info("No firewall events to analyze")
            insert_ml_result(conn, "firewall_analysis", "event_breakdown", json.dumps({
                "total_events": 0,
                "by_action": {},
                "by_source": {},
                "top_countries": [],
                "top_paths": [],
                "top_user_agents": [],
            }))
            return

        df = pd.DataFrame(rows)
        by_action = df["action"].value_counts().to_dict()
        by_source = df["source"].value_counts().to_dict()
        top_countries = df["country"].value_counts().head(10).to_dict()
        top_paths = df["request_path"].value_counts().head(15).to_dict()

        ua_counts = df["user_agent"].value_counts().head(10)
        top_uas = [{"ua": ua, "count": int(c)} for ua, c in ua_counts.items()]

        result = {
            "total_events": len(df),
            "by_action": {k: int(v) for k, v in by_action.items()},
            "by_source": {k: int(v) for k, v in by_source.items()},
            "top_countries": {k: int(v) for k, v in top_countries.items()},
            "top_paths": {k: int(v) for k, v in top_paths.items()},
            "top_user_agents": top_uas,
        }
        insert_ml_result(conn, "firewall_analysis", "event_breakdown", json.dumps(result))
        log.info("Firewall analysis complete: %d events", len(df))

    def _compute_summary_stats(self, conn):
        """Overall summary statistics for the dashboard header."""
        traffic = query_all(conn, """
            SELECT COUNT(*) as row_count,
                   MIN(bucket) as earliest,
                   MAX(bucket) as latest,
                   SUM(request_count) as total_requests,
                   SUM(bytes_total) as total_bytes,
                   SUM(threats) as total_threats,
                   COUNT(DISTINCT country) as unique_countries
            FROM hourly_traffic
        """)
        fw = query_all(conn, "SELECT COUNT(*) as event_count FROM firewall_events")

        result = {
            "data_rows": int(traffic[0]["row_count"]) if traffic else 0,
            "earliest_data": traffic[0]["earliest"] if traffic else None,
            "latest_data": traffic[0]["latest"] if traffic else None,
            "total_requests": int(traffic[0]["total_requests"] or 0) if traffic else 0,
            "total_bytes": int(traffic[0]["total_bytes"] or 0) if traffic else 0,
            "total_threats": int(traffic[0]["total_threats"] or 0) if traffic else 0,
            "unique_countries": int(traffic[0]["unique_countries"] or 0) if traffic else 0,
            "firewall_events": int(fw[0]["event_count"]) if fw else 0,
        }
        insert_ml_result(conn, "summary", "stats", json.dumps(result))
        log.info("Summary stats computed")
