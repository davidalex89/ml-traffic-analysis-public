import json
import logging
from datetime import datetime

from jinja2 import Template

from config import OUTPUT_DIR
from storage import get_db, query_all

log = logging.getLogger(__name__)

DASHBOARD_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ML Traffic Analysis | hiredavid.com</title>
    <link rel="icon" type="image/png" href="https://hiredavid.com/favicon.png">
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
    <style>
        :root {
            --bg-primary: #0f172a;
            --bg-card: #1e293b;
            --bg-card-hover: #334155;
            --text-primary: #f1f5f9;
            --text-secondary: #94a3b8;
            --accent-blue: #3b82f6;
            --accent-green: #10b981;
            --accent-red: #ef4444;
            --accent-amber: #f59e0b;
            --accent-purple: #8b5cf6;
            --border: #334155;
        }

        * { margin: 0; padding: 0; box-sizing: border-box; }

        body {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            background: var(--bg-primary);
            color: var(--text-primary);
            line-height: 1.6;
        }

        .header {
            background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
            border-bottom: 1px solid var(--border);
            padding: 2rem 0;
        }

        .header-inner {
            max-width: 1200px;
            margin: 0 auto;
            padding: 0 2rem;
        }

        .header h1 {
            font-size: 1.75rem;
            font-weight: 700;
            margin-bottom: 0.25rem;
        }

        .header h1 span { color: var(--accent-blue); }

        .header p {
            color: var(--text-secondary);
            font-size: 0.9rem;
        }

        .badge {
            display: inline-block;
            padding: 0.15rem 0.5rem;
            border-radius: 9999px;
            font-size: 0.7rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }

        .badge-blue { background: rgba(59,130,246,0.15); color: var(--accent-blue); }
        .badge-green { background: rgba(16,185,129,0.15); color: var(--accent-green); }
        .badge-red { background: rgba(239,68,68,0.15); color: var(--accent-red); }
        .badge-amber { background: rgba(245,158,11,0.15); color: var(--accent-amber); }

        .container {
            max-width: 1200px;
            margin: 0 auto;
            padding: 2rem;
        }

        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 1rem;
            margin-bottom: 2rem;
        }

        .stat-card {
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: 0.75rem;
            padding: 1.25rem;
            transition: border-color 0.2s;
        }

        .stat-card:hover { border-color: var(--accent-blue); }

        .stat-card .label {
            font-size: 0.75rem;
            color: var(--text-secondary);
            text-transform: uppercase;
            letter-spacing: 0.05em;
            margin-bottom: 0.25rem;
        }

        .stat-card .value {
            font-size: 1.75rem;
            font-weight: 700;
        }

        .stat-card .sub {
            font-size: 0.8rem;
            color: var(--text-secondary);
            margin-top: 0.25rem;
        }

        .section {
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: 0.75rem;
            padding: 1.5rem;
            margin-bottom: 1.5rem;
        }

        .section h2 {
            font-size: 1.1rem;
            font-weight: 600;
            margin-bottom: 0.25rem;
        }

        .section .description {
            font-size: 0.85rem;
            color: var(--text-secondary);
            margin-bottom: 1.25rem;
        }

        .chart-container {
            position: relative;
            height: 300px;
            width: 100%;
            max-width: 100%;
        }

        .section { overflow: hidden; }

        .grid-2 {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 1.5rem;
        }

        @media (max-width: 768px) {
            .grid-2 { grid-template-columns: 1fr; }
        }

        .cluster-card {
            background: var(--bg-primary);
            border: 1px solid var(--border);
            border-radius: 0.5rem;
            padding: 1rem;
            margin-bottom: 0.75rem;
        }

        .cluster-card h3 {
            font-size: 0.95rem;
            font-weight: 600;
            margin-bottom: 0.5rem;
        }

        .cluster-meta {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
            gap: 0.5rem;
            font-size: 0.8rem;
        }

        .cluster-meta .cm-label { color: var(--text-secondary); }
        .cluster-meta .cm-value { font-weight: 600; }

        .table-wrap {
            overflow-x: auto;
            -webkit-overflow-scrolling: touch;
        }

        table {
            width: 100%;
            border-collapse: collapse;
            font-size: 0.85rem;
            min-width: 500px;
        }

        th, td {
            text-align: left;
            padding: 0.6rem 0.75rem;
            white-space: nowrap;
            border-bottom: 1px solid var(--border);
        }

        th {
            color: var(--text-secondary);
            font-weight: 600;
            font-size: 0.75rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }

        tr:hover td { background: rgba(59,130,246,0.05); }

        .anomaly-row { border-left: 3px solid var(--accent-red); }

        .method-tag {
            display: inline-block;
            padding: 0.1rem 0.4rem;
            border-radius: 4px;
            font-size: 0.7rem;
            font-weight: 700;
            font-family: monospace;
        }

        .footer {
            text-align: center;
            padding: 2rem;
            color: var(--text-secondary);
            font-size: 0.8rem;
            border-top: 1px solid var(--border);
        }

        .footer a { color: var(--accent-blue); text-decoration: none; }

        .empty-state {
            text-align: center;
            padding: 3rem 1rem;
            color: var(--text-secondary);
        }

        .empty-state p { margin-bottom: 0.5rem; }

        .pipeline-note {
            background: rgba(59,130,246,0.08);
            border: 1px solid rgba(59,130,246,0.2);
            border-radius: 0.5rem;
            padding: 1rem 1.25rem;
            margin-bottom: 1.5rem;
            font-size: 0.85rem;
            color: var(--text-secondary);
        }

        .pipeline-note strong { color: var(--accent-blue); }
    </style>
</head>
<body>
    <div class="header">
        <div class="header-inner">
            <a href="https://hiredavid.com" style="color: var(--text-secondary); text-decoration: none; font-size: 0.85rem; display: inline-block; margin-bottom: 0.5rem;">&larr; Back to hiredavid.com</a>
            <h1><span>ML</span> Traffic Analysis</h1>
            <p>A practical experiment in applying machine learning to web security</p>
        </div>
    </div>

    <div class="container">

        <div class="section" style="margin-bottom: 1.5rem;">
            <h2>What Is This?</h2>
            <p style="color: var(--text-secondary); font-size: 0.95rem; line-height: 1.8; margin-top: 0.75rem;">
                This is a project for my
                <a href="https://www.sans.org/cyber-security-courses/applied-data-science-machine-learning/" style="color: var(--accent-blue); text-decoration: none;">SEC595: Applied Data Science and Machine Learning for Cybersecurity Professionals</a>
                class with <strong style="color: var(--text-primary);">SANS Institute</strong> &mdash;
                a practical experiment in applying machine learning to real-world security data.
                Every website on the internet receives a constant stream of traffic &mdash; some from real visitors,
                some from bots, scrapers, and automated scanners probing for vulnerabilities. Most of this
                activity goes unnoticed. The question I wanted to answer was simple: <em>can machine learning
                help make sense of it?</em>
            </p>
            <p style="color: var(--text-secondary); font-size: 0.95rem; line-height: 1.8; margin-top: 0.75rem;">
                This page analyzes live traffic data from
                <a href="https://hiredavid.com" style="color: var(--accent-blue); text-decoration: none;">hiredavid.com</a>
                using two core ML techniques:
            </p>
            <ul style="color: var(--text-secondary); font-size: 0.9rem; line-height: 2; margin-top: 0.5rem; padding-left: 1.5rem;">
                <li><strong style="color: var(--text-primary);">K-Means Clustering</strong> &mdash;
                    an unsupervised algorithm that groups traffic into distinct patterns without being told
                    what to look for. It discovers "visitor personas" on its own by analyzing features like
                    geographic origin, request volume, and threat indicators. This is useful because you
                    don't need labeled data &mdash; the model finds structure in raw traffic.</li>
                <li><strong style="color: var(--text-primary);">Isolation Forest</strong> &mdash;
                    an anomaly detection algorithm that learns what "normal" traffic looks like, then flags
                    time periods that deviate significantly from that baseline. Rather than relying on
                    predefined rules, it adapts to the actual patterns of this specific site.</li>
            </ul>
            <p style="color: var(--text-secondary); font-size: 0.95rem; line-height: 1.8; margin-top: 0.75rem;">
                A scheduled pipeline collects traffic analytics from Cloudflare's API every hour, accumulates
                the data over time, retrains the models, and regenerates this page automatically.
                The more data it collects, the smarter the models get.
            </p>
            {% if summary.latest_data %}
            <p style="color: var(--text-secondary); font-size: 0.8rem; margin-top: 1rem;">
                Last updated: <strong style="color: var(--accent-blue);">{{ summary.latest_data }}</strong>
            </p>
            {% endif %}
        </div>

        <!-- Summary Stats -->
        <div class="stats-grid">
            <div class="stat-card">
                <div class="label">Total Requests</div>
                <div class="value">{{ "{:,}".format(summary.total_requests) }}</div>
                <div class="sub">across all collected periods</div>
            </div>
            <div class="stat-card">
                <div class="label">Unique Countries</div>
                <div class="value">{{ summary.unique_countries }}</div>
                <div class="sub">sources of traffic</div>
            </div>
            <div class="stat-card">
                <div class="label">Threats Detected</div>
                <div class="value" style="color: var(--accent-red)">{{ "{:,}".format(summary.total_threats) }}</div>
                <div class="sub">flagged by Cloudflare</div>
            </div>
            <div class="stat-card">
                <div class="label">Firewall Events</div>
                <div class="value" style="color: var(--accent-amber)">{{ "{:,}".format(summary.firewall_events) }}</div>
                <div class="sub">security-related events</div>
            </div>
            <div class="stat-card">
                <div class="label">Data Points</div>
                <div class="value" style="color: var(--accent-purple)">{{ "{:,}".format(summary.data_rows) }}</div>
                <div class="sub">rows in training set</div>
            </div>
        </div>

        <!-- Traffic Over Time -->
        <div class="section">
            <h2>Traffic Volume Over Time</h2>
            <p class="description">Hourly request counts with anomalies highlighted in red</p>
            <div class="chart-container">
                <canvas id="trafficChart"></canvas>
            </div>
        </div>

        <div class="grid-2">
            <!-- Country Distribution -->
            <div class="section">
                <h2>Top Countries by Requests</h2>
                <p class="description">Geographic distribution of traffic sources</p>
                <div class="chart-container">
                    <canvas id="countryChart"></canvas>
                </div>
            </div>

            <!-- Threat by Country -->
            <div class="section">
                <h2>Threat Ratio by Country</h2>
                <p class="description">Percentage of requests flagged as threats per country</p>
                <div class="chart-container">
                    <canvas id="threatChart"></canvas>
                </div>
            </div>
        </div>

        <!-- K-Means Clustering -->
        <div class="section">
            <h2>Visitor Cluster Analysis <span class="badge badge-blue">K-Means</span></h2>
            <p class="description">
                Traffic patterns grouped using K-Means clustering on features:
                country, request volume, bandwidth, and threat count.
                Each cluster represents a distinct "visitor persona."
            </p>
            {% if clusters and clusters.profiles %}
            <div class="grid-2">
                <div>
                    {% for p in clusters.profiles %}
                    <div class="cluster-card">
                        <h3>
                            {% if p.threat_ratio > 0.1 %}
                            <span style="color:var(--accent-red)">&#9888;</span>
                            {% elif p.avg_requests > 100 %}
                            <span style="color:var(--accent-green)">&#9679;</span>
                            {% else %}
                            <span style="color:var(--accent-blue)">&#9679;</span>
                            {% endif %}
                            {{ p.label }}
                        </h3>
                        <div class="cluster-meta">
                            <div>
                                <span class="cm-label">Size</span><br>
                                <span class="cm-value">{{ p.size }} rows</span>
                            </div>
                            <div>
                                <span class="cm-label">Avg Requests</span><br>
                                <span class="cm-value">{{ "%.1f"|format(p.avg_requests) }}</span>
                            </div>
                            <div>
                                <span class="cm-label">Threats</span><br>
                                <span class="cm-value">{{ p.total_threats }}</span>
                            </div>
                            <div>
                                <span class="cm-label">Top Countries</span><br>
                                <span class="cm-value">
                                    {% for c, n in p.top_countries.items() %}{{ c }}{% if not loop.last %}, {% endif %}{% endfor %}
                                </span>
                            </div>
                        </div>
                    </div>
                    {% endfor %}
                </div>
                <div>
                    <div class="chart-container">
                        <canvas id="clusterChart"></canvas>
                    </div>
                </div>
            </div>
            {% else %}
            <div class="empty-state">
                <p>Not enough data for clustering yet.</p>
                <p>The model needs at least 10 data points to form clusters.</p>
            </div>
            {% endif %}
        </div>

        <!-- Anomaly Detection -->
        <div class="section">
            <h2>Anomaly Detection <span class="badge badge-red">Isolation Forest</span></h2>
            <p class="description">
                Hourly traffic buckets scored by an Isolation Forest model.
                Anomalies are time periods where traffic patterns deviate significantly
                from the learned baseline ({{ "%.0f"|format(anomalies.baseline_mean_requests) }} avg requests/hr
                &plusmn; {{ "%.0f"|format(anomalies.baseline_std_requests) }}).
            </p>
            {% if anomalies and anomalies.anomalies %}
            <div class="table-wrap">
            <table>
                <thead>
                    <tr>
                        <th>Time Bucket</th>
                        <th>Requests</th>
                        <th>Bandwidth</th>
                        <th>Threats</th>
                        <th>Countries</th>
                        <th>Anomaly Score</th>
                    </tr>
                </thead>
                <tbody>
                    {% for a in anomalies.anomalies %}
                    <tr class="anomaly-row">
                        <td>{{ a.bucket }}</td>
                        <td>{{ "{:,}".format(a.total_requests) }}</td>
                        <td>{{ "%.1f"|format(a.total_bytes / 1024 / 1024) }} MB</td>
                        <td style="color: var(--accent-red)">{{ a.total_threats }}</td>
                        <td>{{ a.unique_countries }}</td>
                        <td>{{ "%.4f"|format(a.anomaly_score) }}</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
            </div>
            {% else %}
            <div class="empty-state">
                <p>No anomalies detected{% if anomalies %} across {{ anomalies.total_buckets_analyzed }} time buckets{% endif %}.</p>
                <p>This is good — traffic patterns are within normal bounds.</p>
            </div>
            {% endif %}
        </div>

        <!-- Firewall Events -->
        <div class="section">
            <h2>Firewall Event Breakdown</h2>
            <p class="description">Security events captured by Cloudflare WAF (sampled, last 24h window on free plan)</p>
            {% if firewall and firewall.total_events > 0 %}
            <div>
                <h3 style="font-size:0.85rem; margin-bottom:0.75rem; color:var(--text-secondary)">BY ACTION</h3>
                <div class="chart-container" style="height:250px">
                    <canvas id="fwActionChart"></canvas>
                </div>
            </div>
            {% else %}
            <div class="empty-state">
                <p>No firewall events recorded in the current window.</p>
                <p>Events are sampled and only cover the last 24 hours on the free plan.</p>
            </div>
            {% endif %}
        </div>

    </div>

    <div class="footer">
        <p>
            Generated {{ generated_at }} |
            Built with Python, scikit-learn, Cloudflare GraphQL API |
            <a href="https://hiredavid.com">hiredavid.com</a>
        </p>
    </div>

    <script>
    const COLORS = {
        blue: '#3b82f6', green: '#10b981', red: '#ef4444',
        amber: '#f59e0b', purple: '#8b5cf6', cyan: '#06b6d4',
        pink: '#ec4899', indigo: '#6366f1'
    };
    const PALETTE = [COLORS.blue, COLORS.green, COLORS.amber, COLORS.purple, COLORS.cyan, COLORS.pink, COLORS.indigo, COLORS.red];

    Chart.defaults.color = '#94a3b8';
    Chart.defaults.borderColor = '#334155';
    Chart.defaults.font.family = "'Inter', sans-serif";

    // Traffic over time
    const trafficData = {{ traffic_timeseries | tojson }};
    const anomalyBuckets = new Set({{ anomaly_buckets | tojson }});

    if (trafficData.length > 0) {
        new Chart(document.getElementById('trafficChart'), {
            type: 'bar',
            data: {
                labels: trafficData.map(d => d.bucket.replace('T', ' ').substring(5, 16)),
                datasets: [{
                    label: 'Requests',
                    data: trafficData.map(d => d.total_requests),
                    backgroundColor: trafficData.map(d =>
                        anomalyBuckets.has(d.bucket) ? COLORS.red + 'cc' : COLORS.blue + '99'
                    ),
                    borderColor: trafficData.map(d =>
                        anomalyBuckets.has(d.bucket) ? COLORS.red : COLORS.blue
                    ),
                    borderWidth: 1
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { display: false } },
                scales: {
                    y: { beginAtZero: true, grid: { color: '#1e293b' } },
                    x: { grid: { display: false }, ticks: { maxRotation: 45 } }
                }
            }
        });
    }

    // Country chart
    const countryData = {{ country_data | tojson }};
    if (countryData.length > 0) {
        const topN = countryData.slice(0, 10);
        new Chart(document.getElementById('countryChart'), {
            type: 'doughnut',
            data: {
                labels: topN.map(d => d.country),
                datasets: [{
                    data: topN.map(d => d.requests),
                    backgroundColor: PALETTE,
                    borderColor: '#1e293b',
                    borderWidth: 2
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { position: 'right', labels: { boxWidth: 12, padding: 8, font: { size: 11 } } }
                }
            }
        });
    }

    // Threat ratio chart
    const threatCountries = countryData.filter(d => d.threat_ratio > 0).slice(0, 8);
    if (threatCountries.length > 0) {
        new Chart(document.getElementById('threatChart'), {
            type: 'bar',
            data: {
                labels: threatCountries.map(d => d.country),
                datasets: [{
                    label: 'Threat %',
                    data: threatCountries.map(d => d.threat_ratio),
                    backgroundColor: COLORS.red + '99',
                    borderColor: COLORS.red,
                    borderWidth: 1
                }]
            },
            options: {
                indexAxis: 'y',
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { display: false } },
                scales: {
                    x: { beginAtZero: true, grid: { color: '#1e293b' } },
                    y: { grid: { display: false } }
                }
            }
        });
    }

    // Cluster size chart
    const clusterData = {{ cluster_chart_data | tojson }};
    if (clusterData.length > 0) {
        new Chart(document.getElementById('clusterChart'), {
            type: 'bubble',
            data: {
                datasets: clusterData.map((d, i) => ({
                    label: d.label,
                    data: [{ x: d.avg_requests, y: d.total_threats, r: Math.max(Math.sqrt(d.size) * 3, 6) }],
                    backgroundColor: PALETTE[i % PALETTE.length] + '88',
                    borderColor: PALETTE[i % PALETTE.length]
                }))
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { position: 'bottom', labels: { boxWidth: 12, font: { size: 11 } } }
                },
                scales: {
                    x: { title: { display: true, text: 'Avg Requests' }, grid: { color: '#1e293b' } },
                    y: { title: { display: true, text: 'Total Threats' }, beginAtZero: true, grid: { color: '#1e293b' } }
                }
            }
        });
    }

    // Firewall action chart
    const fwActions = {{ fw_actions | tojson }};
    if (Object.keys(fwActions).length > 0) {
        new Chart(document.getElementById('fwActionChart'), {
            type: 'doughnut',
            data: {
                labels: Object.keys(fwActions),
                datasets: [{
                    data: Object.values(fwActions),
                    backgroundColor: [COLORS.red, COLORS.amber, COLORS.blue, COLORS.green, COLORS.purple],
                    borderColor: '#1e293b',
                    borderWidth: 2
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { position: 'bottom', labels: { boxWidth: 12, font: { size: 11 } } } }
            }
        });
    }
    </script>
</body>
</html>
"""


def _get_latest_result(conn, model_name: str) -> dict:
    rows = query_all(conn, """
        SELECT result_json FROM ml_results
        WHERE model_name = ?
        ORDER BY run_at DESC LIMIT 1
    """, (model_name,))
    if rows:
        return json.loads(rows[0]["result_json"])
    return {}


def generate_dashboard():
    """Read ML results from DB and render the static HTML dashboard."""
    with get_db() as conn:
        summary = _get_latest_result(conn, "summary")
        clusters = _get_latest_result(conn, "kmeans_traffic")
        anomalies = _get_latest_result(conn, "isolation_forest")
        countries = _get_latest_result(conn, "geo_analysis")
        firewall = _get_latest_result(conn, "firewall_analysis")

        traffic_ts = query_all(conn, """
            SELECT bucket, SUM(request_count) as total_requests
            FROM hourly_traffic
            GROUP BY bucket
            ORDER BY bucket
        """)

    if not summary:
        summary = {
            "total_requests": 0, "unique_countries": 0, "total_threats": 0,
            "firewall_events": 0, "data_rows": 0, "latest_data": None,
        }
    if not anomalies:
        anomalies = {
            "anomalies": [], "baseline_mean_requests": 0,
            "baseline_std_requests": 0, "total_buckets_analyzed": 0,
        }

    anomaly_buckets = [a["bucket"] for a in anomalies.get("anomalies", [])]
    country_data = countries.get("countries", [])
    cluster_chart = clusters.get("profiles", []) if clusters else []
    fw_actions = firewall.get("by_action", {}) if firewall else {}

    template = Template(DASHBOARD_TEMPLATE)
    html = template.render(
        summary=summary,
        clusters=clusters,
        anomalies=anomalies,
        firewall=firewall,
        traffic_timeseries=traffic_ts,
        anomaly_buckets=anomaly_buckets,
        country_data=country_data,
        cluster_chart_data=cluster_chart,
        fw_actions=fw_actions,
        generated_at=datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
    )

    out_path = OUTPUT_DIR / "index.html"
    out_path.write_text(html, encoding="utf-8")
    log.info("Dashboard written to %s", out_path)
    return out_path
