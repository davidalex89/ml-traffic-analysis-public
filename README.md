# ML Traffic Analysis for hiredavid.com

Machine learning applied to real Cloudflare traffic data — a practical application
of SEC595 (Applied Data Science & ML for Cybersecurity Professionals) concepts.

## What It Does

A scheduled pipeline that:

1. **Collects** traffic analytics from Cloudflare's GraphQL API (4x/day HTTP stats,
   firewall events, request patterns)
2. **Stores** accumulated data in a local SQLite database, building a growing dataset
   over time
3. **Analyzes** the data using scikit-learn ML models:
   - **K-Means Clustering** — groups traffic into "visitor personas" based on
     country, volume, bandwidth, and threat indicators
   - **Isolation Forest** — flags anomalous traffic periods that deviate from
     learned baselines
4. **Generates** a static HTML dashboard with Chart.js visualizations

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Set up credentials (and store them securely)

# Verify your token works

# Run the full pipeline

# Or run individual stages
python run.py collect     # only pull data from Cloudflare
python run.py analyze     # only run ML models on existing data
python run.py dashboard   # only regenerate the HTML dashboard
```

The generated dashboard appears at `output/ml_traffic_analysis.html`.

## Automated Collection via GitHub Actions

The included workflow (`.github/workflows/collect.yml`) runs the pipeline hourly,
accumulating data over time. Set these GitHub repository secrets:

- `CF_API_TOKEN` — Cloudflare API token
- `CF_ZONE_ID` — Cloudflare Zone ID

The workflow persists the SQLite database as a GitHub Actions artifact and commits
the updated dashboard to `docs/index.html` (which can be served via GitHub Pages or elsewhere).

## Project Structure

```
├── run.py              # Pipeline orchestrator (entry point)
├── collector.py        # Cloudflare GraphQL API data collection
├── storage.py          # SQLite database schema and operations
├── ml_analysis.py      # scikit-learn ML models
├── dashboard.py        # Jinja2 HTML dashboard generator
├── config.py           # Configuration and secret loading
├── requirements.txt    # Python dependencies
├── .github/workflows/  # GitHub Actions automation
├── data/               # SQLite database (gitignored)
└── output/             # Generated dashboard HTML (gitignored)
```

## Cloudflare Free Plan Limitations

- Firewall events are **sampled** and only cover the **last 24 hours**
- No bot score data (Business+ only)
- No raw log access (Enterprise only)
- Hourly aggregated traffic data has longer retention

The pipeline works around these limits by collecting frequently and accumulating
its own historical dataset.

## SEC595 Concepts Applied

- **Unsupervised learning** (K-Means) for pattern discovery without labels
- **Anomaly detection** (Isolation Forest) for identifying outliers
- **Feature engineering** from raw network/HTTP data
- **Data pipeline** design for continuous ML on streaming security data
