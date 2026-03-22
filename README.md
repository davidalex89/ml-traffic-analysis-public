# ML Traffic Analysis

Machine learning applied to real Cloudflare traffic data — a practical application
of [SEC595](https://www.sans.org/cyber-security-courses/applied-data-science-machine-learning/) (Applied Data Science & ML for Cybersecurity Professionals) concepts.

I built this for my personal site ([hiredavid.com/ml](https://hiredavid.com/ml/)),
but it can be repurposed for any Cloudflare-protected domain.

## What It Does

A scheduled pipeline that:

1. **Collects** traffic analytics from Cloudflare's GraphQL API every 6 hours
   (HTTP stats, firewall events, request patterns)
2. **Stores** accumulated data in a local SQLite database, building a growing
   dataset over time
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

# Set credentials as environment variables
export CF_API_TOKEN="your-cloudflare-api-token"
export CF_ZONE_ID="your-cloudflare-zone-id"

# Run the full pipeline (collect → analyze → dashboard)
python run.py

# Or run individual stages
python run.py collect     # pull data from Cloudflare
python run.py analyze     # run ML models on existing data
python run.py dashboard   # regenerate the HTML dashboard
```

The generated dashboard appears at `output/index.html`.

## Automated Collection via GitHub Actions

The included workflow (`.github/workflows/collect.yml`) runs the pipeline every
6 hours. Set these GitHub repository secrets:

- `CF_API_TOKEN` — Cloudflare API token with Analytics Read permission
- `CF_ZONE_ID` — Cloudflare Zone ID

The workflow persists the SQLite database via GitHub Actions cache and can deploy
the dashboard however you prefer — FTP, GitHub Pages, S3, etc. See the workflow
file for examples.

## Project Structure

```
├── run.py              # Pipeline orchestrator (entry point)
├── collector.py        # Cloudflare GraphQL API data collection
├── storage.py          # SQLite database schema and operations
├── ml_analysis.py      # scikit-learn ML models (K-Means, Isolation Forest)
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

## How the ML Works

**K-Means Clustering** (unsupervised): We choose the input features — geographic
origin, request volume, bandwidth, and Cloudflare's threat flags — but the
algorithm discovers the groupings on its own. No pre-classified training examples
are needed; it finds natural clusters in the feature space. Summary stats use all
accumulated data; detailed charts use a rolling 7-day window.

**Isolation Forest** (anomaly detection): Trained on a rolling 7-day window of
hourly traffic data. It learns what "normal" looks like and flags time periods
that deviate significantly from that baseline — useful for spotting DDoS attempts,
scraping waves, or unusual traffic patterns.

## SEC595 Concepts Applied

- **Unsupervised learning** (K-Means) for pattern discovery
- **Anomaly detection** (Isolation Forest) for identifying outliers
- **Feature engineering** from raw network/HTTP data
- **Data pipeline** design for continuous ML on streaming security data
