import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"

DATA_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)

DB_PATH = DATA_DIR / "traffic.db"
# Append-only JSONL committed to git — survives CI cache eviction (unlike
# traffic.db). If you add a schedule to collect.yml, the commit it produces each
# run is also what keeps the repository active enough that GitHub does not
# auto-disable that schedule at 60 days.
RUN_HISTORY_PATH = DATA_DIR / "run_history.jsonl"
GRAPHQL_ENDPOINT = "https://api.cloudflare.com/client/v4/graphql"

# Hours between runs, as reported on the dashboard ("every Nh"). The page states
# this rather than repeating it in prose, so it has to be kept in step with
# however you actually run the pipeline — the cron in collect.yml if you add one,
# or your own scheduler.
#
# It is a claim the page makes about itself, not something the code measures, so
# nothing will catch it drifting. Overridable via ML_SLOT_HOURS so a fork can set
# it without editing this file.
SLOT_HOURS = int(os.environ.get("ML_SLOT_HOURS", "12") or 12)


def _load_secret(env_var: str, file_name: str) -> str:
    value = os.environ.get(env_var, "").strip()
    if value:
        return value
    secret_file = BASE_DIR / file_name
    if secret_file.exists():
        first_line = secret_file.read_text().strip().split("\n")[0].strip()
        return first_line
    raise RuntimeError(
        f"Set {env_var} env var or create {file_name} in project root"
    )


def get_api_token() -> str:
    return _load_secret("CF_API_TOKEN", "cf_token.txt")


def get_zone_id() -> str:
    return _load_secret("CF_ZONE_ID", "cf_zone.txt")


def get_dashboard_site_url() -> str:
    """Public site origin for dashboard links (configure via env)."""
    url = os.environ.get("DASHBOARD_SITE_URL", "https://example.com").strip().rstrip("/")
    if url and not url.startswith(("http://", "https://")):
        url = "https://" + url
    return url


def get_dashboard_site_name() -> str:
    """Short site label shown in titles and headers."""
    return os.environ.get("DASHBOARD_SITE_NAME", "example.com").strip() or "example.com"
