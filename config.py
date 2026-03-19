import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"

DATA_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)

DB_PATH = DATA_DIR / "traffic.db"
GRAPHQL_ENDPOINT = "https://api.cloudflare.com/client/v4/graphql"


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
