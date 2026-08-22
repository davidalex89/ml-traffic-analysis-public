"""The pipeline's single source of "now".

Everything that stamps a run — the `run_facts` row, `ml_results` rows, the
rendered timestamp — reads the clock from here rather than calling
`datetime.now()` directly, so a test can place a run in a chosen twelve-hour
slot and exercise behaviour that only appears across slots.

WHY THIS EXISTS
---------------
Run counting collapses `run_facts` to one row per twelve-hour scheduled slot
(see framing._counted_rows), and hysteresis counts observations over those
counted rows. A guarded branch therefore cannot move by running the pipeline
repeatedly: three executions a minute apart are one slot and one observation.

Before this module, exercising a hysteresis branch locally meant running the
pipeline and then hand-editing `run_at` in the database between runs. That is
a fix applied to an artifact, it had to be re-derived every time, and it could
not be used by an automated test. The clock makes slot simulation a capability
of the pipeline instead: set the clock, run normally, advance, run again.

USAGE
-----
    PIPELINE_FAKE_NOW=2026-08-01T06:00:00Z python run.py analyze

Accepts any ISO-8601 timestamp; a trailing "Z" is understood. Naive values are
read as UTC. Advance it by SLOT_HOURS between runs to simulate consecutive
scheduled slots — `scenarios.py::replay_slots` does exactly that.

SAFETY
------
The override is refused outright when GITHUB_ACTIONS is set. A faked clock in a
scheduled run would write a run_facts row into the wrong slot, silently
corrupting every counter derived from slot history — the streak, the recovery
rate, and every hysteresis guard. The assertion fires at import time and is not
catchable by the pipeline's own broad exception handlers, so a workflow that
somehow carried the variable fails loudly rather than publishing bad history.
"""

import os
from datetime import datetime, timezone

ENV_VAR = "PIPELINE_FAKE_NOW"


def _parse(raw):
    text = raw.strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(
            f"{ENV_VAR}={raw!r} is not an ISO-8601 timestamp") from exc
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _override():
    raw = os.environ.get(ENV_VAR, "").strip()
    if not raw:
        return None
    if os.environ.get("GITHUB_ACTIONS"):
        raise RuntimeError(
            f"{ENV_VAR} is set inside GitHub Actions. A faked clock would file "
            f"this run under the wrong scheduled slot and corrupt every counter "
            f"derived from slot history. Refusing to run.")
    return _parse(raw)


def now():
    """Current UTC time, or the simulated time when the override is set."""
    return _override() or datetime.now(timezone.utc)


def is_simulated():
    return _override() is not None


def stamp():
    """ISO-8601 string, the form every timestamp column stores."""
    return now().isoformat()
