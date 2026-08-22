"""Every number in the rendered prose must trace to a computed fact.

The page is unattended. A number that was true when it was typed and is now
frozen reads exactly like a number that is recomputed each run, and nothing on
the page distinguishes them. This walks the facts the run actually produced,
collects every value they contain, and checks the rendered prose against it.

Numbers that legitimately appear without being facts are declared in
PARAMETERS below — nominal levels, thresholds, display rules. Anything else
that fails to match is a frozen value and fails the run.
"""
import re

# Class (a): fixed parameters. These are page vocabulary, not observations.
# Anything added here is a claim that the value does not depend on the data.
PARAMETERS = {
    90, 99, 10, 100,          # nominal interval levels and their complements
    1, 2, 3, 4, 5, 0,         # small ordinals and counts in fixed phrasing
    1.0,                      # ARI's maximum, a property of the statistic
    12, 24,                   # scheduled slot hours, forward horizon
    2026, 2025, 2024,         # dates in historical statements
}


# Per-point series are excluded from the fact space. Prose quotes summary
# values, never an individual chart point, and 169 points x several fields
# contains nearly every small integer — including them made the check pass
# on anything. Excluding them is what gives it power.
# Only the per-hour series. Breaches, tiers, backtest rows and archetypes are
# all quoted directly in prose and must stay in the fact space.
BULK_KEYS = {"points", "forward", "values"}


def _walk(obj, acc):
    if isinstance(obj, bool) or obj is None:
        return acc
    if isinstance(obj, (int, float)):
        v = float(obj)
        acc.add(round(v, 4))
        acc.add(round(v))                      # display rounding
        acc.add(round(v, 1))
        # Truncation, not just rounding. Templates that render an integer with
        # int() disagree with round() on every .5 — an expected value of 133.5
        # displays as 133 while round() yields 134 under banker's rounding, so
        # the gate rejected a number the page had computed correctly.
        acc.add(float(int(v)))
        if v:                                  # percent and ratio renderings
            acc.add(round(v * 100, 1)); acc.add(round(v * 100))
        return acc
    if isinstance(obj, str):
        for tok in re.findall(r"-?\d[\d,]*\.?\d*", obj):
            try: _walk(float(tok.replace(",", "")), acc)
            except ValueError: pass
        return acc
    if isinstance(obj, dict):
        for k, v in obj.items():
            _walk(k, acc)
            if k in BULK_KEYS and isinstance(v, (list, tuple)):
                continue                       # summary scalars only
            _walk(v, acc)
        return acc
    if isinstance(obj, (list, tuple, set)):
        for v in obj: _walk(v, acc)
    return acc


def check(prose_blocks, facts):
    """Return a list of (block, number) that no fact supports."""
    known = _walk(facts, set()) | {float(p) for p in PARAMETERS}
    bad = []
    for name, text in prose_blocks.items():
        plain = re.sub(r"<[^>]+>", " ", text or "")
        # Detach a unit suffix before tokenising. "9.7x" would otherwise fail the
        # trailing (?![\w/-]) guard on the full decimal, backtrack, and yield a
        # bare "9" — a number the facts never contained, reported against prose
        # that was correct. Narrow on purpose: only a multiplier 'x' glued to a
        # digit, so identifiers like h2 or utf8 stay excluded as before.
        plain = re.sub(r"(?<=\d)x\b", " x", plain)
        for tok in re.findall(r"(?<![\w/-])(\d[\d,]*(?:\.\d+)?)(?![\w/-])", plain):
            try: v = float(tok.replace(",", ""))
            except ValueError: continue
            if round(v, 4) in known or round(v) in known or round(v, 1) in known:
                continue
            bad.append((name, tok))
    return bad
