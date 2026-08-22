"""Renderer for the redesigned predictive dashboard.

Five sections, each tagged with the test it represents. Every visual shows
model output; nothing replots Cloudflare data for its own sake.

Design: a violet accent over violet-biased neutrals, using system font stacks.
Nothing is bundled and nothing is fetched — the output is HTML, CSS and JS.

Site identity comes from config (DASHBOARD_SITE_NAME / DASHBOARD_SITE_URL) and
is injected into the template — it is not written into this file.
"""

import json
import logging
import re
import shutil
from datetime import timedelta
from pathlib import Path

from jinja2 import Template

import run_clock
from config import get_dashboard_site_name, get_dashboard_site_url

log = logging.getLogger(__name__)

# Outcome of the build-time gates from the most recent render(), so the job
# summary can report them. A failing gate raises instead of writing here.
LAST_GATES: dict = {}

TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="robots" content="noindex, nofollow">
<title>Traffic analysis — {{ site_name }}</title>
{# A browser requests /favicon.ico on its own whenever no icon is declared, so
   the 404 comes from the browser guessing rather than from the page asking.
   Declaring one stops the request. Inline SVG rather than a file: nothing extra
   to copy into output/ or lose on deploy, no second network round trip, and it
   scales to every size a tab or bookmark asks for. The motif is the page's own
   subject — a series breaking out of its expected band. #}
<link rel="icon" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'%3E%3Crect width='32' height='32' rx='7' fill='%2314161f'/%3E%3Crect x='4' y='12' width='24' height='10' rx='2' fill='%238f7fe8' opacity='.28'/%3E%3Cpath d='M4 20 L9 17 L13 21 L17 7 L21 18 L25 14 L28 19' fill='none' stroke='%238f7fe8' stroke-width='2.2' stroke-linecap='round' stroke-linejoin='round'/%3E%3C/svg%3E">
<link rel="stylesheet" href="dashboard.css?v={{ cache_bust }}">
</head>
<body>
<div class="wrap">

  <a href="{{ site_url }}" class="back-link">&larr; {{ site_name }}</a>

  <header class="head">
    <div>
      <h1>Traffic analysis</h1>
      <p class="sub">Calibrated intervals, learned structure, and self-scored models on Cloudflare traffic</p>
    </div>
    <div class="meta">
      <div>{{ generated_at }} UTC</div>
      <div class="muted">every {{ params.slot_hours }}h</div>
    </div>
  </header>

  <p class="intro">
    Every figure below is computed from this zone's own Cloudflare analytics on each
    run, and the models are scored against what happened next.
  </p>

  <!-- ── T1/T2 ───────────────────────────────────────────────── -->
  <section class="sec">
    <div class="sec-head"><h2>Expected range</h2><span class="tag">T1/T2</span></div>
    {% if expectation.available %}
    <div class="chart-hero"><canvas id="rangeChart"></canvas></div>
    <p class="cap">
      Median {{ params.tier_lo }}% interval spans <strong>{{ expectation.median_width90|int }} requests</strong> around a
      median hour of <strong>{{ median_hour }}</strong>. Right of the divider is the next
      {{ expectation.horizon_hours }} hours of expected range, carried forward from the trailing
      {{ expectation.window_days }}-day distribution. The vertical axis is logarithmic: hourly volume
      spans three decades, and a linear axis flattens everything below the bursts.
    </p>
    {% else %}
    <div class="empty">Not enough observed history to compute an interval ({{ expectation.reason }}).</div>
    {% endif %}
  </section>

  <!-- ── T2 ──────────────────────────────────────────────────── -->
  <section class="sec">
    <div class="sec-head"><h2>Interval breaches</h2><span class="tag">T2</span></div>
    {% if expectation.breaches %}
    <div class="table-wrap"><table class="tbl">
      <thead><tr>
        <th>Hour (UTC)</th><th class="num">Expected</th><th class="num">Observed</th>
        <th>Tier</th><th>Cloudflare label</th>
      </tr></thead>
      <tbody>
      {% for b in expectation.breaches %}
        <tr>
          <td class="mono">{{ b.t[:16]|replace("T", " ") }}</td>
          <td class="num mono muted">{{ b.expected }} <span class="iv">[{{ b.lo|int }}–{{ b.hi|int }}]</span></td>
          <td class="num mono breach">{{ "{:,}".format(b.observed) }}</td>
          <td><span class="badge badge--p99">{{ b.tier }}</span></td>
          <td class="mono muted">{{ b.threat_pct }}% flagged</td>
        </tr>
      {% endfor %}
      </tbody>
    </table></div>
    <p class="cap">
      Two ranges are computed for every hour: the <strong>p{{ params.tier_lo }}</strong> range should be exceeded about
      one hour in {{ params.tier_lo_one_in }}, the <strong>p{{ params.tier_hi }}</strong> range about one hour in {{ params.tier_hi_one_in }}. Tier records which
      of the two an hour broke through, and only p{{ params.tier_hi }} breaches are listed by name, since
      exceeding the p{{ params.tier_lo }} range is the model behaving as designed.
      {{ expectation.tier95_only }} further hour{{ "s" if expectation.tier95_only != 1 else "" }} fell
      outside the {{ params.tier_lo }}% interval but inside the {{ params.tier_hi }}%.
      The Cloudflare column is reference data rather than a model output.
    </p>
    {% elif expectation.available %}
    <div class="empty">
      No {{ params.tier_hi }}% interval breaches in the last {{ expectation.display_days }} days.
    </div>
    {% else %}
    {# Distinct from "no breaches": there is no interval to breach yet. Branching
       only on .breaches printed "in the last  days" with the number missing,
       because an unavailable expectation carries no display_days. That is the
       first two days of any new deployment, and the provenance gate cannot catch
       it — an absent number is not a wrong one. #}
    <div class="empty">No interval computed yet, so there is nothing to breach.</div>
    {% endif %}
  </section>

  <!-- ── T3 ──────────────────────────────────────────────────── -->
  <section class="sec">
    <div class="sec-head"><h2>Learned behavioral archetypes</h2><span class="tag">T3</span></div>
    {% if archetypes.available %}
    <div class="grid">
      {% for a in archetypes.archetypes %}
      <article class="card card--a{{ loop.index0 % 4 }}">
        <h3>{{ a.name }}</h3>
        <div class="card-n mono">{{ a.n_ips }} IP{{ "s" if a.n_ips != 1 else "" }}</div>
        <dl class="feat mono">
          <div><dt>path diversity</dt><dd>{{ a.median_diversity }}</dd></div>
          <div><dt>temporal span</dt><dd>{{ a.median_span_hours }} h</dd></div>
          <div><dt>family mix</dt><dd>{{ a.families|join(", ") }}</dd></div>
        </dl>
        <div class="ex mono">
          <span class="muted">exemplar</span> <span class="ex-ip">{{ a.exemplar.ip }}</span>
          <span class="muted ex-stat">· {{ "{:,}".format(a.exemplar.hits) }} hits</span>
          <span class="muted ex-stat">/ {{ "{:,}".format(a.exemplar.paths) }} paths</span>
        </div>
      </article>
      {% endfor %}
    </div>

    {% if archetypes.stability.available or archetypes.waf.available %}
    <div class="arch-score">
      {% if archetypes.stability.available %}
      <div class="as-item">
        <div class="as-v">{% if archetypes.stability.informative %}{{ archetypes.stability.ari }}{% else %}&mdash;{% endif %}</div>
        <div class="as-l">membership ARI vs last run</div>
        <div class="as-n">{% if archetypes.stability.informative %}{{ archetypes.stability.shared }} addresses in both runs · {{ archetypes.stability.new }} new since{% else %}no new addresses and no new requests since the last run — refitting identical input scores 1.0 by construction, so there is nothing to grade{% endif %}</div>
      </div>
      {% endif %}
      <div class="as-item">
        <div class="as-v">{{ archetypes.k }}</div>
        <div class="as-l">clusters{% if archetypes.k_moved %} — moved this run{% endif %}</div>
        <div class="as-n">carried from the previous run unless another count scores {{ archetypes.k_threshold }} better</div>
      </div>
      {% if archetypes.waf.available %}
      <div class="as-item">
        <div class="as-v">{{ archetypes.waf.ari }}</div>
        <div class="as-l">agreement with Cloudflare's action</div>
        <div class="as-n">{{ archetypes.waf.labeled }} labeled addresses across {{ archetypes.waf.actions }} action types · a low score is the useful one</div>
      </div>
      <div class="as-item">
        <div class="as-v">{{ archetypes.waf.twins }}</div>
        <div class="as-l">behavioral twins actioned differently</div>
        <div class="as-n">same group, different Cloudflare action from their peers</div>
      </div>
      {% endif %}
    </div>
    {% if archetypes.outliers %}
    <div class="outliers">
      <div class="ol-h">Removed before fitting — too few to form a pattern</div>
      {% for o in archetypes.outliers %}
      <div class="ol-r"><span class="mono">{{ o.ip }}</span>
        <span>{{ '{:,}'.format(o.hits) }} hits · {{ '{:,}'.format(o.paths) }} paths · {{ o.span_h }} h</span></div>
      {% endfor %}
    </div>
    {% endif %}

    {% endif %}
    <p class="cap">
      K-means over per-IP feature vectors — request count, path diversity, temporal span and
      probe-family concentration — at K={{ archetypes.k }} (silhouette {{ archetypes.silhouette }}) across {{ archetypes.n_ips }} addresses
      above {{ archetypes.min_hits }} events. K still comes from the data, but the
      count carried over from the last run only gives way when another K scores more
      than {{ archetypes.k_threshold }} better. Taking the top score outright moved the
      count on margins too small to mean anything, so a challenger now has to clear
      that margin to take its place. This run the sweep scored
      {% for k, v in archetypes.k_sweep.items() %}K={{ k }} {{ v }}{{ ", " if not loop.last }}{% endfor %}{% if
      archetypes.k_moved %}, and the count moved from {{ archetypes.k_incumbent }} to
      {{ archetypes.k }}{% elif archetypes.k_margin > 0 %}, leaving {{ archetypes.k }}
      in place {{ archetypes.k_margin }} behind the leader{% else %}, and
      {{ archetypes.k }} led outright{% endif %}.
      {% if archetypes.outliers %}{% set n = archetypes.outliers|length %}Those scores
      are from the sweep across all {{ archetypes.n_ips + n }} addresses. {{ n }}
      {{ 'address' if n == 1 else 'addresses' }} then landed in a group smaller than
      {{ archetypes.min_cluster }}, so {{ 'it was' if n == 1 else 'they were' }} removed
      and the model refitted at the same K — which is where the {{ archetypes.silhouette }}
      above comes from. {{ 'It is' if n == 1 else 'They are' }} listed below rather than
      presented as a pattern.{% endif %} The fit runs over the whole collected record, so
      a group describes every address seen since monitoring began.
    </p>
    {% else %}
    <div class="empty">Not enough per-IP volume to fit archetypes ({{ archetypes.reason }}).</div>
    {% endif %}
  </section>

  <!-- ── T4 ──────────────────────────────────────────────────── -->
  <section class="sec">
    <div class="sec-head"><h2>Model scorecard</h2><span class="tag">T4</span></div>
    {% if calibration.available %}
    <div class="metrics">
      {% for t in calibration.tiers %}
      <div class="metric">
        <div class="metric-label mono">{{ t.tier }} coverage</div>
        <div class="metric-value">{{ t.empirical }}<span class="unit">%</span></div>
        <div class="metric-sub mono">
          nominal {{ t.nominal }}% ·
          <span class="{{ 'good' if t.within_2pp else 'warn' }}">{{ "%+.1f"|format(t.delta) }} pp</span>
        </div>
      </div>
      {% endfor %}
      <div class="metric">
        <div class="metric-label mono">MAE, {{ calibration.days }}d</div>
        <div class="metric-value">{{ calibration.mae }}</div>
        <div class="metric-sub mono">over {{ "{:,}".format(calibration.n_evaluated) }} scored hours</div>
      </div>
    </div>
    {% if run_series|length > 1 %}
    <div class="sparks">
      {% for s in sparks %}
      <div class="spark"><div class="spark-label mono">{{ s.label }}</div>
        <div class="spark-canvas"><canvas id="{{ s.id }}"></canvas></div></div>
      {% endfor %}
    </div>
    {% endif %}
    <p class="cap">
      Coverage is the empirical share of hours that landed inside each interval, against
      the nominal level it claims. MAE is the mean absolute error of the point estimate
      over {{ "{:,}".format(calibration.n_evaluated) }} scored hours. A run that scores badly is shown here at the
      same size as one that scores well.{% if run_series|length <= 1 %} Sparklines appear once a second run has been scored.{% endif %}
    </p>
    {% else %}
    <div class="empty">Not enough history to score calibration yet.</div>
    {% endif %}
  </section>

  <!-- ── T5 ──────────────────────────────────────────────────── -->
  <section class="sec">
    <div class="sec-head"><h2>Model selection evidence</h2><span class="tag">T5</span></div>
    {% if backtest.available %}
    <div class="table-wrap"><table class="tbl">
      <thead><tr><th>Candidate</th><th class="num">MAE</th><th>Verdict</th></tr></thead>
      <tbody>
      {% for r in backtest.rows %}
        <tr class="{{ 'sel' if r.selected else '' }}">
          <td class="mono">{{ r.method }}</td>
          <td class="num mono">{{ r.mae }}</td>
          <td><span class="badge badge--{{ 'sel' if r.selected else 'rej' }}">{{ r.verdict }}</span>
              <span class="muted reason">{{ r.reason }}</span></td>
        </tr>
      {% endfor %}
      </tbody>
    </table></div>
    <p class="cap">
      Rolling-origin backtest, one step ahead, {{ "{:,}".format(backtest.n_evaluated) }} scored hours over
      the last {{ backtest.days }} days against a {{ backtest.window_days }}-day trailing window. The
      selected candidate is whichever scored lowest on this zone's own data. This table compares
      point-estimate accuracy only — the expected range above is always built from empirical
      quantiles and does not change with the winner.
    </p>
    {% endif %}
  </section>

  <details class="method">
    <summary>Methodology</summary>
    <div class="method-body">
      <p><strong>Data.</strong> Cloudflare GraphQL analytics for this zone, collected every twelve hours
      into SQLite. These datasets report traffic reaching the Cloudflare <em>edge</em>, including requests
      blocked or challenged before they ever reach the origin, so counts here are attempted traffic rather
      than served traffic. Per-IP and user-agent detail exists only for requests the WAF actioned; the
      archetypes below therefore describe actioned traffic, not all traffic. All-traffic
      per-IP detail is available on this plan and is not currently used.</p>

      <p><strong>Expected range.</strong> Empirical quantiles over a trailing
      {{ expectation.window_days }}-day window of observed hours, recomputed each hour. There is no
      parametric model and no distributional assumption — the interval is the observed distribution,
      which is what makes it conformal-style rather than a confidence interval. Measured coverage over
      the last {{ calibration.days }} days:
      {% for t in calibration.tiers %}{{ t.tier }} nominal {{ t.nominal }}% → empirical {{ t.empirical }}%{{ "; " if not loop.last }}{% endfor %}.</p>

      <p><strong>Gaps.</strong> Hours with no collected data are skipped rather than treated as
      zero. Imputing zero for a collection gap would drag the lower bound down and manufacture
      breaches that never happened, so the quantiles are computed over observed hours only.</p>

      <p><strong>Archetypes.</strong> K-means on standardized per-IP vectors of log request count, path
      diversity, log temporal span, and probe-family concentration. A fifth feature, the raw request rate,
      was removed: unlogged beside two logged magnitudes it was skewed enough that one address dominated
      its variance, and k-means answered by building three-member clusters around such addresses. Its
      logged form is log request count minus log span, which the vector already carries.
      K selected by silhouette.
      Addresses below {{ archetypes.min_hits }} events are excluded as too sparse to characterize, and the
      site owner's own address is excluded by configuration — it would otherwise rank among the top
      entities and tell you nothing. Exemplar addresses are shown with their final octet masked,
      so no individual visitor can be identified from this page.</p>

      <p><strong>No interpretation.</strong> This page reports measurements and the models' own
      scores against them. It draws no conclusions about what the traffic means, and every number
      shown is recomputed each run from this zone's data — a build-time check fails the run if any
      figure in a caption cannot be traced back to a value the run produced.</p>
    </div>
  </details>

  {# Deliberately no social or author links. This renders on whoever deploys it,
     so anything identifying belongs in their config, not in this template. #}
  <footer class="foot">
    <div class="footer-social">
      <a href="{{ site_url }}">{{ site_name }}</a>
    </div>
    <p class="footer-note">Generated by ml-traffic-analysis &middot; {{ generated_at }}</p>
  </footer>
</div>

<script src="chart-loader.js"
    data-deps="lib/chart.umd.min.js"
    data-app="dashboard.js?v={{ cache_bust }}"></script>
</body>
</html>
"""

CSS = r"""
/* System font stacks — nothing bundled and nothing fetched. The page ships as
   HTML, CSS and JS only, and renders in whatever the reader's OS provides. */
:root{
  /* Page sits at #14161f rather than near-black. Two reasons, both measured:
     body text on the old #0e1017 ran 15.2:1, high enough to halate on OLED over
     a long read; and cards at #171a26 scored 1.10:1 against it, which is below
     the ~1.25 where a raised surface reads as raised at all — the panels were
     effectively invisible. Lifting the page and the cards together brings text
     to 14.4:1 and card separation to 1.29:1, while keeping the violet bias. */
  --accent:#8f7fe8; --accent-soft:#a99bf0; --accent-rgb:143,127,232;
  --text:#e4e5ef; --text-light:#9298ac;
  --bg:#14161f; --bg-alt:#2a2d3e; --bg-card:#282b3b; --border:#3a4054;
  --red:#d9776a; --green:#5bbf8a; --amber:#d4a843;
  --text-strong:#fff;
  --fg-rgb:255,255,255;      /* hairlines and faint fills */
  --border-rgb:58,64,84;     /* == --border, needed for rgba() */
  --red-rgb:217,119,106;     /* == --red, needed for rgba() */
  /* These three were only ever reached as var() fallbacks and so could never
     follow a theme. Declared here at the exact fallback values, so dark is
     byte-identical and light has something to override. */
  --card:#1c2128; --bd:#30363d; --mut:#8b949e;
  --radius:8px;
  --serif:Georgia,'Times New Roman',serif;
  --sans:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,'Helvetica Neue',Arial,sans-serif;
  --mono:ui-monospace,SFMono-Regular,Menlo,Consolas,'Liberation Mono',monospace;
}
*{box-sizing:border-box;margin:0;padding:0}
/* color-scheme is what makes the browser's own furniture follow the page:
   scrollbars, form controls and the overscroll gutter. Without it the scrollbar
   renders light against a dark page — the two rules below only reach the bar
   itself, and only in engines that support them. */
html{color-scheme:dark;background:var(--bg);scrollbar-color:var(--border) var(--bg)}
body{background:var(--bg);color:var(--text);font-family:var(--sans);
  font-size:15px;line-height:1.6;-webkit-font-smoothing:antialiased;}
::-webkit-scrollbar{width:12px;height:12px}
::-webkit-scrollbar-track{background:var(--bg)}
::-webkit-scrollbar-thumb{background:var(--border);border-radius:6px;
  border:3px solid var(--bg)}
::-webkit-scrollbar-thumb:hover{background:var(--text-light)}
::-webkit-scrollbar-corner{background:var(--bg)}
/* Wide by design: the tables here have five columns of numbers and the chart
   spans three decades on a log axis, both of which cramp badly below ~1300px.
   clamp() keeps the gutter proportional instead of fixed, so the page does not
   sit in a narrow column with a wide empty margin on a large display. */
.wrap{max-width:1600px;margin:0 auto;padding:56px clamp(28px,4vw,72px) 80px}
.mono{font-family:var(--mono);font-variant-numeric:tabular-nums}
.muted{color:var(--text-light)}
.num{text-align:right}

.back-link{display:inline-block;font-family:var(--mono);font-size:.76rem;color:var(--text-light);
  text-decoration:none;margin-bottom:22px;letter-spacing:.02em}
.back-link:hover{color:var(--accent)}
.intro{font-size:.92rem;color:var(--text-light);margin:-24px 0 52px}
.intro a{color:var(--accent-soft);text-decoration:none;border-bottom:1px solid rgba(var(--accent-rgb),.3)}
.intro a:hover{border-bottom-color:var(--accent)}

/* header */
.head{display:flex;justify-content:space-between;align-items:flex-start;gap:32px;
  padding-bottom:28px;border-bottom:1px solid var(--border);margin-bottom:52px;flex-wrap:wrap}
h1{font-family:var(--serif);font-size:2.1rem;font-weight:700;letter-spacing:-.015em;color:var(--text-strong)}
.sub{color:var(--text-light);font-size:.95rem;margin-top:6px}
.meta{font-family:var(--mono);font-size:.78rem;text-align:right;color:var(--text);white-space:nowrap}
.meta .muted{font-size:.72rem}

/* sections */
.sec{margin-bottom:60px}
.sec-head{display:flex;align-items:baseline;gap:12px;margin-bottom:18px}
.sec-head h2{font-family:var(--serif);font-size:1.32rem;font-weight:700;color:var(--text-strong)}
.tag{font-family:var(--mono);font-size:.66rem;letter-spacing:.08em;color:var(--accent);
  border:1px solid rgba(var(--accent-rgb),.35);border-radius:3px;padding:2px 6px;
  background:rgba(var(--accent-rgb),.07)}
.cap{margin-top:16px;font-size:.87rem;color:var(--text-light)}
.cap strong{color:var(--text);font-weight:600}
.empty{border:1px dashed var(--border);border-radius:var(--radius);padding:22px;
  color:var(--text-light);font-size:.9rem;background:rgba(var(--fg-rgb),.012)}

/* hero chart */
.chart-hero{height:430px;position:relative;
  background:linear-gradient(180deg,rgba(var(--fg-rgb),.016),transparent);
  border:1px solid var(--border);border-radius:var(--radius);padding:14px}

/* tables */
.tbl{width:100%;border-collapse:collapse;font-size:.85rem}
.tbl th{text-align:left;font-family:var(--mono);font-size:.7rem;letter-spacing:.06em;
  text-transform:uppercase;color:var(--text-light);font-weight:500;
  padding:0 14px 10px;border-bottom:1px solid var(--border)}
.tbl th.num{text-align:right}
.tbl td{padding:11px 14px;border-bottom:1px solid rgba(var(--border-rgb),.5)}
.tbl tr:last-child td{border-bottom:none}
.tbl tr.sel{background:rgba(var(--accent-rgb),.05)}
.iv{color:var(--text-light);font-size:.9em}
.breach{color:var(--red);font-weight:500}
.reason{font-size:.8rem;margin-left:8px}
.badge{font-family:var(--mono);font-size:.68rem;padding:2px 7px;border-radius:3px;
  border:1px solid;letter-spacing:.03em}
.badge--p99{color:var(--red);border-color:rgba(var(--red-rgb),.4);background:rgba(var(--red-rgb),.08)}
.badge--sel{color:var(--accent);border-color:rgba(var(--accent-rgb),.4);background:rgba(var(--accent-rgb),.09)}
.badge--rej{color:var(--text-light);border-color:var(--border)}

.table-wrap{overflow-x:auto;-webkit-overflow-scrolling:touch;max-width:100%}
/* removed outliers */
.outliers{background:var(--card,#1c2128);border:1px solid var(--bd,#30363d);border-radius:10px;
padding:12px 16px;margin:14px 0 0}
.ol-h{font-size:.7rem;text-transform:uppercase;letter-spacing:.07em;color:var(--mut,#8b949e);margin-bottom:8px}
.ol-r{display:flex;justify-content:space-between;gap:14px;flex-wrap:wrap;font-size:.78rem;
padding:4px 0;border-top:1px solid rgba(var(--fg-rgb),.05)}
.ol-r span:last-child{color:var(--mut,#8b949e);font-variant-numeric:tabular-nums}
/* archetype scorecard */
.arch-score{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(210px,100%),1fr));
gap:14px;margin:18px 0 10px}
.as-item{background:var(--card,#1c2128);border:1px solid var(--bd,#30363d);border-radius:10px;padding:14px 16px}
.as-v{font-size:1.45rem;font-weight:800;letter-spacing:-.02em;font-variant-numeric:tabular-nums}
.as-l{font-size:.72rem;color:var(--mut,#8b949e);text-transform:uppercase;letter-spacing:.07em;margin-top:3px}
.as-n{font-size:.72rem;color:var(--mut,#8b949e);margin-top:6px;line-height:1.5}
/* archetype cards */
.grid{display:grid;gap:14px;grid-template-columns:repeat(auto-fill,minmax(280px,1fr))}
.card{background:var(--bg-card);border:1px solid var(--border);border-radius:var(--radius);
  padding:18px;border-top:2px solid var(--a)}
.card--a0{--a:#8f7fe8} .card--a1{--a:#2bcbba}
.card--a2{--a:#4fa3f7} .card--a3{--a:#9b8bc4}
.card h3{font-size:.98rem;font-weight:600;color:var(--text-strong);line-height:1.35}
.card-n{font-size:.74rem;color:var(--a);margin-top:3px;letter-spacing:.04em}
.feat{margin:14px 0 12px;font-size:.78rem}
.feat > div{display:flex;justify-content:space-between;gap:12px;padding:3px 0}
.feat dt{color:var(--text-light)}
.feat dd{color:var(--text);text-align:right}
/* word-break:break-all split "238 paths" into "238 path / s". Only the
   address may break mid-token (IPv6 has no break opportunities and would
   otherwise overflow); the counts are held on one line. */
.ex{font-size:.73rem;padding-top:11px;border-top:1px solid rgba(var(--border-rgb),.6);
  line-height:1.5;word-break:normal;overflow-wrap:break-word}
.ex-ip{overflow-wrap:anywhere}
.ex-stat{white-space:nowrap}

/* metrics */
.metrics{display:grid;gap:14px;grid-template-columns:repeat(auto-fit,minmax(180px,1fr))}
.metric{background:var(--bg-card);border:1px solid var(--border);border-radius:var(--radius);padding:16px 18px}
.metric-label{font-size:.68rem;letter-spacing:.07em;text-transform:uppercase;color:var(--text-light)}
.metric-value{font-family:var(--mono);font-size:1.85rem;font-weight:500;color:var(--text-strong);
  margin-top:6px;line-height:1;font-variant-numeric:tabular-nums}
.metric-value .unit{font-size:1rem;color:var(--text-light);margin-left:2px}
.metric-sub{font-size:.72rem;color:var(--text-light);margin-top:7px}
.good{color:var(--green)} .warn{color:var(--amber)}
.sparks{display:grid;gap:14px;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));margin-top:16px}
.spark{background:var(--bg-card);border:1px solid var(--border);border-radius:var(--radius);padding:12px 14px}
.spark-label{font-size:.68rem;letter-spacing:.06em;text-transform:uppercase;color:var(--text-light);margin-bottom:6px}
/* Chart.js with maintainAspectRatio:false sizes the canvas from its parent.
   With no height on that parent each resize grew the canvas from the one
   before it — the sparklines reached 809,174px tall. The wrapper below
   fixes the height and takes the canvas out of flow so the loop cannot start. */
.spark-canvas{position:relative;height:44px}
.spark-canvas canvas{position:absolute;top:0;left:0;width:100%;height:100%}

/* methodology + footer */
.method{border-top:1px solid var(--border);padding-top:26px;margin-top:12px}
.method summary{font-family:var(--mono);font-size:.76rem;letter-spacing:.07em;
  text-transform:uppercase;color:var(--text-light);cursor:pointer}
.method summary:hover{color:var(--accent)}
.method-body{margin-top:18px}
.method-body p{font-size:.85rem;color:var(--text-light);margin-bottom:14px}
.method-body strong{color:var(--text);font-weight:600}
.foot{margin-top:44px;padding-top:22px;border-top:1px solid var(--border)}
.footer-social{display:flex;flex-wrap:wrap;gap:18px;font-size:.82rem}
.footer-social a{color:var(--text-light);text-decoration:none}
.footer-social a:hover{color:var(--accent)}
.footer-note{font-size:.76rem;color:var(--text-light);margin-top:14px}
.charts-unavailable .chart-hero{display:none}
.chart-unavailable-note{margin:0;font-size:.72rem;line-height:1.45;color:var(--text-light)}
.charts-unavailable .spark-canvas{height:auto;min-height:44px}
@media(max-width:640px){
  .wrap{padding:36px 18px 60px} h1{font-size:1.7rem}
  .head{flex-direction:column;gap:14px} .meta{text-align:left}
  .chart-hero{height:340px}
}

"""

JS = r"""
(function(){
  var D = window.__DATA__ || {};
  // Chart.js keeps colours in JS config, not CSS, so a canvas cannot read a
  // custom property. These mirror the CSS palette and must be changed alongside
  // it if the theme is edited.
  var C = {accent:'#8f7fe8', band:'rgba(143,127,232,0.13)', band99:'rgba(143,127,232,0.30)',
           red:'#d9776a', text:'#9298ac', grid:'rgba(255,255,255,0.04)'};
  if (typeof Chart === 'undefined') return;
  Chart.defaults.font.family = "ui-monospace, SFMono-Regular, Menlo, monospace";
  Chart.defaults.font.size = 10;

  function build(){
  Chart.defaults.color = C.text;

  var exp = D.expectation;
  if (exp && exp.available) {
    var all = exp.points.concat(exp.forward);
    var labels = all.map(function(p){
      var d = new Date(p.t);
      return d.toLocaleString('en-US',{month:'short',day:'numeric',hour:'numeric',timeZone:'UTC'});
    });
    var nowIdx = exp.points.length - 1;

    var ds = [
      {label:'lo90', data: all.map(function(p){return p.lo90;}),
       borderWidth:0, pointRadius:0, fill:false, tension:.3},
      {label:'90% interval', data: all.map(function(p){return p.hi90;}),
       borderWidth:0, pointRadius:0, backgroundColor:C.band, fill:'-1', tension:.3},
      {label:'p99', data: all.map(function(p){return p.hi99;}),
       borderColor:'rgba(143,127,232,0.55)', borderWidth:1, borderDash:[4,4],
       pointRadius:0, fill:false, tension:.3},
      {label:'observed', data: all.map(function(p){return p.observed;}),
       borderColor:C.accent, borderWidth:1.5, tension:.25, spanGaps:false,
       pointRadius: all.map(function(p){return p.breach==='p99'?4:0;}),
       pointBackgroundColor:C.red, pointBorderColor:C.red, fill:false}
    ];

    new Chart(document.getElementById('rangeChart'), {
      type:'line',
      data:{labels:labels, datasets:ds},
      options:{
        responsive:true, maintainAspectRatio:false,
        interaction:{mode:'index', intersect:false},
        scales:{
          y:{type:'logarithmic', grid:{color:C.grid},
             ticks:{
               // Label only 1, 2 and 5 x 10^n. Chart.js's default logarithmic
               // ticks place 900, 1,000 and 1,500 within a few pixels of each
               // other whenever the maximum lands just past a decade boundary,
               // and the labels overlap into an unreadable smear at the top of
               // the axis. Returning '' hides the label while keeping the
               // gridline, so the scale still reads as logarithmic.
               callback:function(v){
                 var e = Math.floor(Math.log10(v));
                 var m = v / Math.pow(10, e);
                 return (Math.abs(m-1) < 0.01 || Math.abs(m-2) < 0.01 || Math.abs(m-5) < 0.01)
                   ? v.toLocaleString() : '';
               },
               // autoSkip off: it culls by tick position, and the blanked ticks
               // still occupy positions, so leaving it on drops half the
               // anchors this callback just chose — 20/100/200/1,000 with the
               // 50 and 500 missing. Density is decided above instead.
               autoSkip:false, color:C.text, font:{size:10}
             },
             title:{display:true,text:'requests / hour',color:C.text,font:{size:10}}},
          x:{grid:{display:false}, ticks:{maxTicksLimit:14, maxRotation:45, minRotation:45,
             autoSkip:true, padding:2}}
        },
        plugins:{
          legend:{display:false},
          tooltip:{
            filter:function(i){return i.datasetIndex===3;},
            callbacks:{
              label:function(ctx){
                var p = all[ctx.dataIndex];
                if (p.observed === null || p.observed === undefined) return null;
                return 'observed ' + p.observed.toLocaleString() +
                       ' · expected ' + p.expected + ' [' + Math.round(p.lo99) + '–' + Math.round(p.hi99) + ']';
              }
            }
          },
          annotation:undefined
        }
      },
      plugins:[{
        id:'nowline',
        afterDraw:function(ch){
          var x = ch.scales.x.getPixelForValue(nowIdx);
          var a = ch.chartArea, g = ch.ctx;
          g.save();
          g.strokeStyle='rgba(255,255,255,0.28)'; g.lineWidth=1; g.setLineDash([3,3]);
          g.beginPath(); g.moveTo(x,a.top); g.lineTo(x,a.bottom); g.stroke();
          g.setLineDash([]);
          g.fillStyle='#9298ac'; g.font="9px ui-monospace, SFMono-Regular, Menlo, monospace";
          g.textAlign='left'; g.fillText('now', x+5, a.top+10);
          g.textAlign='right'; g.fillStyle='rgba(143,127,232,0.75)';
          var yv = ch.scales.y.getPixelForValue(all[all.length-1].hi99);
          g.fillText('p99', a.right-3, yv-4);
          g.restore();
        }
      }]
    });
  }

  (D.sparks || []).forEach(function(s){
    var el = document.getElementById(s.id);
    if (!el) return;
    new Chart(el, {
      type:'line',
      data:{labels:s.values.map(function(_,i){return i;}),
            datasets:[{data:s.values, borderColor:C.accent, borderWidth:1.5,
                       pointRadius:0, fill:false, tension:.3}]},
      options:{responsive:true, maintainAspectRatio:false,
               plugins:{legend:{display:false},tooltip:{enabled:false}},
               scales:{x:{display:false},y:{display:false}}}
    });
  });
  }
  build();
})();
"""


class ChartTableMismatch(AssertionError):
    """The chart series and the table disagree about the same hours."""


def assert_chart_matches_table(expectation):
    """Fail the run rather than publish a chart that contradicts its own table.

    The chart plots expectation["points"]; the breach table renders
    expectation["breaches"]. Both descend from build_expectation today, so they
    cannot drift — this exists to keep it that way. Nobody looks at this page
    between runs, so a chart that silently stops agreeing with the numbers beside
    it would stay wrong until someone happened to notice.

    Two invariants, both stated against the same window:

      1. the largest value handed to the chart equals the largest observed hour
      2. every breach the table lists appears in the series at the same value

    A GitHub annotation is emitted before raising so the failure is visible in
    the job summary rather than only in a stack trace.
    """
    points = expectation.get("points") or []
    breaches = expectation.get("breaches") or []
    observed = [p.get("observed") for p in points if p.get("observed") is not None]
    if not observed:
        return

    problems = []
    series_max, table_max = max(observed), max((b["observed"] for b in breaches), default=None)
    if table_max is not None and table_max > series_max:
        problems.append(f"table lists an hour at {table_max:,} that the chart series tops out "
                        f"below ({series_max:,})")

    by_hour = {p.get("t"): p.get("observed") for p in points}
    for b in breaches:
        plotted = by_hour.get(b.get("t"))
        if plotted is None:
            problems.append(f"breach {b.get('t')} has no plotted point")
        elif plotted != b["observed"]:
            problems.append(f"breach {b.get('t')} plots {plotted:,} but the table says "
                            f"{b['observed']:,}")

    if problems:
        detail = "; ".join(problems)
        print(f"::error title=Chart contradicts table::{detail}", flush=True)
        log.error("CHART_TABLE_MISMATCH: %s", detail)
        raise ChartTableMismatch(detail)
    log.info("CHART_TABLE_OK: series max %s, %d breach marker(s) matched",
             f"{series_max:,}", len(breaches))


# Header display only. The run stamp at the top of the page is rounded to the
# nearest quarter hour so a schedule that advertises "every 12h" is not paired
# with a timestamp implying minute precision it does not have — the run starts
# whenever the runner picks the job up.
#
# DELIBERATELY NOT SHARED. This is not a formatting utility and nothing else may
# call it. run_facts, the job summary, the breach table, the chart axis and every
# prose timestamp keep full precision, because those are used to line rows up
# against each other and to attribute a run to a scheduled slot. Rounding that
# would move a run across a slot boundary and corrupt every counter built on
# slot history.
_HEADER_ROUND_MINUTES = 15


def _header_stamp(dt):
    """"%Y-%m-%d %H:%M", rounded to the nearest quarter hour.

    Half rounds up, and the arithmetic runs through timedelta so the hour and
    the date roll over on their own: 23:53 becomes 00:00 the following day.
    """
    half = _HEADER_ROUND_MINUTES / 2
    offset = dt.minute % _HEADER_ROUND_MINUTES
    dt = dt.replace(second=0, microsecond=0)
    if offset * 1.0 >= half:
        dt += timedelta(minutes=_HEADER_ROUND_MINUTES - offset)
    else:
        dt -= timedelta(minutes=offset)
    return dt.strftime("%Y-%m-%d %H:%M")


def _params():
    """The fixed parameters that reach the page as words rather than numbers.

    CLASS (a) VALUES — deliberately not computed from data. Each one is a
    setting somewhere in the pipeline, and the page reads it from that setting
    rather than repeating it. Nominal interval levels used to be typed into the
    template as "90%" and "99%"; changing predictive.TIERS would have left the
    prose describing an interval the page no longer computes.
    """
    from config import SLOT_HOURS
    from predictive import TIERS

    # A TIERS entry is (name, lower_quantile, upper_quantile) and the interval is
    # two-sided: "p90" runs from the 5th to the 95th percentile. Nominal coverage
    # is therefore the SPAN between the quantiles, not the upper one. Reading the
    # upper quantile instead reported a 95% interval on a page that computes a
    # 90% one.
    def level(t):
        return round((t[2] - t[1]) * 100)

    # Spelled out, because the sentence reads as speech: "one hour in ten", not
    # "one hour in 10". Falls back to the numeral for rates the words don't cover.
    WORDS = {2: "two", 4: "four", 5: "five", 10: "ten", 20: "twenty",
             50: "fifty", 100: "a hundred", 200: "two hundred", 1000: "a thousand"}

    def one_in(level_pct):
        n = round(1 / (1 - level_pct / 100))
        return WORDS.get(n, f"{n:,}")

    lo, hi = level(TIERS[0]), level(TIERS[1])
    return {"slot_hours": SLOT_HOURS,
            "tier_lo": lo, "tier_hi": hi,
            "tier_lo_one_in": one_in(lo), "tier_hi_one_in": one_in(hi)}


def _prose_blocks(html_text):
    """Rendered text blocks that quote computed values, for the provenance gate.

    Captions, methodology paragraphs and empty states. These describe what was
    measured and how; they do not interpret it.
    """
    blocks = {}
    for m in re.finditer(r'<p class="(cap)">(.*?)</p>', html_text, re.S):
        blocks[f"{m.group(1)}#{len(blocks)}"] = m.group(2)
    meth = re.search(r'<details class="method">(.*?)</details>', html_text, re.S)
    if meth:
        for i, para in enumerate(re.findall(r"<p>(.*?)</p>", meth.group(1), re.S)):
            blocks[f"method#{i}"] = para
    for m in re.finditer(r'<div class="empty">(.*?)</div>', html_text, re.S):
        blocks[f"empty#{len(blocks)}"] = m.group(1)
    return blocks


def _assert_number_provenance(html_text, facts):
    import provenance
    blocks = _prose_blocks(html_text)
    bad = provenance.check(blocks, facts)
    if bad:
        for name, tok in bad[:10]:
            print(f"::error title=Frozen number in prose::{tok} in {name} traces to no "
                  f"computed fact and is not a declared parameter", flush=True)
        raise AssertionError(f"{len(bad)} unsupported number(s) in prose: "
                             + ", ".join(t for _, t in bad[:6]))
    log.info("NUMBER_PROVENANCE_OK: every number in %d blocks traces to a fact", len(blocks))


def render(*, expectation, calibration, backtest, archetypes, run_series,
           median_hour, out_dir: Path, asset_src: Path):
    out_dir.mkdir(parents=True, exist_ok=True)

    sparks = []
    if len(run_series) > 1:
        def col(key):
            return [r[key] for r in run_series if r.get(key) is not None]
        for key, label, sid in (("coverage_p90", "p90 coverage", "spk1"),
                                ("coverage_p99", "p99 coverage", "spk2"),
                                ("mae", "MAE", "spk3")):
            v = col(key)
            if len(v) > 1:
                sparks.append({"id": sid, "label": label, "values": v})

    assert_chart_matches_table(expectation)

    payload = json.dumps({"expectation": expectation, "sparks": sparks},
                         separators=(",", ":"), default=str)
    cache_bust = run_clock.now().strftime("%Y%m%d%H%M")

    html = Template(TEMPLATE).render(
        expectation=expectation, calibration=calibration, backtest=backtest,
        archetypes=archetypes, run_series=run_series, sparks=sparks,
        median_hour=median_hour, payload=payload, cache_bust=cache_bust, params=_params(),
        generated_at=_header_stamp(run_clock.now()),
        # Site identity is configuration, never a literal in this file. Defaults
        # to example.com so an unconfigured run produces an obviously-placeholder
        # page rather than silently branding it as someone else's.
        site_name=get_dashboard_site_name(),
        site_url=get_dashboard_site_url(),
    )
    # Build-time gate over the assembled page. The captions quote computed values
    # inline, and this page is unattended — a figure that was correct when it was
    # typed and is now frozen looks exactly like a live one. This fails the run
    # rather than publishing a stale number.
    gates = {}
    _assert_number_provenance(html, {
        "expectation": expectation, "calibration": calibration, "backtest": backtest,
        "archetypes": archetypes,
        # median_hour is quoted directly in the range caption, so it has to be in
        # the fact space. Omitting it meant that caption passed only when the
        # median happened to collide with some other computed value — true of one
        # traffic shape and not another, which is the opposite of a gate.
        "median_hour": median_hour})
    gates["number_provenance"] = "ok"
    gates["chart_table"] = "ok"

    (out_dir / "index.html").write_text(html, encoding="utf-8")
    (out_dir / "dashboard.css").write_text(CSS, encoding="utf-8")
    # Data is prepended to dashboard.js rather than emitted as an inline
    # <script> in the HTML. Keeping the page free of inline script means it
    # serves unchanged from a host that forbids it; emit it inline and the block
    # is dropped, the chart receives no data, and it fails only in the place
    # that has the policy — never locally.
    (out_dir / "dashboard.js").write_text(
        "window.__DATA__ = " + payload + ";\n" + JS, encoding="utf-8")
    # asset_src is BASE_DIR — the repo root — so these live beside this file.
    #
    # Raise rather than skip: the page references this by name, so a missing
    # source means a 404 at runtime. `if src.exists()` would skip without a word
    # and the failure would surface only in someone's browser console.
    for name in ("chart-loader.js",):
        src = asset_src / name
        if not src.exists():
            raise FileNotFoundError(
                f"{src} missing — the page references it and would 404")
        shutil.copy(src, out_dir / name)
    # Chart.js is fetched into lib/ by vendor_chartjs.py rather than committed,
    # and served from this origin at runtime. Absence is a warning, not an
    # error: the page still renders and chart-loader.js shows a note in place of
    # each canvas, so the tables remain readable. Silently producing a page
    # whose charts 404 would be worse than saying so here.
    lib_src, lib_dst = asset_src / "lib", out_dir / "lib"
    libs = sorted(lib_src.glob("*.js")) if lib_src.exists() else []
    if libs:
        lib_dst.mkdir(parents=True, exist_ok=True)
        for lib in libs:
            shutil.copy(lib, lib_dst / lib.name)
    else:
        log.warning("lib/ is empty — charts will not render. "
                    "Run `python vendor_chartjs.py` to fetch Chart.js.")
    log.info("Dashboard written to %s", out_dir / "index.html")
    LAST_GATES.clear()
    LAST_GATES.update(gates)
    return out_dir / "index.html"
