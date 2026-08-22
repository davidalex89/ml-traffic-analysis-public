# Contributing

## Branch workflow

`main` is protected. Do not push code directly to `main`.

1. Create a branch from `main`:
   ```bash
   git checkout main && git pull
   git checkout -b feature/short-description
   ```
2. Make changes and push the branch.
3. Open a pull request against `main`.
4. Wait for the **CI** workflow (`validate` job) to pass.
5. Merge the PR (squash merge is fine).

Pipeline runs append metrics to the unprotected `run-history` branch (not
`main`) so branch protection stays intact.

## CI checks

Pull requests run `.github/workflows/ci.yml`:

- Python syntax check
- Import-graph resolution — catches a module deleted or renamed without its
  importers being updated, which a syntax check alone sails past
- Renderer smoke test against a seeded database (`seed_demo_db.py`), so a broken
  template fails here rather than in production
- A check that the rendered page takes its identity from config, asserted
  positively: CI builds with everything set to `example.com` and fails if the
  title or back-link is anything else

## Pipeline workflow

`.github/workflows/collect.yml` runs on `workflow_dispatch` only — there is no
cron schedule, and it is not triggered by pushes to `main`. Nothing in this
repository runs on its own. Adding a schedule is a per-fork decision; see the
comment at the top of the workflow.

After each run it may append one line to `data/run_history.jsonl` and push
with `[skip ci]` in the commit message. With no Cloudflare credentials
configured the step is skipped rather than failed, so an unconfigured fork does
not show a red Actions tab.

## Secrets (maintainers)

Repository **Secrets**: `CF_API_TOKEN`, `CF_ZONE_ID`

Optional **Variables**: `DASHBOARD_SITE_URL`, `DASHBOARD_SITE_NAME`, `ML_EXCLUDED_IPS`

Never commit `cf_token.txt`, `cf_zone.txt`, or `.env`.
