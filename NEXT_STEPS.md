# NEXT_STEPS.md

## Current status
- Project is live on GitHub with `main` as the stable branch.
- CI and CD workflows are configured and passing.
- Core pipeline is implemented:
  - Data generation -> CDC -> landing -> warehouse load -> dbt -> BI provisioning.
- Documentation and env templates are in place (`README.md`, `.env.example`, folder-level `.env.example` files).

## Known gaps
- Postgres schema bootstrap wiring in Docker Compose needs explicit confirmation and standardization.
- dbt docs/lineage screenshots are not yet committed to `docs/images/`.
- No full integration test that runs the complete pipeline in CI.
- Runtime data directories must stay out of git and be monitored.

## Resume checklist
- Pull latest main:
  - `git checkout main`
  - `git pull origin main`
- Create feature branch:
  - `git checkout -b feat/<topic>`
- Run checks before PR:
  - `python -m py_compile (git ls-files '*.py')`
  - `python -m unittest discover -s tests/unit -p "test_*.py" -v`
  - `docker compose -f docker-compose.yaml config > $null`
- Open PR to `main` and wait for CI pass + required review.

## Top next improvements (priority order)
1. Add integration test workflow for one end-to-end pipeline run.
2. Add dbt docs generation/publish step in CI.
3. Add architecture and DAG/dbt/dashboard screenshots to `docs/images/`.
4. Add a one-command bootstrap script for local setup.
5. Add alerting strategy for Airflow task failures.
6. Pin Superset image version instead of `latest`.

## Branch and release strategy
- Keep `main` protected.
- Do all work in short-lived feature branches.
- Use semantic tags for milestones:
  - `v1.0.0`, `v1.1.0`, `v1.2.0`, etc.
- Create release notes from merged PRs.

## Safety rules
- Never commit real `.env` files or runtime data.
- Only commit `.env.example` templates.
- Keep secrets in GitHub Actions secrets and local env files.
- Do not bypass CI checks on `main`.

## Quick commands
- New work:
  - `git checkout main && git pull origin main`
  - `git checkout -b feat/<next-task>`
- Finish work:
  - `git add -A`
  - `git commit -m "<type>: <summary>"`
  - `git push -u origin feat/<next-task>`
- Release:
  - `git checkout main && git pull origin main`
  - `git tag v1.0.0`
  - `git push origin v1.0.0`
