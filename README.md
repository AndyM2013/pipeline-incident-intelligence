# Pipeline Incident Intelligence

A governed lakehouse on Databricks that brings together Canadian (CER) and US (PHMSA) pipeline incident data, extracts causes from incident narratives with an LLM, scores that extraction against the regulators' own coded causes, and serves an agent that answers integrity questions.

Status: **Week 1 — ingest to Bronze.**

## Layout

```
databricks.yml                 Asset Bundle (catalog variable, dev target)
resources/ingest.pipeline.yml  Lakeflow Spark Declarative Pipeline, serverless
src/pipelines/bronze.py        Auto Loader -> bronze.cer_incident_raw, bronze.phmsa_liquid_raw
scripts/fetch_snapshots.py     Local downloader: dated snapshots -> landing Volume (incl. Internet Archive history)
scripts/sources.json           Source URLs (confirm before first run)
sql/00_setup.sql               Catalog, schemas, landing Volume
sql/10_bronze_checks.sql       Week 1 acceptance checks
tests/                         Unit tests for the downloader and Bronze naming rules
.github/workflows/deploy.yml   Test on every push; deploy the bundle from main
```

## Week 1 runbook

1. **Workspace objects.** Run `sql/00_setup.sql` in the SQL editor.
2. **CLI on your machine.** Install the Databricks CLI, then `databricks auth login --host <your workspace URL>`. Put the same URL in `databricks.yml`.
3. **Python.** `python -m venv .venv && source .venv/bin/activate && pip install -r requirements-dev.txt && pytest -q`
4. **Confirm sources.** Open each URL in `scripts/sources.json` in a browser; fix the PHMSA zip link from the PHMSA page.
5. **Land today's snapshot.** `python scripts/fetch_snapshots.py`
6. **Land history.** Pull two or three older captures so Week 2's CDC has real changes to model:
   ```
   python scripts/fetch_snapshots.py --source cer --wayback 20240101
   python scripts/fetch_snapshots.py --source cer --wayback 20250101
   ```
   The folder date is the capture the archive actually returns. Identical content is skipped.
7. **Deploy and run.** `databricks bundle deploy` then `databricks bundle run incident_ingest`.
8. **Check.** Run `sql/10_bronze_checks.sql`; pick the real key, date, cause and narrative columns from the `DESCRIBE` output and fix query 3.
9. **CI.** Add `DATABRICKS_HOST` and `DATABRICKS_TOKEN` as GitHub Actions secrets. If your workspace does not allow personal access tokens, keep deploying from your machine.

## Data and terms

- CER: *Incidents at CER-regulated pipelines and facilities*, Open Government Licence – Canada.
- PHMSA: hazardous liquid accident data, January 2010 to present, US public data.
- Databricks Free Edition is for non-commercial use; this is a personal portfolio project.
