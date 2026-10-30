# CLAUDE.md — Pipeline Incident Intelligence

## What this repo is
Personal portfolio project on Databricks Free Edition: CER + PHMSA pipeline incidents -> Bronze/Silver/Gold in Unity Catalog -> LLM cause extraction with evaluation -> Vector Search -> agent with read-only Unity Catalog function tools.

## Rules
- Bronze keeps every source column as text. Types, units and keys are applied in Silver, never in Bronze.
- Every layer is rebuildable from the landing Volume. Never hand-edit tables.
- Snapshots are dated by folder (`<source>/<YYYY-MM-DD>/`); history comes from snapshot diffs, not from guessing.
- LLM outputs always carry model name, prompt version and confidence; never overwrite an earlier version's rows.
- Agent tools are read-only. Anything that would write proposes, never executes.
- No client data, ever. Free Edition is non-commercial.

## Environment limits (Free Edition)
Serverless only; one active pipeline per type; one Vector Search endpoint; daily compute quota; outbound internet limited, so downloads run locally via `scripts/fetch_snapshots.py`.

## Commands
- Tests: `pytest -q`
- Deploy: `databricks bundle deploy -t dev`
- Run ingest: `databricks bundle run incident_ingest`

## Open questions (update as answered)
- Real key columns: CER incident number? PHMSA report number? (confirm from `sql/10_bronze_checks.sql`)
- Which CER columns hold narrative text, and how long are they?
- PHMSA file encoding and quoting.
- Does `ai_query` reach a pay-per-token model in this workspace?

## Agent review log
Record each time generated code needed a fix: what was wrong, how it was caught. This is interview material.
