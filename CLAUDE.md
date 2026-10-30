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

## Source facts (checked 2026-10-30)
- CER CSV: 2,034 incidents, 16 columns, years 2008–2026, encoding windows-1252, no ragged rows.
- CER key: `Incident Number` (unique per row) -> `incident_number` in Bronze.
- CER has NO free-text narrative. `What Happened` (immediate cause) and `Why It Happened` (basic cause) are
  comma-separated category lists, e.g. "External Interference, Incorrect Operation". Treat them as multi-label coded causes.
- The CER data dictionary describes ~100 columns (pipe material, coating, inspection dates…) that the published CSV does not contain.
- Consequence: narratives for extraction and retrieval come from PHMSA; CER supplies Canadian facts and a clean cause taxonomy.

## Open questions (update as answered)
- PHMSA key column (expected `REPORT_NUMBER`) and narrative column (expected `NARRATIVE`): confirm from the zip's field description.
- PHMSA file encoding and quoting.
- Does `ai_query` reach a pay-per-token model in this workspace?

## Agent review log
Record each time generated code needed a fix: what was wrong, how it was caught. This is interview material.
- 2026-10-30 — Bronze reader assumed CER was UTF-8. Decoding the live file showed 28 garbled characters
  ("Ile des Ch�nes"); it is windows-1252. Caught by inspecting the source before the first load, not after.
- 2026-10-30 — Plan assumed CER "What Happened / Why It Happened" were narratives. They are coded category
  lists (median 21–24 characters). Caught by profiling value lengths and top values.
- 2026-10-30 — Upload assumed `databricks fs cp` creates missing Volume folders. It does not
  ("no such directory: …/cer/2026-10-30"). Fixed with `fs mkdir` first. Caught on the first real run.
