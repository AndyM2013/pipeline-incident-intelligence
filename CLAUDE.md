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

## Current status (update at the end of every session)
- 2026-11-01: Week 1 done. Bronze loaded for CER (5 snapshots via Internet Archive + live) and PHMSA (1 snapshot);
  full refresh proved Bronze rebuilds from the Volume. Work moves from claude.ai chat to Claude Code in VS Code.
- Next: run check 5 in `sql/10_bronze_checks.sql` to see which CER columns caused the 1,651 "changes" on 2024-09-04,
  then design Week 2: Silver normalizes those columns first, then builds SCD2 from snapshots
  (`create_auto_cdc_from_snapshot_flow`, keys `incident_number` / `report_number`, `stored_as_scd_type=2`).
- Plan and weekly checklist: `docs/project-plan.md` (tick boxes there as work lands).

## Workflow
- Edit and commit on the Mac; GitHub `AndyM2013/pipeline-incident-intelligence` is the source of truth.
- `databricks bundle deploy` uploads to `/Workspace/Users/menglei1025@gmail.com/.bundle/pipeline-incident-intelligence/dev/files`.
  That copy is what the pipeline runs; open SQL files there, never edit them there.
- No workspace Git folder: Free Edition's Git CLI preview causes `.git` Input/output errors and cannot be turned off.
- CLI uses the single `DEFAULT` profile in `~/.databrickscfg`. Workspace: https://dbc-7b9f8b12-ca31.cloud.databricks.com
- Claude Code may run `pytest`, `databricks bundle validate/deploy/run`, and read-only SQL via `databricks` CLI.
  Ask before anything that deletes data, drops tables, or runs a full refresh.

## Source facts (checked 2026-10-30)
- CER CSV: 2,034 incidents, 16 columns, years 2008–2026, encoding windows-1252, no ragged rows.
- CER key: `Incident Number` (unique per row) -> `incident_number` in Bronze.
- CER has NO free-text narrative. `What Happened` (immediate cause) and `Why It Happened` (basic cause) are
  comma-separated category lists, e.g. "External Interference, Incorrect Operation". Treat them as multi-label coded causes.
- The CER data dictionary describes ~100 columns (pipe material, coating, inspection dates…) that the published CSV does not contain.
- Consequence: narratives for extraction and retrieval come from PHMSA; CER supplies Canadian facts and a clean cause taxonomy.

- CER history in Bronze (first load 2026-11-01): 5 snapshots, 1,655 -> 1,887 -> 1,923 -> 1,954 -> 2,034 rows; key unique in each.
  New incidents per snapshot: 236, 36, 32, 81. Changed incidents: 1,651 (2024-09-04 — almost every row, likely a
  format change, see check 5), then 140, 150, 34.
- PHMSA hazardous liquid (2010–present): 6,052 rows, ~650 columns, key `report_number` (unique), `supplemental_number`
  for report revisions. Narrative in `narrative`; coded causes in `cause` and `cause_details` (evaluation labels).
- PHMSA contains personal contact fields: `preparer_*`, `local_contact_*`, `authorizer_*` (names, emails, phones).
  Public data, but they never leave Bronze: Silver selects explicit columns and excludes them; never embedded or sent to an LLM.

## Open questions (update as answered)
- Why did 1,651 of 1,655 CER incidents change between 2022-09-14 and 2024-09-04? Run check 5 in `sql/10_bronze_checks.sql`.
- Does `ai_query` reach a pay-per-token model in this workspace?

## Agent review log
Record each time generated code needed a fix: what was wrong, how it was caught. This is interview material.
- 2026-10-30 — Bronze reader assumed CER was UTF-8. Decoding the live file showed 28 garbled characters
  ("Ile des Ch�nes"); it is windows-1252. Caught by inspecting the source before the first load, not after.
- 2026-10-30 — Plan assumed CER "What Happened / Why It Happened" were narratives. They are coded category
  lists (median 21–24 characters). Caught by profiling value lengths and top values.
- 2026-10-30 — Upload assumed `databricks fs cp` creates missing Volume folders. It does not
  ("no such directory: …/cer/2026-10-30"). Fixed with `fs mkdir` first. Caught on the first real run.
- 2026-11-01 — Column cleaner turned Auto Loader's `_rescued_data` into `rescued_data`, so the expectation
  `_rescued_data IS NULL` failed with UNRESOLVED_COLUMN on the first pipeline run. Unit tests had no system
  column in their inputs. Fixed by keeping system columns verbatim; regression test added. The failed run left a
  stray `rescued_data` column in both tables; removed with a full refresh, which also proved Bronze rebuilds from the Volume.
