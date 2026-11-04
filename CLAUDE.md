# CLAUDE.md — Pipeline Incident Intelligence

## What this repo is
Personal portfolio project on Databricks Free Edition: CER + PHMSA pipeline incidents -> Bronze/Silver/Gold in Unity Catalog -> LLM cause extraction with evaluation -> Vector Search -> agent with read-only Unity Catalog function tools.

## Rules
- Bronze keeps every source column as text. Types, units and keys are applied in Silver, never in Bronze.
- Every layer is rebuildable from the landing Volume. Never hand-edit tables.
- Snapshots are dated by folder (`<source>/<YYYY-MM-DD>/`); history comes from snapshot diffs, not from guessing.
- Auto Loader tracks files by path and reads each path once. Never overwrite a file in an already-loaded date folder
  (it would be silently ignored); land changed content in a new date folder, or full-refresh Bronze.
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
- 2026-11-01: Check 5 run (CLI and SQLTools in VS Code agree): the 2024-09-04 spike is a `reported_date` format change.
- 2026-11-01: Week 2 step 1 done. `silver.cer_incident_snapshot` (MV in `incident_ingest`) normalizes CER; rows match
  Bronze per snapshot, all 11 expectations pass on 9,453 rows. Changed incidents per snapshot drop from
  1,651 / 140 / 150 / 34 (raw) to 45 / 22 / 28 / 34: only status, causes, coordinates, volume, company remain.
- 2026-11-01: Week 2 step 2 done. `incident_history` pipeline builds `silver.cer_incident` (SCD2, version = int
  yyyymmdd); `incident_refresh` job runs ingest then history (`databricks bundle run incident_refresh`).
  Versions per snapshot 1,655 / 281 / 58 / 60 / 115; 2,034 current; 5 withdrawn incidents; INC2024-095 has a gap.
- 2026-11-02: Week 2 done. `silver.phmsa_incident_snapshot` (62 columns incl. pipe attributes, no contact fields,
  emails/phones in narratives redacted) and `silver.phmsa_incident` (SCD2, 6,052 versions). Checks 6-8 pass.
- 2026-11-02: Week 3 started. `ai_query` and `ai_mask` work (see Open questions). Cause taxonomy approved:
  `reference/cause_taxonomy.csv` maps PHMSA's 41 `cause_details` onto CER's 7 immediate causes
  (5,669 reports direct, 333 judgment calls with rationale, 50 UNKNOWN excluded from scoring).
  CER basic causes have no PHMSA equivalent and are not scored. Equipment Failure is 46% of PHMSA reports,
  so extraction is reported with macro-F1 and a confusion matrix, not accuracy alone.
- 2026-11-04: `silver.cause_taxonomy` (MV in `incident_ingest`, read in Python from the bundle-deployed CSV via the
  `cause_taxonomy_path` setting). Snapshot views gain `taxonomy_causes` (CER) and `taxonomy_cause` / `taxonomy_mapping`
  (PHMSA); every report maps. `history.py` drops them before diffing (`NOT_DIFFED`), so no versions opened. Checks 9-9d pass.
- Next: barrels -> m³ in `gold.incident_fact`, operator resolution (`silver.operator`), reconciliation vs published summaries.
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
  New incidents per snapshot: 236, 36, 32, 81. Changed incidents: 1,651 (2024-09-04 — every one is a `reported_date`
  format change), then 140, 150, 34 (mostly `incident_types` relabels in 2025; see Open questions).
- PHMSA hazardous liquid (2010–present): 6,052 rows, ~650 columns, key `report_number` (unique), `supplemental_number`
  for report revisions. Narrative in `narrative`; coded causes in `cause` and `cause_details` (evaluation labels).
- PHMSA contains personal contact fields: `preparer_*`, `local_contact_*`, `authorizer_*` (names, emails, phones).
  Public data, but they never leave Bronze: Silver selects explicit columns and excludes them; never embedded or sent to an LLM.
- PHMSA narratives quote contact details too (10 emails, 12 phone numbers, people's names). Silver redacts emails
  and phones. Names need an `ai_mask` pass; until then LLM extraction and Vector Search must not read
  `silver.phmsa_incident*.narrative`. 31 reports have no narrative.

## Open questions (update as answered)
- ~~Why did 1,651 of 1,655 CER incidents change between 2022-09-14 and 2024-09-04?~~ Answered 2026-11-01 by check 5:
  `reported_date` changed format from `01/02/2008` to `1/2/2008 12:00:00 AM` in all 1,651 rows. Most of the 140 and 150
  changes in 2025 are `incident_types` relabels ("Serious Injury (CER or TSB)" -> "(CER)" -> "(as defined in the OPR)").
  Real updates are `status`, causes moving from "To be determined" to a code, coordinates, volume corrections, company renames.
- ~~Does `ai_query` reach a pay-per-token model in this workspace?~~ Yes (2026-11-02): 12 READY endpoints incl.
  `databricks-meta-llama-3-3-70b-instruct`, `databricks-llama-4-maverick`, Qwen, GPT-OSS, and embeddings
  (`databricks-gte-large-en`, `databricks-bge-large-en`). `ai_mask(text, array('person'))` masks names in a
  made-up narrative and leaves the facts. No Claude API fallback needed.

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
- 2026-11-01 — Check 5 (per-column snapshot diff) had two bugs. (1) `to_json` drops null fields, so value <-> null
  changes vanished in the inner join on column name; caught by reading the query before running it, fixed with
  `ignoreNullFields=false`. (2) `r.*` inside `LATERAL VIEW explode(...)` fails with INVALID_USAGE_OF_STAR_OR_REGEX;
  the query had never been run. Caught on the first run, fixed by building the map in a CTE first.
- 2026-11-01 — Profiling said 6 CER incidents were deleted; the SCD2 check found 5. The "vanished" query compared
  only neighbouring snapshots, so INC2024-095 (missing from 2025-09-01, back in 2026-10-30) looked deleted.
  Caught by the acceptance check disagreeing with the design doc; added check 5b for gaps.
- 2026-11-02 — Profiling found 1 email in PHMSA narratives; the Silver redaction replaced 10. The profiling regex
  ended in `[a-z]{2,}`, so upper-case addresses ("...@KINDERMORGAN.COM") never matched. Caught because check 6b
  counted redactions instead of only checking that none were left. Lesson: count both sides of a filter.
