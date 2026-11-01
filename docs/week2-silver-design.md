# Week 2 design — Silver and CDC

Goal: typed Silver tables with SCD2 history built from the dated Bronze snapshots, where a new version means the
regulator changed a fact, not that it changed how it publishes the file.

Status: design, 2026-11-01. Facts below come from profiling `bronze.cer_incident_raw` (5 snapshots, 9,453 rows).

## What check 5 and the profile showed

| Column | What changes between snapshots | Kind |
| --- | --- | --- |
| `reported_date` | `01/02/2008` (2022-09-14) -> `1/2/2008 12:00:00 AM` (2024-09-04 on). All 1,651 incidents. Time is always midnight; `year` always matches. | Format |
| `incident_types` | Comma list. Only one label moves: "Serious Injury (CER or TSB)" -> "(CER)" -> "(as defined in the OPR)". 122 and 124 incidents. | Label |
| `status` | Same three values in every snapshot: Initially Submitted, Submitted, Closed. | Real |
| `what_happened`, `why_it_happened` | "To be determined" replaced by coded causes. 17 cause labels, all present in every snapshot. | Real |
| `latitude`, `longitude`, `approximate_volume_released_m3`, `significant`, `company`, `nearest_populated_centre` | Corrections, e.g. volume 2,400,000 -> 5.98 m³; NOVA -> NGTL rename. | Real |

Other facts:
- 5 incidents disappear for good (INC2022-104/126/136/151 after 2022-09-14, INC2025-055 after 2025-09-01).
  All were Submitted or Initially Submitted when last seen, so they look like withdrawn reports.
- 1 incident disappears and comes back: INC2024-095 is missing from 2025-09-01 and back as Closed in 2026-10-30.
  (First counted as a deletion: the profiling query compared only neighbouring snapshots. SCD2 showed the gap.)
- `incident_number` always matches `INC9999-999`.
- Volume is a number, "Not Applicable" (no release) or "Not Provided".
- `release_type` and `substance` use "Not Applicable" when nothing was released.
- One incident has `province = Maine`. Keep it and flag it.

## Shape

```
incident_ingest pipeline (exists)          incident_history pipeline (new)
  bronze.cer_incident_raw                     silver.cer_incident  (SCD2, streaming table)
  bronze.phmsa_liquid_raw                       <- create_auto_cdc_from_snapshot_flow
  silver.cer_incident_snapshot  (MV)  --------->   source = next_cer_snapshot(latest_version)
  silver.phmsa_incident_snapshot (MV) --------->  silver.phmsa_incident (SCD2)

job incident_refresh: run incident_ingest, then incident_history
```

**Normalize in a materialized view, then build history from it.** `silver.cer_incident_snapshot` keeps one typed,
normalized row per incident per snapshot. Expectations go on this view: the docs say not to put them on the
snapshot-CDC flow. The view is also what check 5 runs against to prove that normalization removed the noise.

**Build history in a second pipeline.** The snapshot source is a Python function that reads a table. A function
like that hides its input from the pipeline's dependency graph. In the same pipeline it could run before the view
refreshes, or fail on the first run because the view doesn't exist yet. A separate pipeline, run after ingest by one
job, has a clear order. Free Edition allows one *active* pipeline per type, and the job runs them one after the
other, so the limit isn't hit.

**History is replayable.** The snapshot function hands over the next snapshot date after `latest_version` until there
are none left. A full refresh of `incident_history` replays all snapshots in order, which keeps the rule that every
layer rebuilds from the landing Volume.

## `silver.cer_incident_snapshot` columns

| Column | Type | Rule |
| --- | --- | --- |
| `incident_number` | string | As is. Key. |
| `reported_date` | date | `to_date(split(reported_date, ' ')[0], 'M/d/yyyy')` handles both formats (0 failures on all 9,453 rows). |
| `incident_types` | array<string> | Split, trim, map every "Serious Injury (...)" to "Serious Injury", `array_sort`. |
| `immediate_causes` | array<string> | From `what_happened`. Split, trim, sort. "To be determined" -> null. |
| `basic_causes` | array<string> | From `why_it_happened`, same rules. |
| `status` | string | As is. |
| `province`, `nearest_populated_centre`, `company` | string | As is. Operator names are resolved in Week 3. |
| `latitude`, `longitude` | double | Cast. |
| `volume_released_m3` | decimal(18,4) | Number or null. |
| `volume_reported` | string | `reported`, `not_applicable` or `not_provided`, so a null volume still says why. |
| `release_type`, `substance` | string | "Not Applicable" -> null. |
| `significant` | boolean | Yes/No. |
| `_snapshot_date` | date | Lineage. Not tracked for history. |

Dropped: `year` (derived from `reported_date`; `sql/20_silver_checks.sql` checks they agree against Bronze).

Sorted arrays make the comparison ignore list order, so reordering a list is not a change.

Expectations (warn, so failures are counted but rows are kept, like Bronze):
- `incident_number` not null and matches `^INC\d{4}-\d{3}$` (fail the update if null: the key must exist).
- `reported_date` not null.
- `status IN ('Initially Submitted', 'Submitted', 'Closed')`.
- `latitude BETWEEN 41 AND 84`, `longitude BETWEEN -142 AND -52`.
- `volume_reported = 'reported'` implies `volume_released_m3 >= 0`.
- Every cause and incident-type label is in the known list. A new label from the regulator shows up as a warning
  instead of quietly becoming a new category.

## `silver.cer_incident` (SCD2)

```python
dp.create_streaming_table("silver.cer_incident")
dp.create_auto_cdc_from_snapshot_flow(
    target="silver.cer_incident",
    source=next_cer_snapshot,           # (latest_version) -> (DataFrame, version) | None
    keys=["incident_number"],
    stored_as_scd_type=2,
    track_history_except_column_list=["_snapshot_date"],
)
```

- **Version:** the snapshot date as an int `yyyymmdd`. The docs only show int versions. `__START_AT` / `__END_AT` are
  then the snapshot where a version was *first seen*, not when the regulator changed it. Document this; it is the
  honest limit of snapshot CDC.
- **Deletes:** keys missing from a later snapshot get closed. The 5 withdrawn incidents come out as rows with
  `__END_AT` set and no later version; INC2024-095 has a gap between two versions.
- **Lineage:** `_snapshot_date` is carried but not tracked. If it were tracked, every incident would get a new
  version in every snapshot.

## PHMSA

One snapshot so far, so its SCD2 table has one version per report. Build it the same way now, so the next PHMSA
download produces history without new code.
- `silver.phmsa_incident_snapshot` selects an explicit list of columns, never `*`. That keeps `preparer_*`,
  `local_contact_*` and `authorizer_*` out of Silver.
- Choosing that list (dates, location, operator, commodity, volumes, `cause`, `cause_details`, `narrative`) needs its
  own profiling pass over the ~650 columns. That is step 4 below.
- Key `report_number`. `supplemental_number` is tracked, so a revised report becomes a new version.

## Acceptance checks (`sql/20_silver_checks.sql`)

1. Rows per snapshot in `cer_incident_snapshot` equal Bronze: 1,655 / 1,887 / 1,923 / 1,954 / 2,034.
2. Check 5 rerun on `cer_incident_snapshot`: `reported_date` and `incident_types` no longer appear.
3. Versions opened per snapshot in `cer_incident` = new + changed from check 2b: 1,655 / 281 / 58 / 60 / 115.
4. Current rows (`__END_AT IS NULL`) = 2,034.
5. Closed with no successor = exactly the 5 withdrawn incidents. This is the deleted-incident report.
   5b. Gaps between versions = INC2024-095 only.
6. No PHMSA personal-contact column exists in any `silver` table (`information_schema.columns`).

## Tests

- Label maps and cause lists live as Python constants in `src/pipelines/silver.py`. Unit tests check them without
  Spark, in the same `ast` style as `tests/test_bronze_names.py`.
- Spark expressions (date parsing, arrays) are checked on the real data by `sql/20_silver_checks.sql`, which avoids
  adding a local Spark/Java dependency.

## Build order

1. `silver.py`: `cer_incident_snapshot` + expectations, added to `incident_ingest`. Run checks 1–2.
2. `history.py` + `incident_history` pipeline + `incident_refresh` job. Run checks 3–5.
3. Write the results into CLAUDE.md and tick Week 2 in the project plan.
4. Profile PHMSA columns, then `phmsa_incident_snapshot` and `phmsa_incident`. Run check 6.

## To verify while building

- ~~Whether a pipeline whose default schema is `bronze` can publish `silver.*` by name.~~ Yes (update d449b4,
  2026-11-01), and `spark.read.table("cer_incident_raw")` makes the view wait for Bronze.
- ~~Whether `__START_AT` holds the int version as expected.~~ Yes: `__START_AT` / `__END_AT` are int `yyyymmdd`.
  One update of `incident_history` replayed all 5 snapshots (job run 503105846552935, 2026-11-01).
- The first call of the snapshot function must return a snapshot. If there are none, the update fails, which is the
  correct outcome for an empty Volume.
