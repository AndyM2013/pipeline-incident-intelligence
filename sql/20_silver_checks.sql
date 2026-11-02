-- Week 2 acceptance checks for Silver. Run after the incident_ingest update finishes.
-- Numbers are in docs/week2-silver-design.md.

-- 1. One normalized row per Bronze row, per snapshot. Expect 1,655 / 1,887 / 1,923 / 1,954 / 2,034 and diff = 0.
SELECT b._snapshot_date, b.bronze_rows, s.silver_rows, b.bronze_rows - s.silver_rows AS diff
FROM (SELECT _snapshot_date, count(*) AS bronze_rows FROM pipeline_intel.bronze.cer_incident_raw GROUP BY 1) b
LEFT JOIN (SELECT _snapshot_date, count(*) AS silver_rows FROM pipeline_intel.silver.cer_incident_snapshot GROUP BY 1) s
  USING (_snapshot_date)
ORDER BY 1;

-- 1b. Parsed dates agree with the regulator's own year column. Expect 0 and 0.
SELECT count_if(s.reported_date IS NULL) AS unparsed,
       count_if(year(s.reported_date) <> int(b.year)) AS year_mismatch
FROM pipeline_intel.silver.cer_incident_snapshot s
JOIN pipeline_intel.bronze.cer_incident_raw b USING (incident_number, _snapshot_date);

-- 2. Check 5 on the normalized view: which columns still change between consecutive snapshots?
--    reported_date and incident_types should be gone. What remains is real incident history.
WITH snaps AS (
  SELECT _snapshot_date, row_number() OVER (ORDER BY _snapshot_date) AS seq
  FROM (SELECT DISTINCT _snapshot_date FROM pipeline_intel.silver.cer_incident_snapshot)
),
rows_as_maps AS (  -- built here because '*' is not allowed inside explode()
  SELECT r.incident_number, n.seq, n._snapshot_date,
         from_json(to_json(struct(r.* EXCEPT (_snapshot_date)),
                           map('ignoreNullFields', 'false')),  -- keep nulls so value <-> null changes are counted
                   'map<string,string>') AS m
  FROM pipeline_intel.silver.cer_incident_snapshot r
  JOIN snaps n USING (_snapshot_date)
),
cells AS (
  SELECT incident_number, seq, _snapshot_date, kv.key AS col, kv.value AS val
  FROM rows_as_maps
  LATERAL VIEW explode(m) kv AS key, value
)
SELECT cur._snapshot_date, cur.col,
       count(*) AS incidents_changed,
       any_value(prev.val) AS example_before,
       any_value(cur.val)  AS example_after
FROM cells cur
JOIN cells prev
  ON prev.incident_number = cur.incident_number
 AND prev.col = cur.col
 AND prev.seq = cur.seq - 1
WHERE NOT (prev.val <=> cur.val)
GROUP BY cur._snapshot_date, cur.col
ORDER BY cur._snapshot_date, incidents_changed DESC;

-- 2b. Incidents with at least one real change per snapshot: the version counts SCD2 should produce in step 2.
WITH snaps AS (
  SELECT _snapshot_date, row_number() OVER (ORDER BY _snapshot_date) AS seq
  FROM (SELECT DISTINCT _snapshot_date FROM pipeline_intel.silver.cer_incident_snapshot)
),
s AS (
  SELECT r.incident_number, n.seq, r._snapshot_date,
         sha2(to_json(struct(r.* EXCEPT (_snapshot_date)), map('ignoreNullFields', 'false')), 256) AS row_hash
  FROM pipeline_intel.silver.cer_incident_snapshot r
  JOIN snaps n USING (_snapshot_date)
)
SELECT cur._snapshot_date,
       count_if(prev.incident_number IS NULL)  AS new_or_first_seen,
       count_if(prev.row_hash <> cur.row_hash) AS changed_since_previous
FROM s cur
LEFT JOIN s prev
  ON prev.incident_number = cur.incident_number
 AND prev.seq = cur.seq - 1
GROUP BY cur._snapshot_date
ORDER BY cur._snapshot_date;

-- 3. Versions SCD2 opened per snapshot. Expect new + changed from check 2b:
--    20220914: 1,655  20240904: 281  20250318: 58  20250901: 60  20261030: 115.
SELECT __START_AT AS snapshot_version, count(*) AS versions_opened
FROM pipeline_intel.silver.cer_incident
GROUP BY 1
ORDER BY 1;

-- 4. Current incidents equal the latest snapshot. Expect 2,034.
SELECT count(*) AS current_incidents
FROM pipeline_intel.silver.cer_incident
WHERE __END_AT IS NULL;

-- 5. Deleted incidents: closed, with no current version. Expect 5:
--    INC2022-104/126/136/151 (gone in 20240904), INC2025-055 (gone in 20261030).
SELECT incident_number, max(__END_AT) AS gone_in_snapshot, max_by(status, __START_AT) AS last_status,
       max_by(company, __START_AT) AS company
FROM pipeline_intel.silver.cer_incident
GROUP BY incident_number
HAVING count_if(__END_AT IS NULL) = 0
ORDER BY gone_in_snapshot, incident_number;

-- 5b. Incidents that left the publication and came back: a gap between two versions. Expect 1:
--     INC2024-095, missing from 20250901, back as Closed in 20261030.
SELECT incident_number, __END_AT AS gone_in_snapshot, next_start AS back_in_snapshot
FROM (
  SELECT incident_number, __START_AT, __END_AT,
         lead(__START_AT) OVER (PARTITION BY incident_number ORDER BY __START_AT) AS next_start
  FROM pipeline_intel.silver.cer_incident
)
WHERE next_start > __END_AT
ORDER BY 1;

-- 6. No personal contact column in any Silver table. Expect no rows.
SELECT table_name, column_name
FROM pipeline_intel.information_schema.columns
WHERE table_schema = 'silver'
  AND (column_name LIKE 'preparer%' OR column_name LIKE 'local_contact%' OR column_name LIKE 'authorizer%'
       OR column_name IN ('prepared_date', 'operator_street_address', 'operator_postal_code', 'operator_city_name'));

-- 6b. Narrative redaction. Expect residual_email = 0 and residual_phone = 0;
--     redacted_email = 1 and redacted_phone >= 7 in the 2026-10-30 snapshot.
--     Names are NOT redacted here (ai_mask pass before any LLM or Vector Search use).
SELECT _snapshot_date,
       count_if(narrative RLIKE '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+[.][A-Za-z]{2,}') AS residual_email,
       count_if(narrative RLIKE '([(][0-9]{3}[)] ?|[0-9]{3}[-. ])[0-9]{3}[-.][0-9]{4}') AS residual_phone,
       sum(size(split(narrative, '\\[EMAIL\\]')) - 1) AS redacted_email,
       sum(size(split(narrative, '\\[PHONE\\]')) - 1) AS redacted_phone
FROM pipeline_intel.silver.phmsa_incident_snapshot
GROUP BY 1;

-- 7. PHMSA rows match Bronze, and nothing parseable was lost. Expect diff = 0 and every *_lost = 0.
--    ("UNKNOWN" in year and pipe columns is a legitimate null.)
SELECT s._snapshot_date, count(*) AS silver_rows,
       any_value(r.bronze_rows) - count(*) AS diff,
       count_if(b.installation_year IS NOT NULL AND b.installation_year <> 'UNKNOWN' AND s.installation_year IS NULL) AS install_year_lost,
       count_if(b.pipe_diameter IS NOT NULL AND b.pipe_diameter <> 'UNKNOWN' AND s.pipe_diameter_in IS NULL) AS diameter_lost,
       count_if(b.unintentional_release_bbls IS NOT NULL AND s.unintentional_release_bbls IS NULL) AS volume_lost,
       count_if(b.prpty IS NOT NULL AND s.total_cost_usd IS NULL) AS cost_lost,
       count_if(b.incident_identified_datetime IS NOT NULL AND s.identified_datetime IS NULL) AS identified_lost,
       count_if(b.restart_datetime IS NOT NULL AND s.restart_datetime IS NULL) AS restart_lost
FROM pipeline_intel.silver.phmsa_incident_snapshot s
JOIN pipeline_intel.bronze.phmsa_liquid_raw b USING (report_number, _snapshot_date)
JOIN (SELECT _snapshot_date, count(*) AS bronze_rows FROM pipeline_intel.bronze.phmsa_liquid_raw GROUP BY 1) r
  USING (_snapshot_date)
GROUP BY 1;

-- 8. PHMSA history: one version per report after the first snapshot. Expect 20261030: 6,052 versions, 6,052 current.
SELECT __START_AT AS snapshot_version, count(*) AS versions_opened, count_if(__END_AT IS NULL) AS current_versions
FROM pipeline_intel.silver.phmsa_incident
GROUP BY 1
ORDER BY 1;
