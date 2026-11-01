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
