-- Week 1 acceptance checks. Run after the pipeline update finishes.

-- 1. Every snapshot landed, with plausible row counts and no parse failures.
SELECT 'cer' AS source, _snapshot_date, count(*) AS rows,
       count_if(_rescued_data IS NOT NULL) AS rescued_rows,
       count(DISTINCT _source_file) AS files
FROM pipeline_intel.bronze.cer_incident_raw
GROUP BY _snapshot_date
UNION ALL
SELECT 'phmsa_liquid', _snapshot_date, count(*),
       count_if(_rescued_data IS NOT NULL),
       count(DISTINCT _source_file)
FROM pipeline_intel.bronze.phmsa_liquid_raw
GROUP BY _snapshot_date
ORDER BY source, _snapshot_date;

-- 2. Column names as landed: pick the incident key, dates, cause and narrative columns from here.
DESCRIBE TABLE pipeline_intel.bronze.cer_incident_raw;
DESCRIBE TABLE pipeline_intel.bronze.phmsa_liquid_raw;

-- 3. Candidate key is unique within each snapshot. Replace incident_number / report_number
--    with the real key columns from step 2; any row returned means the key is wrong.
SELECT _snapshot_date, incident_number, count(*) AS n
FROM pipeline_intel.bronze.cer_incident_raw
GROUP BY _snapshot_date, incident_number
HAVING count(*) > 1
LIMIT 20;

SELECT _snapshot_date, report_number, count(*) AS n
FROM pipeline_intel.bronze.phmsa_liquid_raw
GROUP BY _snapshot_date, report_number
HAVING count(*) > 1
LIMIT 20;

-- 4. How much changes between snapshots (the CDC you will model in Week 2).
WITH snaps AS (
  SELECT _snapshot_date, row_number() OVER (ORDER BY _snapshot_date) AS seq
  FROM (SELECT DISTINCT _snapshot_date FROM pipeline_intel.bronze.cer_incident_raw)
),
s AS (
  SELECT r.incident_number, r._snapshot_date, n.seq,
         sha2(to_json(struct(r.* EXCEPT (_snapshot_date, _source_file, _ingested_at, _rescued_data))), 256) AS row_hash
  FROM pipeline_intel.bronze.cer_incident_raw r
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
-- Rows that vanish between snapshots (deleted or re-keyed incidents) are the third case to check in Week 2.
