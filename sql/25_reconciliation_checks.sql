-- Reconciliation against published figures. Run after scripts/fetch_phmsa_trends.py --all-states.
-- The PHMSA export repeats some header names, so columns are read by position (checked on the 2026-11-04 export):
-- _c0 Calendar Year, _c2 # of Incidents, _c8 State Name ("[b] STATE:[/b] TEXAS", blank in the national file).
-- Only the latest export date is compared.

-- 1. PHMSA hazardous liquid incidents by state and accident year, 2010 to last full year.
--    Region = onshore state, else offshore state (state waters). Federal-waters reports have neither and PHMSA
--    counts them in no state. Expect differs = 0 (2026-11-04: 832 cells, 5,792 incidents, 37 federal waters).
WITH state_names(state_name, state) AS (VALUES
  ('ALABAMA', 'AL'),
  ('ALASKA', 'AK'),
  ('ARIZONA', 'AZ'),
  ('ARKANSAS', 'AR'),
  ('CALIFORNIA', 'CA'),
  ('COLORADO', 'CO'),
  ('CONNECTICUT', 'CT'),
  ('DELAWARE', 'DE'),
  ('DISTRICT OF COLUMBIA', 'DC'),
  ('FLORIDA', 'FL'),
  ('GEORGIA', 'GA'),
  ('HAWAII', 'HI'),
  ('IDAHO', 'ID'),
  ('ILLINOIS', 'IL'),
  ('INDIANA', 'IN'),
  ('IOWA', 'IA'),
  ('KANSAS', 'KS'),
  ('KENTUCKY', 'KY'),
  ('LOUISIANA', 'LA'),
  ('MAINE', 'ME'),
  ('MARYLAND', 'MD'),
  ('MASSACHUSETTS', 'MA'),
  ('MICHIGAN', 'MI'),
  ('MINNESOTA', 'MN'),
  ('MISSISSIPPI', 'MS'),
  ('MISSOURI', 'MO'),
  ('MONTANA', 'MT'),
  ('NEBRASKA', 'NE'),
  ('NEVADA', 'NV'),
  ('NEW HAMPSHIRE', 'NH'),
  ('NEW JERSEY', 'NJ'),
  ('NEW MEXICO', 'NM'),
  ('NEW YORK', 'NY'),
  ('NORTH CAROLINA', 'NC'),
  ('NORTH DAKOTA', 'ND'),
  ('OHIO', 'OH'),
  ('OKLAHOMA', 'OK'),
  ('OREGON', 'OR'),
  ('PENNSYLVANIA', 'PA'),
  ('PUERTO RICO', 'PR'),
  ('RHODE ISLAND', 'RI'),
  ('SOUTH CAROLINA', 'SC'),
  ('SOUTH DAKOTA', 'SD'),
  ('TENNESSEE', 'TN'),
  ('TEXAS', 'TX'),
  ('UTAH', 'UT'),
  ('VERMONT', 'VT'),
  ('VIRGINIA', 'VA'),
  ('WASHINGTON', 'WA'),
  ('WEST VIRGINIA', 'WV'),
  ('WISCONSIN', 'WI'),
  ('WYOMING', 'WY')
),
exported AS (
  SELECT _c0 AS calendar_year, try_cast(_c2 AS int) AS incidents,
         trim(regexp_extract(_c8, '\\[/b\\](.*)', 1)) AS state_name,
         regexp_extract(_metadata.file_path, '/([0-9]{4}-[0-9]{2}-[0-9]{2})/', 1) AS export_date
  FROM read_files('/Volumes/pipeline_intel/bronze/landing/published/phmsa_trends/', format => 'csv',
                  header => false, skipRows => 1, fileNamePattern => 'hazardous_liquid_*.csv')
),
published AS (
  SELECT n.state, int(e.calendar_year) AS accident_year, sum(e.incidents) AS published
  FROM exported e JOIN state_names n USING (state_name)
  WHERE e.export_date = (SELECT max(export_date) FROM exported) AND e.calendar_year RLIKE '^[0-9]{4}$'
  GROUP BY ALL
),
ours AS (
  SELECT coalesce(state, offshore_state) AS state, year(local_datetime) AS accident_year, count(*) AS silver
  FROM pipeline_intel.silver.phmsa_incident_snapshot
  WHERE coalesce(state, offshore_state) IS NOT NULL
  GROUP BY ALL
)
SELECT count(*) AS cells, count_if(coalesce(p.published, 0) <> coalesce(o.silver, 0)) AS differs,
       sum(p.published) AS published_incidents, sum(o.silver) AS silver_incidents
FROM published p FULL JOIN ours o USING (state, accident_year)
WHERE accident_year BETWEEN 2010 AND (SELECT max(accident_year) FROM published);

-- 1b. National by accident year. Expect differs = 0 (the national file includes federal waters).
WITH exported AS (
  SELECT _c0 AS calendar_year, try_cast(_c2 AS int) AS incidents,
         regexp_extract(_metadata.file_path, '/([0-9]{4}-[0-9]{2}-[0-9]{2})/', 1) AS export_date
  FROM read_files('/Volumes/pipeline_intel/bronze/landing/published/phmsa_trends/', format => 'csv',
                  header => false, skipRows => 1, fileNamePattern => 'hazardous_liquid_national.csv')
),
published AS (
  SELECT int(calendar_year) AS accident_year, sum(incidents) AS published FROM exported
  WHERE export_date = (SELECT max(export_date) FROM exported) AND calendar_year RLIKE '^[0-9]{4}$'
  GROUP BY 1
)
SELECT p.accident_year, p.published, count(s.report_number) AS silver, p.published - count(s.report_number) AS differs
FROM published p
LEFT JOIN pipeline_intel.silver.phmsa_incident_snapshot s ON year(s.local_datetime) = p.accident_year
WHERE p.accident_year >= 2010
GROUP BY 1, 2
ORDER BY 1;

-- 2. CER's two files: the 16-column file (cer) and the comprehensive file (cer_comprehensive), compared on the
--    dates both were landed (2024-09-04, 2026-11-04). Raw Bronze text, 15 shared columns; the comprehensive file
--    names the coded causes what_happened_category / why_it_happened_category.
--    Expect only_one_file = 0 and differ = 0 for every column except reported_date (format only, see 2b).
--    2026-11-04: 1,887 and 2,039 incidents, 0 differences.
WITH pairs AS (
  SELECT _snapshot_date, incident_number, s.incident_number IS NULL OR c.incident_number IS NULL AS only_one_file,
    map('incident_types', s.incident_types, 'reported_date', s.reported_date,
        'nearest_populated_centre', s.nearest_populated_centre, 'province', s.province, 'company', s.company,
        'status', s.status, 'latitude', s.latitude, 'longitude', s.longitude,
        'volume', s.approximate_volume_released_m3, 'substance', s.substance, 'release_type', s.release_type,
        'significant', s.significant, 'year', s.year, 'what_happened', s.what_happened,
        'why_it_happened', s.why_it_happened) AS a,
    map('incident_types', c.incident_types, 'reported_date', c.reported_date,
        'nearest_populated_centre', c.nearest_populated_centre, 'province', c.province, 'company', c.company,
        'status', c.status, 'latitude', c.latitude, 'longitude', c.longitude,
        'volume', c.approximate_volume_released_m3, 'substance', c.substance, 'release_type', c.release_type,
        'significant', c.significant, 'year', c.year, 'what_happened', c.what_happened_category,
        'why_it_happened', c.why_it_happened_category) AS b
  FROM pipeline_intel.bronze.cer_incident_raw s
  FULL JOIN pipeline_intel.bronze.cer_comprehensive_raw c USING (incident_number, _snapshot_date)
  WHERE _snapshot_date IN (SELECT DISTINCT _snapshot_date FROM pipeline_intel.bronze.cer_comprehensive_raw)
    AND _snapshot_date IN (SELECT DISTINCT _snapshot_date FROM pipeline_intel.bronze.cer_incident_raw)
)
SELECT _snapshot_date, column_name, count(*) AS incidents, count_if(only_one_file) AS only_one_file,
       count_if(NOT only_one_file AND NOT (a[column_name] <=> b[column_name])) AS differ
FROM pairs LATERAL VIEW explode(map_keys(coalesce(a, b))) t AS column_name
GROUP BY 1, 2
ORDER BY 1, 2;

-- 2b. reported_date differs only in format (16-column: 1/2/2008 12:00:00 AM since 2024-09-04; comprehensive:
--     01/02/2008). Expect same = incidents and unparsed = 0.
SELECT _snapshot_date, count(*) AS incidents,
       count_if(try_to_date(split(s.reported_date, ' ')[0], 'M/d/yyyy')
                <=> try_to_date(c.reported_date, 'MM/dd/yyyy')) AS same,
       count_if(try_to_date(c.reported_date, 'MM/dd/yyyy') IS NULL) AS unparsed
FROM pipeline_intel.bronze.cer_incident_raw s
JOIN pipeline_intel.bronze.cer_comprehensive_raw c USING (incident_number, _snapshot_date)
GROUP BY 1
ORDER BY 1;

-- 3. TSB occurrences against CER incidents by year (TSB occurrence date, CER year column). TSB counts reportable
--    occurrences under the TSB Regulations, CER counts incidents under the OPR, so totals differ by definition and
--    are compared, not matched. TSB's 2022 annual summary reports 180 for 2012 and 67 for 2022; this file has 180
--    and 68 (cause of the 1 not checked). P21H0022 is repeated 4 times in the file and counted once.
--    2025-06-13 capture: TSB above CER until 2014, below from 2015 except 2024 (63 vs 62); 2022: 68 vs 148.
WITH t AS (
  SELECT DISTINCT occno, year(to_date(split(occdate, ' ')[0], 'M/d/yyyy')) AS y, occtypeid_displayeng AS occ_type
  FROM pipeline_intel.bronze.tsb_occurrence_raw
  WHERE _snapshot_date = (SELECT max(_snapshot_date) FROM pipeline_intel.bronze.tsb_occurrence_raw)
),
c AS (
  SELECT int(year) AS y, count(*) AS cer_incidents
  FROM pipeline_intel.bronze.cer_incident_raw
  WHERE _snapshot_date = (SELECT max(_snapshot_date) FROM pipeline_intel.bronze.cer_incident_raw)
  GROUP BY 1
)
SELECT t.y AS year, count(*) AS tsb_occurrences, count_if(occ_type = 'ACCIDENT') AS tsb_accidents,
       any_value(c.cer_incidents) AS cer_incidents
FROM t LEFT JOIN c USING (y)
WHERE t.y >= 2008
GROUP BY 1
ORDER BY 1;

-- 3b. The same by province, 2010-2024 (full years in both files). 2025-06-13 capture: every province in both,
--     plus 1 TSB occurrence in Nunavut that CER does not list; NWT 45 vs 10, BC 357 vs 511, Alberta 536 vs 638.
WITH t AS (
  SELECT DISTINCT occno, initcap(provinceid_displayeng) AS province
  FROM pipeline_intel.bronze.tsb_occurrence_raw
  WHERE _snapshot_date = (SELECT max(_snapshot_date) FROM pipeline_intel.bronze.tsb_occurrence_raw)
    AND year(to_date(split(occdate, ' ')[0], 'M/d/yyyy')) BETWEEN 2010 AND 2024
),
c AS (
  SELECT initcap(province) AS province, count(*) AS cer_incidents
  FROM pipeline_intel.bronze.cer_incident_raw
  WHERE _snapshot_date = (SELECT max(_snapshot_date) FROM pipeline_intel.bronze.cer_incident_raw)
    AND int(year) BETWEEN 2010 AND 2024
  GROUP BY 1
)
SELECT coalesce(t.province, c.province) AS province, count(t.occno) AS tsb_occurrences,
       any_value(c.cer_incidents) AS cer_incidents
FROM t FULL JOIN c USING (province)
GROUP BY 1
ORDER BY 2 DESC;
