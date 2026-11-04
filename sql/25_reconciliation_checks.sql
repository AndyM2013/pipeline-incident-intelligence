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
