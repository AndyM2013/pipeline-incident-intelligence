-- Week 3 acceptance checks for silver.operator and gold.incident_fact. Run after the incident_refresh job.
-- Design and decisions: docs/week3-gold-design.md.

-- 1. Operators: every source operator resolves, every listed row was seen.
--    Expect cer listed 63 rows / 51 keys; phmsa listed 33 + default 328; unresolved = 0, unseen = 0.
SELECT source, mapping, count(*) AS rows, count(DISTINCT operator_key) AS keys,
       count_if(operator_key IS NULL) AS unresolved, count_if(incidents = 0) AS unseen
FROM pipeline_intel.silver.operator
GROUP BY 1, 2
ORDER BY 1, 2;

-- 2. One fact per current incident. Expect cer 2,039 (2,034 before the 2026-11-04 snapshot) and phmsa 6,052, missing = 0, duplicates = 0.
WITH scd AS (
  SELECT 'cer' AS source, incident_number AS incident_id FROM pipeline_intel.silver.cer_incident WHERE __END_AT IS NULL
  UNION ALL
  SELECT 'phmsa', report_number FROM pipeline_intel.silver.phmsa_incident WHERE __END_AT IS NULL
)
SELECT s.source, count(*) AS current_incidents, count(f.incident_id) AS facts,
       count(*) - count(f.incident_id) AS missing,
       (SELECT count(*) - count(DISTINCT source, incident_id) FROM pipeline_intel.gold.incident_fact) AS duplicates
FROM scd s
LEFT JOIN pipeline_intel.gold.incident_fact f USING (source, incident_id)
GROUP BY 1
ORDER BY 1;

-- 3. Volumes by phase. Expect cer gas 396 / liquid 142 volumes, unknown with no volume in either column
--    (70 incidents, 56 with a volume: 45 HVL + 11 sulphur), no row with both columns; phmsa liquid 6,052.
SELECT source, phase, count(*) AS incidents,
       count_if(volume_status = 'reported') AS volume_reported,
       count(liquid_released_m3) AS liquid_volumes, count(gas_released_m3) AS gas_volumes,
       count_if(liquid_released_m3 IS NOT NULL AND gas_released_m3 IS NOT NULL) AS both
FROM pipeline_intel.gold.incident_fact
GROUP BY 1, 2
ORDER BY 1, 2;

-- 3b. PHMSA barrels -> m3: Gold sums equal the Silver barrel sums converted once. Each row is rounded to 0.0001 m3,
--     so expect |diff| <= rows x 0.00005 (0.30 m3 over 6,052 rows). 2026-11-04: 0.0151 / 0.0115 / -0.0007.
SELECT sum(f.liquid_released_m3) AS gold_released_m3,
       round(sum(p.unintentional_release_bbls) * 0.158987294928, 4) AS silver_released_m3,
       sum(f.liquid_released_m3) - round(sum(p.unintentional_release_bbls) * 0.158987294928, 4) AS released_diff,
       sum(f.liquid_recovered_m3) - round(sum(p.recovered_bbls) * 0.158987294928, 4) AS recovered_diff,
       sum(f.intentional_released_m3) - round(sum(p.intentional_release_bbls) * 0.158987294928, 4) AS intentional_diff
FROM pipeline_intel.gold.incident_fact f
JOIN pipeline_intel.silver.phmsa_incident p ON p.report_number = f.incident_id AND p.__END_AT IS NULL
WHERE f.source = 'phmsa';

-- 3c. CER m3 pass through unchanged into the column of their phase. Expect diff = 0 for gas and liquid.
SELECT f.phase, sum(coalesce(f.liquid_released_m3, f.gas_released_m3)) AS gold_m3,
       sum(c.volume_released_m3) FILTER (WHERE c.volume_reported = 'reported') AS silver_m3,
       sum(coalesce(f.liquid_released_m3, f.gas_released_m3))
         - sum(c.volume_released_m3) FILTER (WHERE c.volume_reported = 'reported') AS diff
FROM pipeline_intel.gold.incident_fact f
JOIN pipeline_intel.silver.cer_incident c ON c.incident_number = f.incident_id AND c.__END_AT IS NULL
WHERE f.source = 'cer' AND f.phase IN ('gas', 'liquid')
GROUP BY 1
ORDER BY 1;

-- 4. Regions. Expect no null region except PHMSA offshore in federal waters (41: the 37 of sql/25 for 2010-2025,
--    plus 4 in 2026), and CER's 1 incident in Maine under US.
SELECT source, country, count(*) AS incidents, count_if(region IS NULL) AS no_region,
       count_if(region IS NULL AND NOT coalesce(offshore, false)) AS no_region_onshore
FROM pipeline_intel.gold.incident_fact
GROUP BY 1, 2
ORDER BY 1, 2;

-- 5. Incidents per parent group per country (today's owner). Expect every group in both countries.
SELECT parent_group, country, count(*) AS incidents
FROM pipeline_intel.gold.incident_fact
WHERE parent_group IS NOT NULL
GROUP BY 1, 2
ORDER BY 1, 2;

-- 5b. Name look-alikes are in no group. Expect 0 rows.
SELECT source, operator_key, operator_name, parent_group
FROM pipeline_intel.gold.incident_fact
WHERE parent_group IS NOT NULL
  AND upper(operator_name) RLIKE 'GREAT SALT PLAINS|HIGH PLAINS|PONY EXPRESS|PERMIAN EXPRESS|ALLIANCE REFINERY|NOVA CHEMICALS'
GROUP BY ALL;

-- 6. Mapped causes. Expect cer mapped 2,001, unmapped 33 (no immediate cause yet), differs = 0;
--    phmsa mapped 6,002 and unmapped 50 (UNKNOWN, excluded from scoring).
SELECT f.source,
       count_if(f.taxonomy_causes IS NOT NULL) AS mapped,
       count_if(f.taxonomy_causes IS NULL) AS unmapped,
       count_if(f.source = 'cer'
                AND coalesce(f.taxonomy_causes, array()) <> coalesce(c.immediate_causes, array())) AS differs
FROM pipeline_intel.gold.incident_fact f
LEFT JOIN pipeline_intel.silver.cer_incident c
  ON f.source = 'cer' AND c.incident_number = f.incident_id AND c.__END_AT IS NULL
GROUP BY 1
ORDER BY 1;
