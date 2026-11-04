# Week 3 design — conformance: units, operators, `gold.incident_fact`

Status: draft for review, 2026-11-04. The cause taxonomy (step 1) is built; see `reference/cause_taxonomy.csv`.

## What the profile showed (2026-11-04, current snapshots)

### Volumes

- **PHMSA** reports barrels for every commodity, HVL and CO2 included (HVL volume is the liquid volume).
  `unintentional_release_bbls` is never null (34 zeros). `intentional_release_bbls` is non-zero only for HVL (354)
  and CO2 (45): planned blowdowns, not leaks. `recovered_bbls` never exceeds the unintentional volume.
- **CER** reports m³, but for gases the m³ are **gas volume** (Natural Gas - Sweet: median 199 m³, max 16.5 million m³).
  Gas and liquid volumes cannot be summed or compared. Of 2,034 current incidents: 1,188 not applicable, 252 not
  provided, 594 with a volume: 396 gases (natural gas, fuel gas, H2S, SO2, CO2), 45 HVLs, 153 liquids and other.
- CER HVLs (propane, butane, NGL, mixed HVP) are ambiguous: NGL median 0.00075 m³ looks like liquid, propane 37 m³ could
  be either. Their basis cannot be determined from the data.

### Operators

- CER: 63 company strings in all snapshots, 62 after removing punctuation. Variants are wording, not typos:
  "NGTL GP Ltd., as general partner[,] on behalf of NGTL Limited Partnership", "Westcoast Energy Inc." vs
  "Westcoast Energy Inc., carrying on business as Spectra Energy Transmission".
- PHMSA: 361 `operator_id`, 411 names, 416 id-name pairs. The id is stable; names change (EXXONMOBIL PIPELINE CO /
  ... COMPANY LLC; ENBRIDGE PIPELINES (NORTH DAKOTA) LLC / NORTH DAKOTA PIPELINE COMPANY LLC).
- Cross-border overlap is at the **parent** level, never the legal entity: Enbridge, TC Energy / South Bow
  (Keystone), Kinder Morgan (Cochin), Plains, ExxonMobil, Express.
- Name matching would be wrong: "PLAINS" also hits GREAT SALT PLAINS and TESORO HIGH PLAINS, "EXPRESS" hits
  TALLGRASS PONY EXPRESS and PERMIAN EXPRESS, "ALLIANCE" hits PHILLIPS 66 ALLIANCE REFINERY.
- Ownership changes over time: Express joined Enbridge (2013), Kinder Morgan Cochin went to Pembina (2019),
  Keystone moved from TC Energy to South Bow (2024).

## Proposal

### `reference/operator_map.csv` + `silver.operator`

Same pattern as the taxonomy: a reviewed CSV in git, loaded by the pipeline, checked by tests.

| column | meaning |
| --- | --- |
| `source` | cer / phmsa |
| `source_operator` | CER `company` string, or PHMSA `operator_id` |
| `operator_key` | one legal operator, e.g. `ngtl`, `westcoast`, `phmsa:11169` |
| `operator_name` | display name |
| `parent_group` | current parent, only for cross-border groups (else null) |
| `rationale` | why, for every non-obvious row |

- Every CER company string (63) is mapped by hand: small, and the variants need judgment.
- PHMSA operators default to `phmsa:<operator_id>` with their latest name; only the ~40 ids in the candidate groups
  above are listed in the CSV, each confirmed by hand. Unlisted ids need no row.
- `parent_group` is the **current** parent. Incident-time ownership is out of scope; the doc and table comment say so.
- An expectation fails (warn) when a CER company has no row, so a new operator name is noticed, not silently unmapped.

### `gold.incident_fact`

One row per current incident, both regulators, from the SCD2 tables (`__END_AT IS NULL`). Withdrawn CER incidents
(5) are left out. New pipeline library `src/pipelines/gold.py` in `incident_history` (it reads the SCD2 tables).

| column | CER | PHMSA |
| --- | --- | --- |
| `source`, `incident_id` | cer, incident_number | phmsa, report_number |
| `country`, `region` | CA, province | US, state |
| `event_date`, `event_year` | reported_date (no occurrence date published) | local_datetime |
| `operator_key`, `parent_group` | via `silver.operator` | via `silver.operator` |
| `substance` | substance | commodity_released_type |
| `phase` | liquid / gas / unknown, from a substance list | liquid (all PHMSA volumes are liquid barrels) |
| `liquid_released_m3` | volume when phase = liquid | unintentional_release_bbls × 0.158987294928 |
| `gas_released_m3` | volume when phase = gas | null (hazardous liquid data) |
| `liquid_recovered_m3` | null (not published) | recovered_bbls × 0.158987294928 |
| `intentional_released_m3` | null | intentional_release_bbls × 0.158987294928 |
| `fire`, `explosion` | incident_types | ignited, exploded |
| `fatality`, `serious_injury` | incident_types | fatalities > 0, injuries > 0 |
| `taxonomy_causes` | taxonomy_causes | array(taxonomy_cause) |

1 US barrel = 42 US gal = 0.158987294928 m³ exactly. Kept as `decimal(18,4)`; a unit test pins the constant.

### Reconciliation (step 4)

Counts by year and province / state against published figures. What exists (searched 2026-11-04):

- **CER publishes no summary table of its own.** Its dashboards are built from the same CSV, so comparing with them
  would be circular. But CER publishes the data twice (open.canada.ca, both updated 2026-11-02):
  - `pipeline-incidents-data.csv` (dataset fd17f08f): the 16-column file we load.
  - `pipeline-incidents-comprehensive-data.csv` (dataset 7dffedc4): 102 columns, 2,039 rows (5 more than ours),
    key unique, no ragged rows. This is the file the ~100-column data dictionary describes.
  Comparing the two files is a real reconciliation: same publisher, two products.
- **TSB** (Transportation Safety Board) publishes an annual pipeline statistical summary, an independent count for
  federally regulated pipelines, in HTML tables: occurrences by year (Table 1) and by province (Table 2), plus
  product type and volume bands. Editions 2021-2025 are listed. TSB counts "reportable occurrences" under its own
  rules, so totals differ by design: 2022 TSB 67 vs CER 148, 2012 TSB 180 vs CER 159. The check is to explain the
  gap by definition, not to match. The 2025 edition blocks scripted download and is not archived. It would have
  to be saved from a browser and landed by hand (2022 is readable).
- **PHMSA** "Pipeline Incident 20 Year Trends" (All Reported, filter System Type = Hazardous Liquid, by state) is an
  Oracle Analytics dashboard that needs JavaScript and a session. It cannot be fetched by script. Export to Excel
  from a browser, then land it in the Volume like any snapshot. Its counts come from the same flagged files we load,
  so differences come from filters (onshore / offshore, gravity lines, report date vs accident year).

### Finding: CER does publish narratives

The comprehensive file has `Detailed what happened` and `Detailed why it happened` (99% filled, median 76 and
92 characters, p90 186 and 263; no emails or phone numbers found) next to the coded categories. It also has
`Occurrence Date and Time` (75% filled), `Year`, `Number of fatalities`, `Number of individuals injured`,
`Released volume (m3)`, `Released substance type`, and the pipe and inspection attributes (5-27% filled).
CLAUDE.md says CER has no narrative: that is true of the 16-column file only.

Consequence if we switch the CER source: a second, native evaluation set. The LLM classifies CER's own short
narratives into CER's own causes, with no mapping judgment. That is cleaner ground truth than PHMSA plus the
taxonomy. History is not lost: the Internet Archive has 5 distinct captures (2024-03-30, 2024-07-15, 2024-09-04,
2025-04-21, 2026-09-02) plus the live file. Cost: a new Bronze table and a Silver view per file, and a re-profile
(the date format already differs: `Reported Date` is `08/10/2023` here).

## Decisions needed

1. **Liquid volume for PHMSA.** Use unintentional only (proposed: comparable with a CER spill; blowdowns are
   planned) or unintentional + intentional.
2. **CER HVLs** (propane, butane, NGL, mixed HVP: 45 incidents with a volume). Proposed: `phase = unknown`, volume in neither
   column. Alternative: treat as liquid and accept some gas volumes in the liquid total.
3. **Parent group as of today**, not as of the incident (proposed), or skip parent groups altogether.
4. **CER comprehensive file.** Decided 2026-11-04: add it as a second CER source, keep the 16-column file and its
   history, land the comprehensive file and its 5 archive captures as `cer_comprehensive/<date>/`, reconcile the two.
5. **Published summaries.** PHMSA: scripted, done 2026-11-04 (`scripts/fetch_phmsa_trends.py`, below).
   TSB 2025: still needs saving from a browser.

### PHMSA trend export (`scripts/fetch_phmsa_trends.py`)

The dashboard refuses stateless export URLs from an anonymous session, so the script drives it in headless Chromium:
public entry page (anonymous session), then the dashboard with `Action=Navigate&P0..` filters (System Sub Type =
HAZARDOUS LIQUID, optionally State Name), then the table's own Export > CSV. The export's label columns (`SYS`,
`State Name`) must show the requested filters, or the file is refused. Lands in
`published/phmsa_trends/<date>/` with `export_info.json` (dashboard "Data as of" date, rows per file).
The export's grain is calendar year x onshore / offshore, 2006-2025: incidents, fatalities, injuries, cost,
barrels spilled, net barrels lost, plus 3/5/10/20-year averages.

First run (data as of 2026-11-03): national hazardous liquid incidents equal our Silver counts by accident year for
every year 2010-2025. Texas equals ours in every year except 2021 (published 174, Silver 173): the first real
reconciliation item. `--all-states` exports every state, about a minute each.

## Acceptance checks (to add as `sql/30_gold_checks.sql`)

- Rows: CER current minus withdrawn, plus PHMSA current. No duplicate (`source`, `incident_id`).
- Sum of `liquid_released_m3` per source equals the Silver sum after conversion (to the cent).
- No row with both a liquid and a gas volume.
- Every CER company resolves to an `operator_key`. Report incidents per `parent_group` per country.
- The false-positive names above are not in any parent group (also a unit test).
