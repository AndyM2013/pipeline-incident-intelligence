# Week 3 design — conformance: units, operators, `gold.incident_fact`

Status: 2026-11-05. Built: cause taxonomy (`reference/cause_taxonomy.csv`), operator map (`reference/operator_map.csv`,
`silver.operator`), `gold.incident_fact`, PHMSA reconciliation. CER comprehensive file landed and reconciled (Bronze only, decision 4b). TSB occurrence data in Bronze (June 2025 archive copy) and reconciled by year and
province; today's copy waits on a browser download. `silver.tsb_occurrence_snapshot` built (decision 6). Open: decision 7.

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
- Cross-border overlap is at the **parent** level, never the legal entity. Candidates were Enbridge, TC Energy /
  South Bow (Keystone), Kinder Morgan / Pembina (Cochin), Plains, ExxonMobil, Express. Checked against today's owners,
  five groups remain (see below); TC Energy, Kinder Morgan and Plains are no longer on both sides.
- Name matching would be wrong: "PLAINS" also hits GREAT SALT PLAINS and TESORO HIGH PLAINS, "EXPRESS" hits
  TALLGRASS PONY EXPRESS and PERMIAN EXPRESS, "ALLIANCE" hits PHILLIPS 66 ALLIANCE REFINERY.
- Ownership changes over time: Express went from Kinder Morgan to Spectra (2013) and with Spectra to Enbridge (2017);
  Kinder Morgan Canada and the US part of Cochin went to Pembina (closed 2019-12-17); Keystone moved from TC Energy to
  South Bow (2024); Pembina bought out Enbridge's half of Alliance (2024-04); Keyera bought Plains Midstream Canada
  ULC, Plains' Canadian NGL business (closed 2026-05-12).

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
- PHMSA operators default to `phmsa:<operator_id>` with their latest name; only the 33 ids in the candidate groups
  above are listed in the CSV, each confirmed by hand (including the name look-alikes, listed with no group so the
  decision is visible). Unlisted ids need no row.
- `parent_group` is the **current** parent. Incident-time ownership is out of scope; the doc and table comment say so.
- An expectation fails (warn) when a CER company has no row, so a new operator name is noticed, not silently unmapped.
  A second one warns when a CSV row matches nothing (a typo).

Built 2026-11-04: 63 CER strings -> 51 operators; 361 PHMSA ids (33 listed, 328 default). A group is set only when
today's owner is confirmed and the group has members in both countries:

| parent_group | CER operators / incidents | PHMSA ids / reports |
| --- | --- | --- |
| Enbridge | 7 / 627 (Mainline, NW, Southern Lights, Express, Westcoast) | 10 / 234 |
| Pembina | 8 / 44 (Alliance, Cochin, Pouce Coupé, Provident, ...) | 1 / 9 (Cochin US) |
| South Bow | 2 / 38 (Keystone) | 1 / 31 |
| ExxonMobil | 1 / 3 | 7 / 135 |
| Portland-Montreal Pipe Line | 1 / 9 | 1 / 2 |

Left out until checked (rationale in the CSV): Maritimes & Northeast, the two Spectra midstream entities, Plains
Marketing Canada, Enbridge Pipelines (East Texas), ExxonMobil West Coast, Mobil Pacific Pipeline. Ozark (sold to MPLX,
2017) and Westspur (now Kingston Midstream) are no longer Enbridge.

### `gold.incident_fact`

One row per current incident, both regulators, from the SCD2 tables (`__END_AT IS NULL`). Withdrawn CER incidents
(5) are left out. New pipeline library `src/pipelines/gold.py` in `incident_history` (it reads the SCD2 tables).

| column | CER | PHMSA |
| --- | --- | --- |
| `source`, `incident_id` | cer, incident_number | phmsa, report_number |
| `country`, `region` | ISO code of the province (1 CER incident is in Maine: US) | US, state, else offshore state |
| `offshore` | null (not in the 16-column file) | on_off_shore = OFFSHORE |
| `event_date`, `event_year` | reported_date (no occurrence date published) | local_datetime, accident_year |
| `operator_key`, `parent_group` | via `silver.operator` | via `silver.operator` |
| `substance` | substance | commodity_released_type |
| `phase` | liquid / gas / unknown, from a substance list | liquid (all PHMSA volumes are liquid barrels) |
| `volume_status` | reported / not_provided / not_applicable | reported |
| `liquid_released_m3` | volume when phase = liquid | unintentional_release_bbls × 0.158987294928 |
| `gas_released_m3` | volume when phase = gas | null (hazardous liquid data) |
| `liquid_recovered_m3` | null (not published) | recovered_bbls × 0.158987294928 |
| `intentional_released_m3` | null | intentional_release_bbls × 0.158987294928 |
| `fire`, `explosion` | incident_types | ignited, exploded |
| `fatality`, `serious_injury` | incident_types | fatalities > 0, injuries > 0 |
| `taxonomy_causes` | taxonomy_causes | array(taxonomy_cause) |

1 US barrel = 42 US gal = 0.158987294928 m³ exactly. Kept as `decimal(18,4)`; a unit test pins the constant.

CER `release_type` (Gas / Liquid / Miscellaneous) cannot set the phase: it is fixed per substance, files every HVL
under Gas and H2S / SO2 under Miscellaneous. Phase lists are constants in `src/pipelines/gold.py`: gas (natural gas,
fuel gas, H2S, SO2, CO2), unknown (propane, butane, NGL, mixed HVP, sulphur: solid or molten), liquid (the other 20).

Built 2026-11-04 in `incident_history` (`src/pipelines/gold.py`): 8,086 rows (CER 2,034, PHMSA 6,052). CER volumes:
396 gas, 142 liquid, 56 phase unknown in neither column.

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
  gap by definition, not to match. Superseded 2026-11-05 by the occurrence-level file below.
- **PHMSA** "Pipeline Incident 20 Year Trends" (All Reported, filter System Type = Hazardous Liquid, by state) is an
  Oracle Analytics dashboard that needs JavaScript and a session. It cannot be fetched by script. Export to Excel
  from a browser, then land it in the Volume like any snapshot. Its counts come from the same flagged files we load,
  so differences come from filters (onshore / offshore, gravity lines, report date vs accident year).

### Finding: the comprehensive file has detailed cause codes, not narratives (corrected 2026-11-04)

A first profile of `Detailed what happened` / `Detailed why it happened` (99% filled, median 76 and 92 characters)
was read as short narratives. Reading the values after loading showed they are **coded sub-categories**: a
semicolon list of comma paths, e.g. "Damage or deterioration mechanism, Equipment, Valve Seals or Packing" or
"Job or system factors, Inadequate maintenance, Inadequate preventive, Assessment of needs". 6,531 items in the
2026-11-04 file, 228 distinct, 1 ending like a sentence. CER publishes no free-text narrative in either file.

What the comprehensive file does add over the 16-column file: a finer, hierarchical cause coding (228 sub-codes
under the 16 categories); `Occurrence Date and Time` (75% filled, with a time zone) and `Discovered Date and Time`;
counts (`Number of fatalities` 6, `Number of individuals injured` 125, people evacuated); `Country`; pipeline and
facility names; and pipe, coating, inspection and maintenance attributes (2-27% filled).

So the "native CER evaluation set" consequence does not hold: LLM extraction stays PHMSA-only, scored through the
taxonomy. Landed 2026-11-04 as `cer_comprehensive/<date>/` (5 archive captures 2024-03-30 to 2026-09-02, plus
2026-11-04) into `bronze.cer_comprehensive_raw`: 1,862 -> 2,039 rows, key unique, 0 rescued, windows-1252.

### Reconciliation: CER 16-column file vs comprehensive file (`sql/25`, checks 2 and 2b)

Compared on the two dates both were landed (2024-09-04: 1,887 incidents; 2026-11-04: 2,039; today's 16-column file
was landed as a normal new snapshot for this). Same incidents in both files, and all 15 shared columns identical,
except `reported_date`, which differs in format only (parsed dates equal for every incident). The 16-column file is
an exact projection of the comprehensive file.

### TSB occurrence data (added 2026-11-05)

TSB publishes the rows behind its summaries: "Pipeline transportation occurrence data from January 1979"
(open.canada.ca dataset `3f273148-3aaf-4194-b846-04fbf524e476`): `PODSdb_MDOTW_VW_OCCURRENCE_PUBLIC.csv`, an
injuries file and a data dictionary. tsb.gc.ca answers every scripted request with 403 from its application
gateway (summary pages, CSVs and `robots.txt`), and we do not disguise the client. So:

- The Internet Archive's one capture (2025-06-13) is landed as `tsb/2025-06-13/` with `--wayback 20250613`; the
  dictionary is not archived.
- Today's copy is saved from a browser and landed with `scripts/fetch_snapshots.py --source tsb --from-dir <dir>`
  (same manifest, same dated folder; the date defaults to today).
  `--browser` does the clicking: it opens each link in the user's own browser, waits for the finished download in
  `~/Downloads` (ignoring partial `.crdownload` / `.download` / `.part` files and older copies), then lands it.
- The injuries file is not landed: the occurrence file has `TotalFatalInjuries` / `TotalSeriousInjuries`.

Profile of the 2025-06-13 capture, read in `bronze.tsb_occurrence_raw`: UTF-8 with BOM (not windows-1252 like CER),
117 columns (each code with English and French labels), 3,343 rows, 3,340 occurrences (`P21H0022` repeated 4 times,
identical rows), 1979-2025, 0 rescued rows.

**`OccSummary` is a free-text narrative**, the first Canadian one in the project: 3,338 filled, median 282
characters, 3,190 ending like a sentence, and read as prose in samples ("A technician responded to an emergency shut
down at the Ile Des Chenes Compressor Station ... causing a fuel gas release from a 1/2 inch line"). No emails;
the 5 phone-like matches are NEB safety-order numbers. Names are not checked, so the `ai_mask` rule for PHMSA
narratives applies here too. TSB has **no coded cause**: the threat / hazard fields are blank in 97% / 87% of
2010+ rows.

Reconciliation (`sql/25`, checks 3 and 3b): totals differ by definition (TSB reportable occurrences vs CER OPR
incidents). TSB counts more until 2014 and fewer from 2015 (2022: 68 vs 148); the file reproduces TSB's own
summary for 2012 (180) and differs by 1 for 2022 (68 vs 67). By province 2010-2024, NWT (45 vs 10), BC (357 vs 511)
and Alberta (536 vs 638) differ most.

There is no shared key. A trial match on occurrence date (CER occurrence date, else reported date) within 1 day,
province and the first 6 letters of the operator name finds exactly one CER incident for 36-66% of TSB occurrences
per year (2010-2024), several for up to 31 per year. That is a heuristic link, not a reconciliation: decision 7.

### First reconciliation: PHMSA states (all 52, run 2026-11-04, data as of 2026-11-03)

847 state x year cells (2010-2025) compared with Silver; 31 differ, all explained by one cause:

- Silver's `state` is `onshore_state_abbreviation` only. PHMSA's trend assigns offshore incidents in state waters to
  that state (`offshore_state_abbreviation`): 19 LA, 1 CA (2022), 1 TX (2021, the Texas off-by-one above).
- The other 37 offshore reports have no state in either column (federal waters); PHMSA counts them in no state.

Fixed 2026-11-04: `silver.phmsa_incident_snapshot` adds `offshore_state`; `incident_history` was fully refreshed
once (history identical: CER 1,655 / 281 / 58 / 60 / 115 versions, 5 withdrawn, INC2024-095 gap; PHMSA 6,052).
`gold.incident_fact.region` will be onshore state, else offshore state. `sql/25_reconciliation_checks.sql` now
compares all 832 state x year cells and the national years: 0 differences, 5,792 incidents, 37 in federal waters.

## Decisions needed

1. **Liquid volume for PHMSA.** Decided 2026-11-04: unintentional only (comparable with a CER spill; blowdowns are
   planned). Intentional release has its own column.
2. **CER HVLs** (propane, butane, NGL, mixed HVP: 45 incidents with a volume). Decided 2026-11-04: `phase = unknown`,
   volume in neither column. Sulphur (11 with a volume) is unknown too.
3. **Parent group as of today**, not as of the incident. Decided 2026-11-04. Consequence: Plains has no group
   (Canadian business sold 2026-05), and Ozark's Enbridge-era reports count under no group.
4. **CER comprehensive file.** Decided 2026-11-04: add it as a second CER source, keep the 16-column file and its
   history, land the comprehensive file and its 5 archive captures as `cer_comprehensive/<date>/`, reconcile the two.
   Done 2026-11-04 (Bronze and reconciliation).
4b. **Silver for the comprehensive file.** The decision above assumed narratives, which it does not have.
   Options were (a) a narrow `silver.cer_incident_detail` snapshot view (occurrence datetime, counts, detailed cause
   codes, pipe attributes) joined to the 16-column history by key, or (b) leave it in Bronze until a use needs it.
   Decided 2026-11-05: (b). Nothing in Weeks 4-7 reads it; Bronze keeps every snapshot and rebuilds from the Volume.
   Revisit if Gold needs CER occurrence time instead of reported date, or the agent needs fatality / injury counts.
5. **Published summaries.** PHMSA: scripted, done 2026-11-04 (`scripts/fetch_phmsa_trends.py`, below).
   TSB: superseded by the occurrence file, landed 2026-11-05 from the June 2025 archive copy; today's copy needs
   saving from a browser (the site refuses scripted clients).
6. **TSB Silver.** Options were (a) `silver.tsb_occurrence_snapshot` (typed, deduplicated, `OccSummary` with emails
   and phones redacted like PHMSA) so TSB narratives can join the Week 4 `ai_mask` pass and later retrieval, or
   (b) Bronze only. Decided 2026-11-05: (a); built on the 2025-06-13 capture, today's copy becomes a second snapshot.
   - 61 columns: English labels only (no ids, no French), `True`/`False` indicators as booleans, `occurrence_date`
     as a date with `occurrence_time` and `time_zone` apart, `summary` = redacted `OccSummary`.
   - Identical repeated rows collapse (3,343 Bronze rows -> 3,340); `copies` and `key_unique_in_snapshot` catch a
     key repeated with different values.
   - Coordinates are the nearest named place (`nearest_location_*`), not the occurrence position; 228 blank.
   - `quantity_lost` / `quantity_recovered` carry no unit until the data dictionary is landed.
   - Checks 10-10b (`sql/20`) pass: nothing parseable lost, 0 emails, 0 phones, the 5 safety-order numbers kept.
     Expectations fail only where the profile said: 5 without a summary, P00Z0001 (type N/A, no province).
   - No SCD2 yet: with one snapshot there is no history. Add a `silver.tsb_occurrence` flow in `incident_history`
     once a second snapshot lands, if versions are needed.
7. **TSB-CER link.** Open, after decision 6. A dated, rule-based match (date, province, operator) would attach CER coded causes to TSB
   narratives: a Canadian evaluation set for cause extraction. Needs its own design: match rules, ambiguous
   candidates, and a manual review sample before any score uses it.

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
reconciliation item. `--all-states` exports every state one after another, about 10 seconds each (~9 minutes).

## Acceptance checks (`sql/30_gold_checks.sql`, all pass 2026-11-04)

- Operators: 0 unresolved, 0 listed rows unseen.
- Rows: CER 2,034 current (withdrawn have an `__END_AT`), PHMSA 6,052. 0 missing, 0 duplicate (`source`, `incident_id`).
- PHMSA m³ sums equal the Silver barrel sums converted once, within per-row rounding (0.0151 m³ on 220,816 m³).
  CER gas and liquid m³ pass through with diff 0.
- No row with both a liquid and a gas volume.
- Region null only offshore in federal waters (41: 37 in 2010-2025 as in `sql/25`, 4 in 2026).
- Every parent group has incidents in both countries; the name look-alikes are in none (also unit tests).
- Mapped causes: CER equal to immediate causes; PHMSA 6,002 mapped, 50 UNKNOWN.
