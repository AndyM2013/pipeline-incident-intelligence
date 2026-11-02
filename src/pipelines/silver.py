"""Silver snapshots: one typed, normalized row per incident per snapshot.

History (SCD2) is built from these views by the incident_history pipeline; see docs/week2-silver-design.md.

Design choices:
- Normalize before diffing. CER changed its date format (2024-09-04) and renamed one incident-type label
  (2025-03-18, 2025-09-01). Left raw, those publication changes would look like 1,651 + ~250 incident updates.
- Lists become sorted arrays, so a reordered list is not a change.
- Known labels are constants here. A label the regulator adds later fails an expectation (warn) instead of
  silently becoming a new category.
- Expectations warn, except a missing key, which stops the update: history cannot be built without it.
- try_cast / try_to_date, because serverless runs ANSI mode: a bad value becomes null and an expectation counts it,
  instead of failing the whole update.
"""

from pyspark.sql import Column
from pyspark.sql import functions as F

try:  # current Lakeflow Spark Declarative Pipelines API
    from pyspark import pipelines as dp
except ImportError:  # older runtimes
    import dlt as dp  # type: ignore[no-redef]

materialized_view = getattr(dp, "materialized_view", dp.table)

# Profiled 2026-11-01 over all 5 CER snapshots.
CER_STATUSES = ["Initially Submitted", "Submitted", "Closed"]
CER_INCIDENT_TYPES = [
    "Adverse Environmental Effects",
    "Explosion",
    "Fatality",
    "Fire",
    "Operation Beyond Design Limits",
    "Release of Substance",
    "Serious Injury",
]
# "Serious Injury (CER or TSB)" -> "(CER)" -> "(as defined in the OPR)": one label, renamed twice.
SERIOUS_INJURY_PATTERN = r"^Serious Injury \(.*\)$"
CER_CAUSES = [
    "Corrosion and Cracking",
    "Defect and Deterioration",
    "Engineering and Planning",
    "Equipment Failure",
    "External Interference",
    "Failure in communication",
    "Human Factors",
    "Inadequate Procurement",
    "Inadequate Supervision",
    "Incorrect Operation",
    "Maintenance",
    "Natural Force Damage",
    "Natural or Environmental Forces",
    "Other Causes",
    "Standards and Procedures",
    "Tools and Equipment",
]
CAUSE_PENDING = "To be determined"
NOT_APPLICABLE = "Not Applicable"
NOT_PROVIDED = "Not Provided"


def label_list(col: str) -> Column:
    """Comma list -> sorted array of trimmed labels; empty -> null."""
    items = F.filter(F.transform(F.split(F.col(col), ","), lambda x: F.trim(x)), lambda x: x != "")
    return F.when(F.size(items) > 0, F.array_sort(items))


def cause_list(col: str) -> Column:
    """Like label_list, but "To be determined" means no cause yet: null, not a category."""
    items = F.filter(label_list(col), lambda x: x != CAUSE_PENDING)
    return F.when(F.size(items) > 0, items)


def not_applicable_to_null(col: str) -> Column:
    return F.when(F.col(col) != NOT_APPLICABLE, F.col(col))


def only_known(col: str, labels: list[str]) -> str:
    quoted = ", ".join("'" + label.replace("'", "\\'") + "'" for label in labels)
    return f"size(array_except(coalesce({col}, array()), array({quoted}))) = 0"


CER_EXPECTATIONS = {
    "key_format": r"incident_number RLIKE '^INC[0-9]{4}-[0-9]{3}$'",
    "reported_date_parsed": "reported_date IS NOT NULL",
    "known_status": "status IN (" + ", ".join(f"'{s}'" for s in CER_STATUSES) + ")",
    "known_incident_types": only_known("incident_types", CER_INCIDENT_TYPES),
    "known_immediate_causes": only_known("immediate_causes", CER_CAUSES),
    "known_basic_causes": only_known("basic_causes", CER_CAUSES),
    "latitude_in_canada": "latitude BETWEEN 41 AND 84",
    "longitude_in_canada": "longitude BETWEEN -142 AND -52",
    "volume_parsed": "volume_reported <> 'unparsed'",
    "volume_not_negative": "volume_released_m3 IS NULL OR volume_released_m3 >= 0",
}


@materialized_view(
    name="silver.cer_incident_snapshot",
    comment="CER incidents, one typed and normalized row per incident per snapshot. Source for SCD2 history.",
)
@dp.expect_or_fail("incident_number_present", "incident_number IS NOT NULL")
@dp.expect_all(CER_EXPECTATIONS)
def cer_incident_snapshot():
    raw = spark.read.table("cer_incident_raw")  # noqa: F821  (spark is provided by the pipeline runtime)
    volume = F.col("approximate_volume_released_m3")
    volume_number = F.expr("try_cast(approximate_volume_released_m3 AS decimal(18,4))")
    incident_types = F.transform(
        label_list("incident_types"),
        lambda x: F.when(x.rlike(SERIOUS_INJURY_PATTERN), F.lit("Serious Injury")).otherwise(x),
    )
    return raw.select(
        "incident_number",
        # Both formats seen: 01/02/2008 and 1/2/2008 12:00:00 AM (time always midnight).
        F.expr("try_to_date(split(reported_date, ' ')[0], 'M/d/yyyy')").alias("reported_date"),
        F.array_sort(F.array_distinct(incident_types)).alias("incident_types"),
        cause_list("what_happened").alias("immediate_causes"),
        cause_list("why_it_happened").alias("basic_causes"),
        "status",
        "province",
        "nearest_populated_centre",
        "company",
        F.expr("try_cast(latitude AS double)").alias("latitude"),
        F.expr("try_cast(longitude AS double)").alias("longitude"),
        volume_number.alias("volume_released_m3"),
        F.when(volume == NOT_APPLICABLE, "not_applicable")
        .when(volume == NOT_PROVIDED, "not_provided")
        .when(volume_number.isNotNull(), "reported")
        .otherwise("unparsed")
        .alias("volume_reported"),
        not_applicable_to_null("release_type").alias("release_type"),
        not_applicable_to_null("substance").alias("substance"),
        (F.col("significant") == "Yes").alias("significant"),
        "_snapshot_date",
    )


# PHMSA hazardous liquid. Profiled 2026-11-01 (one snapshot, 6,052 reports); see docs/week2-silver-design.md.
# Explicit column list: preparer_*, local_contact_*, authorizer_* and the operator's street address never leave Bronze.
PHMSA_CAUSES = [
    "CORROSION FAILURE",
    "EQUIPMENT FAILURE",
    "EXCAVATION DAMAGE",
    "INCORRECT OPERATION",
    "MATERIAL FAILURE OF PIPE OR WELD",
    "NATURAL FORCE DAMAGE",
    "OTHER ACCIDENT CAUSE",
    "OTHER OUTSIDE FORCE DAMAGE",
]
PHMSA_DATETIME_FORMAT = "M/d/yyyy H:mm"
# Narratives quote contact details (1 email, 7 phone numbers in 2026-10-30). Names are not caught here:
# they need an entity-aware pass (ai_mask) before any LLM or Vector Search use.
EMAIL_PATTERN = r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"
PHONE_PATTERN = r"(?:\([0-9]{3}\) ?|\b[0-9]{3}[-. ])[0-9]{3}[-.][0-9]{4}\b"  # (512) 463-6788, 225-925-6595

# Silver name -> Bronze name, by type.
PHMSA_TEXT = {
    "report_number": "report_number",
    "report_type": "report_type",
    "operator_id": "operator_id",
    "operator_name": "name",
    "state": "onshore_state_abbreviation",
    "county": "onshore_county_name",
    "city": "onshore_city_name",
    "on_off_shore": "on_off_shore",
    "location_type": "location_type",
    "incident_area_type": "incident_area_type",
    "pipe_facility_type": "pipe_facility_type",
    "pipeline_function": "pipeline_function",
    "system_part_involved": "system_part_involved",
    "item_involved": "item_involved",
    "material_involved": "material_involved",
    "pipe_type": "pipe_type",
    "pipe_specification": "pipe_specification",
    "pipe_seam_type": "pipe_seam_type",
    "pipe_manufacturer": "pipe_manufacturer",
    "pipe_coating_type": "pipe_coating_type",
    "commodity_released_type": "commodity_released_type",
    "commodity_subtype": "commodity_subtype",
    "release_type": "release_type",
    "leak_type": "leak_type",
    "cause": "cause",
    "cause_details": "cause_details",
}
PHMSA_INT = {
    "supplemental_number": "supplemental_number",
    "accident_year": "iyear",
    "installation_year": "installation_year",
    "manufactured_year": "manufactured_year",
    "pipe_smys_psi": "pipe_smys",
    "fatalities": "fatal",
    "injuries": "injure",
    "num_pub_evacuated": "num_pub_evacuated",
}
PHMSA_DECIMAL = {  # name -> (Bronze name, type)
    "latitude": ("location_latitude", "double"),
    "longitude": ("location_longitude", "double"),
    "pipe_diameter_in": ("pipe_diameter", "decimal(8,3)"),
    "pipe_wall_thickness_in": ("pipe_wall_thickness", "decimal(8,4)"),
    "unintentional_release_bbls": ("unintentional_release_bbls", "decimal(18,2)"),
    "intentional_release_bbls": ("intentional_release_bbls", "decimal(18,2)"),
    "recovered_bbls": ("recovered_bbls", "decimal(18,2)"),
    "est_cost_oper_paid": ("est_cost_oper_paid", "decimal(18,0)"),
    "est_cost_gas_released": ("est_cost_gas_released", "decimal(18,0)"),
    "est_cost_prop_damage": ("est_cost_prop_damage", "decimal(18,0)"),
    "est_cost_emergency": ("est_cost_emergency", "decimal(18,0)"),
    "est_cost_environmental": ("est_cost_environmental", "decimal(18,0)"),
    "est_cost_other": ("est_cost_other", "decimal(18,0)"),
    "total_cost_usd": ("prpty", "decimal(18,0)"),
}
PHMSA_FLAG = {  # YES / NO -> boolean
    "federal_land": "federal",
    "ignited": "ignite_ind",
    "exploded": "explode_ind",
    "water_contaminated": "water_contam_ind",
    "soil_contaminated": "soil_contamination",
    "wildlife_impact": "wildlife_impact_ind",
    "could_be_hca": "could_be_hca",
    "reached_hca": "commodity_reached_hca",
    "shutdown_due_accident": "shutdown_due_accident_ind",
}
PHMSA_TIMESTAMP = {
    "local_datetime": "local_datetime",
    "identified_datetime": "incident_identified_datetime",
    "on_site_datetime": "on_site_datetime",
    "shutdown_datetime": "shutdown_datetime",
    "restart_datetime": "restart_datetime",
}

PHMSA_EXPECTATIONS = {
    "key_format": "report_number RLIKE '^[0-9]{8}$'",
    "report_received_date_parsed": "report_received_date IS NOT NULL",
    "local_datetime_parsed": "local_datetime IS NOT NULL",
    "known_cause": "cause IN (" + ", ".join(f"'{c}'" for c in PHMSA_CAUSES) + ")",
    "cause_details_present": "cause_details IS NOT NULL",
    "latitude_in_us": "latitude BETWEEN 17 AND 72",  # Puerto Rico to Alaska, Gulf offshore included
    "longitude_in_us": "longitude BETWEEN -180 AND -64",
    "volumes_not_negative": "coalesce(unintentional_release_bbls, 0) >= 0 AND coalesce(recovered_bbls, 0) >= 0",
    "narrative_present": "narrative IS NOT NULL",
}


def redact_contacts(col: str) -> Column:
    without_email = F.regexp_replace(F.col(col), EMAIL_PATTERN, "[EMAIL]")
    return F.regexp_replace(without_email, PHONE_PATTERN, "[PHONE]")


@materialized_view(
    name="silver.phmsa_incident_snapshot",
    comment="PHMSA hazardous liquid accidents, one typed row per report per snapshot. No personal contact columns; "
    "emails and phone numbers in the narrative are redacted (names are not).",
)
@dp.expect_or_fail("report_number_present", "report_number IS NOT NULL")
@dp.expect_all(PHMSA_EXPECTATIONS)
def phmsa_incident_snapshot():
    raw = spark.read.table("phmsa_liquid_raw")  # noqa: F821
    columns = [F.trim(F.col(src)).alias(name) for name, src in PHMSA_TEXT.items()]
    columns += [F.expr(f"try_cast({src} AS int)").alias(name) for name, src in PHMSA_INT.items()]
    columns += [F.expr(f"try_cast({src} AS {t})").alias(name) for name, (src, t) in PHMSA_DECIMAL.items()]
    columns += [
        F.when(F.upper(F.col(src)) == "YES", True).when(F.upper(F.col(src)) == "NO", False).alias(name)
        for name, src in PHMSA_FLAG.items()
    ]
    columns += [
        F.expr(f"try_to_timestamp({src}, '{PHMSA_DATETIME_FORMAT}')").alias(name)
        for name, src in PHMSA_TIMESTAMP.items()
    ]
    return raw.select(
        *columns,
        F.expr("try_to_date(report_received_date, 'M/d/yyyy')").alias("report_received_date"),
        redact_contacts("narrative").alias("narrative"),
        "_snapshot_date",
    )
