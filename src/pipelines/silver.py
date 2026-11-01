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
