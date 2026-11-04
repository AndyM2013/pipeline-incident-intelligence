"""Gold: one row per current incident from both regulators, in one set of units, regions and operator keys.

Runs in incident_history, because it reads the SCD2 tables built there. See docs/week3-gold-design.md.

Design choices (decisions 1-3, agreed 2026-11-04):
- Current versions only (__END_AT IS NULL). Withdrawn CER incidents have an __END_AT, so they drop out.
- PHMSA reports barrels of liquid for every commodity, HVL included: 1 US bbl = 0.158987294928 m3 exactly.
  Released volume is the unintentional release. Intentional release (planned blowdowns) has its own column.
- CER m3 are gas volumes for gases. Gas and liquid volumes never share a column. CER HVLs (propane, butane, NGL,
  mixed HVP) are phase unknown: their basis cannot be told from the data, so their volume is in neither column.
- parent_group is the owner today (silver.operator), not the owner at the time of the incident.
- The mapped cause is joined from silver.cause_taxonomy: history does not keep it.
"""

from pyspark.sql import Column
from pyspark.sql import functions as F

try:  # current Lakeflow Spark Declarative Pipelines API
    from pyspark import pipelines as dp
except ImportError:  # older runtimes
    import dlt as dp  # type: ignore[no-redef]

materialized_view = getattr(dp, "materialized_view", dp.table)

BARREL_M3 = "0.158987294928"  # 42 US gal x 0.003785411784 m3, exact; a SQL decimal literal
# CER substances by phase, profiled 2026-11-04 (all current incidents). release_type does not settle the phase:
# CER files every HVL under "Gas" and H2S / SO2 under "Miscellaneous".
CER_GAS = ["Carbon Dioxide", "Fuel Gas", "Hydrogen Sulphide", "Natural Gas - Sour", "Natural Gas - Sweet", "Sulphur Dioxide"]
CER_PHASE_UNKNOWN = [
    "Butane",
    "Mixed HVP Hydrocarbons",
    "Natural Gas Liquids",
    "Propane",
    "Sulphur",  # solid or molten
]
CER_LIQUID = [
    "Amine",
    "Condensate",
    "Contaminated Water",
    "Crude Oil - Sour",
    "Crude Oil - Sweet",
    "Crude Oil - Synthetic",
    "Diesel Fuel",
    "Drilling Fluid",
    "Gasoline",
    "Glycol",
    "Hydraulic Fluid",
    "Jet Fuel",
    "Lube Oil",
    "Odourant",
    "Potassium Carbonate",
    "Potassium Hydroxide (caustic solution)",
    "Produced Water",
    "Pulp slurry",
    "Waste Oil",
    "Water",
]
# CER province -> (country, ISO 3166-2 subdivision). CER regulates a few US segments of cross-border lines.
CER_REGIONS = {
    "Alberta": ("CA", "AB"),
    "British Columbia": ("CA", "BC"),
    "Manitoba": ("CA", "MB"),
    "New Brunswick": ("CA", "NB"),
    "Newfoundland and Labrador": ("CA", "NL"),
    "Northwest Territories": ("CA", "NT"),
    "Nova Scotia": ("CA", "NS"),
    "Nunavut": ("CA", "NU"),
    "Ontario": ("CA", "ON"),
    "Prince Edward Island": ("CA", "PE"),
    "Quebec": ("CA", "QC"),
    "Saskatchewan": ("CA", "SK"),
    "Yukon": ("CA", "YT"),
    "Maine": ("US", "ME"),
}

FACT_EXPECTATIONS = {
    "region_known_unless_federal_waters": "region IS NOT NULL OR offshore",
    "known_phase": "substance IS NULL OR phase IS NOT NULL",
    "one_volume_column": "liquid_released_m3 IS NULL OR gas_released_m3 IS NULL",
    "operator_resolved": "operator_key IS NOT NULL",
    "event_year_matches_date": "event_year = year(event_date)",
}


def literal_map(pairs: dict) -> Column:
    return F.create_map(*[F.lit(x) for pair in pairs.items() for x in pair])


def cer_phase(col: str) -> Column:
    c = F.col(col)
    return F.when(c.isin(CER_GAS), "gas").when(c.isin(CER_LIQUID), "liquid").when(c.isin(CER_PHASE_UNKNOWN), "unknown")


def barrels_to_m3(col: str) -> Column:
    return F.expr(f"cast({col} * {BARREL_M3} AS decimal(18,4))")


def current(table: str):
    return spark.read.table(table).where(F.col("__END_AT").isNull())  # noqa: F821


def cer_facts(operators, taxonomy):
    cause_map = taxonomy.where(F.col("source") == "cer").agg(
        F.map_from_entries(F.collect_list(F.struct("source_cause", "taxonomy_cause"))).alias("cause_map")
    )
    phase = cer_phase("substance")
    volume = F.when(F.col("volume_reported") == "reported", F.col("volume_released_m3"))
    mapped = F.transform("immediate_causes", lambda x: F.try_element_at(F.col("cause_map"), x))
    return (
        current("cer_incident")
        .crossJoin(cause_map)
        .join(operators, (F.col("source_operator") == F.col("company")) & (F.col("source") == "cer"), "left")
        .select(
            F.lit("cer").alias("source"),
            F.col("incident_number").alias("incident_id"),
            F.col("__START_AT").cast("int").alias("version"),
            F.try_element_at(literal_map({k: v[0] for k, v in CER_REGIONS.items()}), F.col("province")).alias("country"),
            F.try_element_at(literal_map({k: v[1] for k, v in CER_REGIONS.items()}), F.col("province")).alias("region"),
            F.lit(None).cast("boolean").alias("offshore"),  # not published in the 16-column file
            F.col("reported_date").alias("event_date"),  # the 16-column file has no occurrence date
            F.year("reported_date").alias("event_year"),
            "operator_key",
            F.col("silver_operator_name").alias("operator_name"),
            "parent_group",
            "substance",
            phase.alias("phase"),
            F.when(phase == "liquid", volume).alias("liquid_released_m3"),
            F.when(phase == "gas", volume).alias("gas_released_m3"),
            F.lit(None).cast("decimal(18,4)").alias("liquid_recovered_m3"),
            F.lit(None).cast("decimal(18,4)").alias("intentional_released_m3"),
            F.col("volume_reported").alias("volume_status"),
            F.array_contains("incident_types", "Fire").alias("fire"),
            F.array_contains("incident_types", "Explosion").alias("explosion"),
            F.array_contains("incident_types", "Fatality").alias("fatality"),
            F.array_contains("incident_types", "Serious Injury").alias("serious_injury"),
            F.array_sort(F.array_distinct(F.filter(mapped, lambda x: x.isNotNull()))).alias("taxonomy_causes"),
            "latitude",
            "longitude",
        )
    )


def phmsa_facts(operators, taxonomy):
    causes = taxonomy.where(F.col("source") == "phmsa").select(
        F.col("source_cause").alias("cause"),
        F.coalesce("source_cause_detail", F.lit("")).alias("_cause_detail"),
        "taxonomy_cause",
    )
    return (
        current("phmsa_incident")
        .withColumn("_cause_detail", F.coalesce("cause_details", F.lit("")))
        .join(causes, ["cause", "_cause_detail"], "left")
        .join(operators, (F.col("source_operator") == F.col("operator_id")) & (F.col("source") == "phmsa"), "left")
        .select(
            F.lit("phmsa").alias("source"),
            F.col("report_number").alias("incident_id"),
            F.col("__START_AT").cast("int").alias("version"),
            F.lit("US").alias("country"),
            # PHMSA's own trend counts offshore incidents in state waters under that state; federal waters: null
            F.coalesce("state", "offshore_state").alias("region"),
            (F.col("on_off_shore") == "OFFSHORE").alias("offshore"),
            F.to_date("local_datetime").alias("event_date"),
            F.col("accident_year").alias("event_year"),
            "operator_key",
            F.col("silver_operator_name").alias("operator_name"),
            "parent_group",
            F.col("commodity_released_type").alias("substance"),
            F.lit("liquid").alias("phase"),  # barrels of liquid, HVL and CO2 included
            barrels_to_m3("unintentional_release_bbls").alias("liquid_released_m3"),
            F.lit(None).cast("decimal(18,4)").alias("gas_released_m3"),
            barrels_to_m3("recovered_bbls").alias("liquid_recovered_m3"),
            barrels_to_m3("intentional_release_bbls").alias("intentional_released_m3"),
            F.when(F.col("unintentional_release_bbls").isNotNull(), "reported").alias("volume_status"),
            F.col("ignited").alias("fire"),
            F.col("exploded").alias("explosion"),
            (F.col("fatalities") > 0).alias("fatality"),
            (F.col("injuries") > 0).alias("serious_injury"),
            F.when(F.col("taxonomy_cause").isNotNull(), F.array("taxonomy_cause")).alias("taxonomy_causes"),
            "latitude",
            "longitude",
        )
    )


@materialized_view(
    name="gold.incident_fact",
    comment="One row per current incident, CER and PHMSA: region, operator, volumes in m3 (liquid and gas apart), "
    "consequences and mapped causes. parent_group is today's owner. See docs/week3-gold-design.md.",
)
@dp.expect_or_fail("incident_id_present", "incident_id IS NOT NULL")
@dp.expect_all(FACT_EXPECTATIONS)
def incident_fact():
    operators = spark.read.table(spark.conf.get("operator_table")).select(  # noqa: F821
        # renamed: PHMSA history has its own operator_name (the name on the report)
        "source", "source_operator", "operator_key", F.col("operator_name").alias("silver_operator_name"), "parent_group"
    )
    taxonomy = spark.read.table(spark.conf.get("cause_taxonomy_table"))  # noqa: F821
    return cer_facts(operators, taxonomy).unionByName(phmsa_facts(operators, taxonomy))
