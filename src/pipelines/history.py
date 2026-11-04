"""SCD2 history from the normalized Silver snapshot views.

Runs as its own pipeline (incident_history), after incident_ingest, from the incident_refresh job.
The snapshot function reads a table, which hides that dependency from the pipeline graph, so the two
cannot safely share one pipeline. See docs/week2-silver-design.md.

Design choices:
- Version = snapshot date as int yyyymmdd. __START_AT / __END_AT therefore say in which snapshot a version was
  first and last seen, not when the regulator changed it: the honest limit of snapshot CDC.
- A full refresh replays every snapshot in date order, so history rebuilds from the landing Volume.
- Keys missing from a later snapshot are deleted: their last version gets an __END_AT and no successor.
- _snapshot_date is dropped before diffing; it differs in every snapshot and would make every row a new version.
- The mapped cause (taxonomy_*) is dropped too: it is our interpretation, not the regulator's. Join
  silver.cause_taxonomy when history needs it.
"""

from datetime import date

from pyspark.sql import functions as F

try:  # current Lakeflow Spark Declarative Pipelines API
    from pyspark import pipelines as dp
except ImportError:  # older runtimes
    import dlt as dp  # type: ignore[no-redef]

# target table in this pipeline's schema -> (snapshot view, keys)
HISTORIES = {
    "cer_incident": ("cer_snapshot_table", ["incident_number"]),
    "phmsa_incident": ("phmsa_snapshot_table", ["report_number"]),
}
# Columns of the snapshot views that are not part of the regulator's record.
NOT_DIFFED = ["_snapshot_date", "taxonomy_causes", "taxonomy_cause", "taxonomy_mapping"]


def version_of(snapshot_date: date) -> int:
    return snapshot_date.year * 10000 + snapshot_date.month * 100 + snapshot_date.day


def next_snapshot(table: str):
    """Snapshot source: the earliest snapshot newer than latest_version, or None when there are no more."""

    def _next(latest_version):
        snapshots = spark.read.table(table)  # noqa: F821  (spark is provided by the pipeline runtime)
        dates = snapshots.select("_snapshot_date").distinct()
        if latest_version is not None:
            dates = dates.where(F.date_format("_snapshot_date", "yyyyMMdd").cast("int") > latest_version)
        next_date = dates.agg(F.min("_snapshot_date")).first()[0]
        if next_date is None:
            return None
        return snapshots.where(F.col("_snapshot_date") == next_date).drop(*NOT_DIFFED), version_of(next_date)

    return _next


def define_history(target: str, table_conf: str, keys: list[str]) -> None:
    source = spark.conf.get(table_conf)  # noqa: F821
    dp.create_streaming_table(name=target, comment=f"SCD2 history of {source}, one row per version.")
    dp.create_auto_cdc_from_snapshot_flow(
        target=target,
        source=next_snapshot(source),
        keys=keys,
        stored_as_scd_type=2,
    )


for _target, (_table_conf, _keys) in HISTORIES.items():
    define_history(_target, _table_conf, _keys)
