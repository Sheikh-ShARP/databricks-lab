# writers/quarantine_writer.py
# -----------------------------------------------------------
# Writes failed records to a Delta quarantine table.
#
# Table: {catalog}.meta.dq_quarantine
#
# One row per record per failed rule — append only.
# -----------------------------------------------------------

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def write_quarantine(
    results_df: DataFrame,
    source_df: DataFrame,
    target_table: str,
    spark: SparkSession,
) -> None:
    """
    Filters failed error-severity records and writes them to the
    quarantine table with full source record context.

    Args:
        results_df:     Per-record per-rule results from evaluate_rules().
        source_df:      The original Silver DataFrame (pre-validation).
        target_table:   Full Unity Catalog path, e.g.
                        "{catalog}.meta.dq_quarantine"
        spark:          Active SparkSession.
    """

    _ensure_quarantine_table_exists(target_table, spark)

    # Only quarantine error-severity failures
    failed_df = (
        results_df
        .filter(
            (F.col("passed")   == False) &
            (F.col("severity") == "error")
        )
        .select(
            "_row_key",
            "run_id",
            "run_timestamp",
            "pipeline_name",
            "source_table",
            "rule_id",
            "column",
            "rule_type",
            "severity",
            "description",
            "column_value",
        )
    )

    if _is_empty(failed_df):
        print("  ✓ No error-severity failures - quarantine table unchanged.")
        return

    # Attach the full source record for analyst context
    quarantine_df = (
        failed_df
        .join(
            source_df,
            on="_row_key",
            how="left",
        )
        .withColumn(
            "failure_reason",
            F.concat(
                F.lit("["),   F.col("rule_id"),     F.lit("] "),
                F.col("description"),
                F.lit(" | column: "), F.col("column"),
                F.lit(" | value: "),  F.col("column_value"),
            )
        )
        .withColumn("quarantined_at", F.current_timestamp())
        .withColumn("resolved",       F.lit(False))
        .withColumn("resolved_at",    F.lit(None).cast("timestamp"))
        .withColumn("resolved_note",  F.lit(None).cast("string"))
        .drop("_row_key")
    )

    # ✅ Serverless compatible - use writeTo().append() instead of saveAsTable()
    (
        quarantine_df
        .writeTo(target_table)
        .option("mergeSchema", "true")
        .append()
    )

    _print_quarantine_summary(failed_df, target_table)


def _ensure_quarantine_table_exists(target_table: str, spark: SparkSession) -> None:
    """
    Creates the quarantine table on first run if it does not exist.
    """

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            run_id          STRING      COMMENT 'Pipeline run that produced this failure',
            run_timestamp   STRING      COMMENT 'UTC timestamp of the run',
            pipeline_name   STRING      COMMENT 'Name of the pipeline',
            source_table    STRING      COMMENT 'Silver table the record came from',
            rule_id         STRING      COMMENT 'Rule that this record failed',
            column          STRING      COMMENT 'Column the rule was applied to',
            rule_type       STRING      COMMENT 'Type of rule that failed',
            severity        STRING      COMMENT 'Always error for quarantined records',
            description     STRING      COMMENT 'Human-readable rule description',
            column_value    STRING      COMMENT 'Actual value that caused the failure',
            failure_reason  STRING      COMMENT 'Full human-readable failure description',
            quarantined_at  TIMESTAMP   COMMENT 'When this record was written to quarantine',
            resolved        BOOLEAN     COMMENT 'Whether this failure has been resolved',
            resolved_at     TIMESTAMP   COMMENT 'When the failure was resolved',
            resolved_note   STRING      COMMENT 'Notes on how the failure was resolved'
        )
        USING delta
        COMMENT 'Quarantine table - failed records, one row per record per failed rule.'
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact'   = 'true'
        )
    """)


def _is_empty(df: DataFrame) -> bool:
    """Returns True if the DataFrame has no rows."""
    return df.limit(1).count() == 0


def _print_quarantine_summary(failed_df: DataFrame, target_table: str) -> None:
    """
    Prints a breakdown of quarantined records by rule to the notebook output.
    """

    rows = (
        failed_df
        .groupBy("rule_id", "severity")
        .agg(F.count("*").alias("quarantined_count"))
        .orderBy("rule_id")
        .collect()
    )

    print(f"\n{'='*60}")
    print(f"  QUARANTINE SUMMARY  -->  {target_table}")
    print(f"{'='*60}")
    print(f"  {'RULE':<45} {'COUNT':>8}")
    print(f"  {'-'*45} {'-'*8}")

    total = 0
    for row in rows:
        print(f"  ✗ {row['rule_id']:<43} {row['quarantined_count']:>8}")
        total += row["quarantined_count"]

    print(f"\n  Total quarantined rows: {total}")
    print(f"{'='*60}\n")