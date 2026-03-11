# writers/report_writer.py
# Writes DQ validation summaries to a Delta table for each pipeline run.
# Table: {catalog}.meta.dq_validation_report

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def write_report(
    summary_df: DataFrame,
    target_table: str,
    spark: SparkSession,
) -> None:
    """
    Appends DQ summary to the validation report table, creating it if needed.

    Args:
        summary_df:     DataFrame from evaluate_rules().
        target_table:   Full Unity Catalog path, e.g. "{catalog}.meta.dq_validation_report"
        spark:          Active SparkSession.
    """

    _ensure_report_table_exists(target_table, spark)

    # Use writeTo().append() for serverless compatibility
    (
        summary_df
        .writeTo(target_table)
        .option("mergeSchema", "true")
        .append()
    )

    _print_summary(summary_df, target_table)


def _ensure_report_table_exists(target_table: str, spark: SparkSession) -> None:
    """
    Creates the report table with schema and comments if it does not exist.
    """

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            run_id          STRING    COMMENT 'Pipeline run ID',
            run_timestamp   STRING    COMMENT 'UTC run start timestamp',
            pipeline_name   STRING    COMMENT 'Pipeline name',
            source_table    STRING    COMMENT 'Validated silver table',
            rule_id         STRING    COMMENT 'Rule ID from config',
            column          STRING    COMMENT 'Column validated',
            rule_type       STRING    COMMENT 'Rule type',
            severity        STRING    COMMENT 'error or warning',
            description     STRING    COMMENT 'Rule description',
            total_records   BIGINT    COMMENT 'Total records evaluated',
            passed_records  BIGINT    COMMENT 'Records passed',
            failed_records  BIGINT    COMMENT 'Records failed',
            pass_rate       DOUBLE    COMMENT 'Pass percentage (0-100)'
        )
        USING delta
        COMMENT 'DQ validation report: one row per rule per pipeline run.'
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact'   = 'true'
        )
    """)


def _print_summary(summary_df: DataFrame, target_table: str) -> None:
    """
    Prints a summary of the DQ run to notebook output.
    """

    rows = summary_df.collect()

    print(f"\n{'='*60}")
    print(f"  DQ VALIDATION REPORT  -->  {target_table}")
    print(f"{'='*60}")
    print(f"  {'RULE':<45} {'SEV':<8} {'PASS RATE':>10}")
    print(f"  {'-'*45} {'-'*8} {'-'*10}")

    for row in rows:
        status = "✓" if row["pass_rate"] == 100.0 else "✗"
        print(
            f"  {status} {row['rule_id']:<43} "
            f"{row['severity']:<8} "
            f"{row['pass_rate']:>9.1f}%  "
            f"({row['failed_records']} failed / {row['total_records']} total)"
        )

    errors   = [r for r in rows if r["severity"] == "error"   and r["pass_rate"] < 100]
    warnings = [r for r in rows if r["severity"] == "warning"  and r["pass_rate"] < 100]

    print(f"\n  Errors:   {len(errors)}")
    print(f"  Warnings: {len(warnings)}")
    print(f"{'='*60}\n")