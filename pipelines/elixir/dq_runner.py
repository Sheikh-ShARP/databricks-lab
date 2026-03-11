
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from engine.rule_evaluator import evaluate_rules
from writers.report_writer import write_report
from writers.quarantine_writer import write_quarantine

def run_dq(
    spark: SparkSession,
    rules_config: dict,
    pipeline_name: str,
    report_table: str,
    quarantine_table: str,
    input_df: DataFrame = None,
) -> dict:
    # Extract source table and rules from config
    source_table = rules_config.SOURCE_TABLE
    rules        = rules_config.RULES

    print(f"\n{'='*60}")
    print(f"  STARTING DQ RUN")
    print(f"  Pipeline : {pipeline_name}")
    print(f"  Table    : {source_table}")
    print(f"  Rules    : {len(rules)}")
    print(f"{'='*60}\n")

    # Use provided DataFrame or load from table
    if input_df is not None:
        silver_df = input_df
        print("  ✓ Using provided DataFrame.\n")
    else:
        silver_df = spark.table(source_table)
        print(f"  ✓ Loaded table: {source_table}\n")

    print("  Running rule evaluation...\n")
    
    # Add unique row key for tracking
    silver_df = silver_df.withColumn("_row_key", F.monotonically_increasing_id())

    # Evaluate rules and get results
    results_df, summary_df = evaluate_rules(
        df            = silver_df,
        rules         = rules,
        source_table  = source_table,
        pipeline_name = pipeline_name,
        spark         = spark,
    )

    print("  Writing quarantine records...\n")
    # Write failed records to quarantine table
    write_quarantine(
        results_df   = results_df,
        source_df    = silver_df,
        target_table = quarantine_table,
        spark        = spark,
    )

    print("  Writing validation report...\n")
    # Write summary report
    write_report(
        summary_df   = summary_df,
        target_table = report_table,
        spark        = spark,
    )

    summary_rows = summary_df.collect()

    # Identify error and warning failures
    error_failures   = [
        r for r in summary_rows
        if r["severity"] == "error" and r["failed_records"] > 0
    ]
    warning_failures = [
        r for r in summary_rows
        if r["severity"] == "warning" and r["failed_records"] > 0
    ]

    has_errors   = len(error_failures)   > 0
    has_warnings = len(warning_failures) > 0

    # Prepare result summary
    result = {
        "run_id":           summary_rows[0]["run_id"],
        "pipeline_name":    pipeline_name,
        "source_table":     source_table,
        "total_rules":      len(summary_rows),
        "error_failures":   len(error_failures),
        "warning_failures": len(warning_failures),
        "has_errors":       has_errors,
        "has_warnings":     has_warnings,
        "passed":           not has_errors,
    }

    _print_final_verdict(result)

    return result

def _print_final_verdict(result: dict) -> None:
    print(f"\n{'='*60}")
    print(f"  FINAL VERDICT")
    print(f"{'='*60}")

    if result["passed"]:
        print(f"  ✓ PASSED — no error-severity failures detected.")
    else:
        print(f"  ✗ FAILED — {result['error_failures']} rule(s) with error-severity failures.")

    if result["has_warnings"]:
        print(f"  ⚠  {result['warning_failures']} rule(s) with warnings — check the report.")

    print(f"\n  run_id : {result['run_id']}")
    print(f"{'='*60}\n")