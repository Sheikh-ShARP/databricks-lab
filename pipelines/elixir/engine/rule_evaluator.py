# engine/rule_evaluator.py
# -----------------------------------------------------------
# Core rule evaluation engine.
# Reads the rules config and evaluates each rule against the
# Silver DataFrame, producing per-record and per-rule results.
# -----------------------------------------------------------

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
import uuid
from typing import Any

# Evaluates rules on a DataFrame and returns detailed and summary results
def evaluate_rules(
    df: DataFrame,
    rules: list[dict],
    source_table: str,
    pipeline_name: str,
    spark: SparkSession,
) -> tuple[DataFrame, DataFrame]:
    """
    Evaluates a list of rules against a Spark DataFrame.
    Returns a tuple of (per-record results DataFrame, per-rule summary DataFrame).
    """
    run_id        = str(uuid.uuid4())
    run_timestamp = datetime.utcnow().isoformat()

    # Add a unique row key to track records across rules
    df = df.withColumn("_row_key", F.monotonically_increasing_id())

    result_frames = []

    for rule in rules:
        rule_id     = rule["rule_id"]
        column      = rule["column"]
        rule_type   = rule["rule_type"]
        severity    = rule["severity"]
        description = rule["description"]
        params      = rule.get("params", {})

        # For 'unique' rules, count occurrences of the column value within its partition
        if rule_type == "unique":
            window     = Window.partitionBy(column)
            working_df = df.withColumn("_unique_count", F.count(column).over(window))
            pass_condition = F.col("_unique_count") == 1
        else:
            working_df     = df
            # Build the pass condition for other rule types
            pass_condition = _build_condition(rule_type, column, params)

        # Evaluate the rule and collect results for each record
        rule_result = (
            working_df
            .withColumn("passed", pass_condition)
            .select(
                "_row_key",
                F.col(column).cast("string").alias("column_value"),
                "passed",
                F.lit(rule_id)      .alias("rule_id"),
                F.lit(column)       .alias("column"),
                F.lit(rule_type)    .alias("rule_type"),
                F.lit(severity)     .alias("severity"),
                F.lit(description)  .alias("description"),
                F.lit(source_table) .alias("source_table"),
                F.lit(pipeline_name).alias("pipeline_name"),
                F.lit(run_id)       .alias("run_id"),
                F.lit(run_timestamp).alias("run_timestamp"),
            )
        )

        result_frames.append(rule_result)

    # Combine all per-rule result DataFrames into a single DataFrame
    results_df = result_frames[0]
    for frame in result_frames[1:]:
        results_df = results_df.union(frame)

    # Aggregate per-rule summary statistics
    summary_df = (
        results_df
        .groupBy(
            "run_id", "run_timestamp", "pipeline_name", "source_table",
            "rule_id", "column", "rule_type", "severity", "description"
        )
        .agg(
            F.count("*")                          .alias("total_records"),
            F.sum(F.col("passed").cast("int"))    .alias("passed_records"),
            F.sum((~F.col("passed")).cast("int")) .alias("failed_records"),
        )
        .withColumn(
            "pass_rate",
            F.round(F.col("passed_records") / F.col("total_records") * 100, 2)
        )
    )

    return results_df, summary_df

# Builds the pass condition Column for a given rule type and parameters
def _build_condition(
    rule_type: str,
    column: str,
    params: dict[str, Any],
) -> "Column":
    """
    Constructs a Spark SQL Column expression representing the pass condition
    for a given rule type, column, and parameters.
    """
    if rule_type == "not_null":
        # Passes if the column value is not null
        return F.col(column).isNotNull()

    elif rule_type == "accepted_values":
        # Passes if the column value is in the accepted set (case-sensitive or not)
        values         = [v.lower() for v in params["values"]]
        case_sensitive = params.get("case_sensitive", True)
        if case_sensitive:
            return F.col(column).isin(params["values"])
        else:
            return F.lower(F.col(column)).isin(values)

    elif rule_type == "regex":
        # Passes if the column value matches the regex pattern
        pattern        = params["pattern"]
        case_sensitive = params.get("case_sensitive", True)
        if case_sensitive:
            return F.col(column).rlike(pattern)
        else:
            return F.lower(F.col(column)).rlike(pattern.lower())

    elif rule_type == "range":
        # Passes if the column value is within the specified min/max range
        col     = F.col(column)
        min_val = params.get("min")
        max_val = params.get("max")
        cond    = F.lit(True)
        if min_val is not None:
            cond = cond & (col >= min_val)
        if max_val is not None:
            cond = cond & (col <= max_val)
        return cond

    elif rule_type == "custom_sql":
        # Passes if the custom SQL expression evaluates to true
        return F.expr(params["expression"])

    else:
        # Raise error for unsupported rule types
        raise ValueError(
            f"Unknown rule_type '{rule_type}'. "
            f"Supported: not_null, unique, accepted_values, regex, range, custom_sql"
        )