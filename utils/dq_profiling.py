"""Data Quality Profiling Utilities"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum, when, approx_count_distinct, min, max, avg, stddev
from typing import Dict, List, Optional


def profile_null_rates(df, cols: list):
    total_rows = df.count()
    exprs = [
        (sum(when(col(c).isNull(), 1).otherwise(0)) / total_rows).alias(f"{c}_null_pct")
        for c in cols if c in df.columns
    ]
    return df.agg(*exprs)

def profile_duplicate_ratio(df, key_cols: list):
    total_rows = df.count()

    dup_df = df.groupBy(*key_cols).count().filter("count > 1")

    duplicate_groups = dup_df.count()
    duplicate_rows = dup_df.selectExpr("sum(count) - count(*) as dup_rows").collect()[0]["dup_rows"]
    duplicate_row_pct = duplicate_rows / total_rows if total_rows else 0

    return {
    "keys": key_cols,
    "duplicate_key_groups": duplicate_groups,
    "duplicate_rows": duplicate_rows,
    "duplicate_row_pct": round(duplicate_row_pct, 4),
    "total_rows": total_rows
}


def profile_cardinality(df, cols: list):
    total_rows = df.count()
    exprs = []

    for c in cols:
        if c in df.columns:
            exprs.append(
                approx_count_distinct(c).alias(f"{c}_distinct_count")
            )

    card_df = df.agg(*exprs).first().asDict()

    metrics = {}
    for k, v in card_df.items():
        metrics[k] = v
        metrics[k.replace("_distinct_count", "_distinct_pct")] = round(v / total_rows, 4) if total_rows else 0

    return metrics


def profile_numeric_distribution(df, numeric_cols: list):
    exprs = []

    for c in numeric_cols:
        if c in df.columns:
            exprs.extend([
                min(c).alias(f"{c}_min"),
                max(c).alias(f"{c}_max"),
                avg(c).alias(f"{c}_avg"),
                stddev(c).alias(f"{c}_stddev")
            ])

    return df.agg(*exprs)


def profile_timestamp_range(df, ts_cols: list):
    exprs = []

    for c in ts_cols:
        if c in df.columns:
            exprs.append(min(c).alias(f"{c}_min"))
            exprs.append(max(c).alias(f"{c}_max"))

    ts_df = df.agg(*exprs).first().asDict()

    return ts_df

def run_all_profiling_checks(df: DataFrame, config: Dict) -> Dict[str, object]:
    """
    Execute configured profiling checks and return a dictionary of metrics.
    """

    report = {}

    if "null_rate_cols" in config:
        report["null_rates"] = profile_null_rates(df, config["null_rate_cols"])

    if "duplicate_keys" in config:
        report["duplicates"] = profile_duplicate_ratio(df, config["duplicate_keys"])

    if "cardinality_cols" in config:
        report["cardinality"] = profile_cardinality(df, config["cardinality_cols"])

    if "numeric_cols" in config:
        report["numeric_distribution"] = profile_numeric_distribution(df, config["numeric_cols"])

    if "timestamp_cols" in config:
        report["timestamp_range"] = profile_timestamp_range(df, config["timestamp_cols"])

    return report