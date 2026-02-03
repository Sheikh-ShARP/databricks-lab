"""Silver cleaning functions"""
from pyspark.sql.functions import col, sum, when, count, approx_count_distinct, min, max

def profile_null_rates(df):
    """ Calculate percentage of null values in each column """
    total_rows = df.count()
    exprs = [
        (sum(when(col(c).isNull(), 1).otherwise(0)) / total_rows).alias(f"{c}_null_pct")
        for c in df.columns
    ]
    return df.agg(*exprs)

def profile_duplicate_ratio(df, key_cols: list):
    """Measures duplicate key groups and duplicate row count"""
    total_rows = df.count()

    dup_df = df.groupBy(*key_cols).count().filter("count > 1")

    duplicate_groups = dup_df.count()
    duplicate_rows = dup_df.selectExpr("sum(count) - count(*) as dup_rows").collect()[0]["dup_rows"]

    return {
        "duplicate_key_groups": duplicate_groups,
        "duplicate_rows": duplicate_rows,
        "total_rows": total_rows
    }

def profile_cardinality(df, cols: list):
    """Estimates distinct values per column"""
    return df.agg(*[approx_count_distinct(c).alias(f"{c}_distinct_count") for c in cols if c in df.columns
    ])

def profile_numeric_distribution(df, numeric_cols: list):
    """Summary stats for numeric columns"""

    return df.select(numeric_cols).describe()

def profile_timestamp_range(df, ts_cols: list):
    """Find min and max timestamps"""
    exprs = []
    for c in ts_cols:
        if c in df.columns:
            exprs.append(min(c).alias(f"{c}_min"))
            exprs.append(max(c).alias(f"{c}_max"))
    return df.agg(*exprs)

def run_all_profiling_checks(df, config: dict):
    """
    Runs all profiling checks based on config and returns a dictionary of metrics.
    """
    report = {}

    report["null_rates"] = profile_null_rates(df)

    if "duplicate_keys" in config:
        report["duplicates"] = profile_duplicate_ratio(df, config["duplicate_keys"])

    if "cardinality_cols" in config:
        report["cardinality"] = profile_cardinality(df, config["cardinality_cols"])

    if "numeric_cols" in config:
        report["numeric_distribution"] = profile_numeric_distribution(df, config["numeric_cols"])

    if "timestamp_cols" in config:
        report["timestamp_range"] = profile_timestamp_range(df, config["timestamp_cols"])

    return report


    