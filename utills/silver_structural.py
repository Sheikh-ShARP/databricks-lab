"""Silver cleaning functions"""
from pyspark.sql.functions import col, sum, when, count, approx_count_distinct, min, max

def profile_null_rates(df):
    """ Calculate percentage of null values in each column """
    total_rows = df.count()
    exprs = [
        (sum(when(col(c).isNull(), 1).otherwise(0)) / total_rows).alias(f "{c}_null_pct")
        for c in df.columns
    ]
    return df.agg(*exprs)

def profile_duplicate_ratio(df, key_cols: list):
    """"Measures how many duplicate records exist for given keys"""
    total_rows = df.count()

    dup_count = (
        df.groupBy(*key_cols)
        .count()
        .filter("count > 1")
        .count()
    )   
    return {"duplicatye_key_group" : dup_count, "total_rows" : total_rows}

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


