from pyspark.sql.functions import col, row_number, lit,  desc, col, lower, upper, trim, to_utc_timestamp
from functools import reduce
from pyspark.sql.window import Window

def enforce_schema(df, schema_map: dict):
    """
    Enforce schema on a dataframe by casting columns to the specified data types.

    Schema_map = {
        "column_name": "data_type",
        ...}
    """
    for c , dtype in schema_map.items():
        if c in df.columns:
            df = df.withColumn(c, col(c).cast(dtype))
    return df

def drop_nulls(df, required_columns: list):
    """
    Drop rows with null values in the specified columns.

    Required_columns = ["column_name", ...]
    """
    conditions = [col(c).isNotNull() for c in required_columns]
    return df.where(reduce(lambda x, y: x & y, conditions))

def deduplicate_latest(df, key_cols: list, timestamp_col: str):
    """
    Deduplicate rows based on the latest timestamp for each key.

    Key_cols = ["column_name", ...]
    """
    window_spec = Window.partitionBy(*key_cols).orderBy(desc(timestamp_col))
    return df.withColumn("row_num", row_number().over(window_spec)).filter(col("row_num") == 1).drop("row_num")

def standardize_strings(df, upper_cols= None, lower_cols= None, trim_cols= None):
    """
    Standardize string columns by converting to upper or lower case and trimming whitespace.
    """
    lower_cols = lower_cols  or []
    trim_cols = trim_cols or []
    upper_cols = upper_cols or []

    for c in lower_cols:
        if c in df.columns:
            df = df.withColumn(c, lower(col(c)))
    for c in upper_cols:
        if c in df.columns:
            df = df.withColumn(c, upper(col(c)))
    for c in trim_cols:
        if c in df.columns:
            df = df.withColumn(c, trim(col(c)))
    return df

def filter_numeric_ranges(df, range_rules: dict):
    """
    Filter rows based on numeric ranges.

    Range_rules = {
        "column_name": (min_value, max_value),
        ...}
    """
    for c, (rule_min, rule_max) in range_rules.items():
        if c in df.columns:
            df = df.filter((col(c) >= rule_min) & (col(c) <= rule_max))
    return df

def normalize_timestamps(df, ts_cols: list, source_tz="UTC"):
    for c in ts_cols:
        if c in df.columns:
            df = df.withCoumn(c, to_utc_timestamp(col(c), source_tz))
    return df

def normalize_booleans(df, bool_map: dict):
    """"
    Standardize boolean-like columns into True/False.
    """
    for col_name, mapping in bool_map.items():
        if col_name in df.columns:
            expr = None
            for k, v in  mapping.items():
                cond = When(col(col_name) == k, lit(v))
                expr = cond if expr is None else expr.otherwise(cond)
            df = df.withColumn(col_name, expr.otherwise(lit(None)))
    return df

def split_ingestion_metadata(df, metadata_cols: str):
    """
    Split ingestion metadata into separate columns.
    """
    existing = [c for c in metadata_cols if c in df.columns]

    metadata_df = df.select(existing) if existing else None
    clean_df = df.drop(*existing)
    
    return clean_df, metadata_df

    