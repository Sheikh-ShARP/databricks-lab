from pyspark.sql.functions import col, row_number, lit,  desc, col, lower, upper, trim, to_utc_timestamp, sha2, concat_ws
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
    if not required_columns:
        return df
    return df.dropna(subset=[c for c in required_columns if c in df.columns])


from pyspark.sql.functions import row_number, desc, monotonically_increasing_id

from pyspark.sql.functions import to_timestamp

def deduplicate_latest(df, key_cols: list, timestamp_col: str):
    if not key_cols or timestamp_col not in df.columns:
        return df

    df = df.withColumn(timestamp_col, to_timestamp(col(timestamp_col)))
    df = df.withColumn("_tie_breaker", monotonically_increasing_id())

    window_spec = Window.partitionBy(*key_cols).orderBy(
        col(timestamp_col).desc(), col("_tie_breaker").desc()
    )

    return (
        df.withColumn("_rn", row_number().over(window_spec))
          .filter(col("_rn") == 1)
          .drop("_rn", "_tie_breaker")
    )


from pyspark.sql.functions import coalesce

def dedup_exact_rows(df, exclude_cols=None):
    exclude_cols = exclude_cols or []
    cols = [c for c in df.columns if c not in exclude_cols]

    safe_cols = [coalesce(col(c).cast("string"), lit("NULL")) for c in cols]

    return (
        df.withColumn("_row_hash", sha2(concat_ws("||", *safe_cols), 256))
          .dropDuplicates(["_row_hash"])
          .drop("_row_hash")
    )


def standardize_strings(df, upper_cols=None, lower_cols=None, trim_cols=None):
    upper_cols = upper_cols or []
    lower_cols = lower_cols or []
    trim_cols = trim_cols or []

    for c in trim_cols:
        if c in df.columns:
            df = df.withColumn(c, trim(col(c)))

    for c in lower_cols:
        if c in df.columns:
            df = df.withColumn(c, lower(col(c)))

    for c in upper_cols:
        if c in df.columns:
            df = df.withColumn(c, upper(col(c)))

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
    for c in ts_cols or []:
        if c in df.columns:
            df = df.withColumn(c, to_utc_timestamp(col(c).cast("timestamp"), source_tz))
    return df


from pyspark.sql.functions import when

def normalize_booleans(df, bool_map: dict):
    for col_name, mapping in (bool_map or {}).items():
        if col_name not in df.columns or not mapping:
            continue

        expr = None
        for raw_val, bool_val in mapping.items():
            condition = lower(trim(col(col_name))) == str(raw_val).lower()
            expr = when(condition, lit(bool_val)) if expr is None else expr.when(condition, lit(bool_val))

        df = df.withColumn(col_name, expr.otherwise(lit(None)))

    return df


def split_ingestion_metadata(df, metadata_cols: list):
    metadata_cols = metadata_cols or []
    existing = [c for c in metadata_cols if c in df.columns]

    metadata_df = df.select(*existing) if existing else None
    clean_df = df.drop(*existing) if existing else df

    return clean_df, metadata_df


def apply_structural_cleaning(df, config: dict):
    config = config or {}

    df = enforce_schema(df, config.get("schema", {}))
    df = standardize_strings(
        df,
        upper_cols=config.get("upper_columns", []),
        lower_cols=config.get("lower_columns", []),
        trim_cols=config.get("trim_columns", []),
    )
    df = drop_nulls(df, config.get("required_columns", []))
    df = dedup_exact_rows(df, exclude_cols=config.get("dedup_exclude_columns", []))
    df = deduplicate_latest(df, config.get("key_columns", []), config.get("timestamp_column"))
    df = filter_numeric_ranges(df, config.get("range_rules", {}))
    df = normalize_timestamps(df, config.get("timestamp_columns", []))
    df = normalize_booleans(df, config.get("boolean_map", {}))
    df, metadata_df = split_ingestion_metadata(df, config.get("metadata_columns", []))

    return df, metadata_df




