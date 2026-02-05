"""Data Profiling Report to DataFrame"""
from pyspark.sql.functions import lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

def profiling_report_to_df(spark, dataset_name: str, report: dict):
    rows = []

    # ---- Null Rates ----
    if "null_rates" in report and report["null_rates"] is not None:
        nr = report["null_rates"].first().asDict()
        for col_name, value in nr.items():
            rows.append((dataset_name, "null_rate", col_name, float(value)))

    # ---- Duplicate Info ----
    if "duplicates" in report and report["duplicates"] is not None:
        dup = report["duplicates"]
        key_label = ",".join(dup.get("keys", [])) or "N/A"

        rows.append((dataset_name, "duplicate_key_groups", key_label, float(dup["duplicate_key_groups"])))
        rows.append((dataset_name, "duplicate_rows", key_label, float(dup["duplicate_rows"])))
        rows.append((dataset_name, "duplicate_row_pct", key_label, float(dup["duplicate_row_pct"])))

    # ---- Cardinality ----
    if "cardinality" in report and report["cardinality"] is not None:
        card = report["cardinality"].first().asDict()
        for col_name, value in card.items():
            rows.append((dataset_name, "cardinality", col_name, float(value)))

    schema = StructType([
        StructField("dataset", StringType(), False),
        StructField("metric_type", StringType(), False),
        StructField("column", StringType(), False),
        StructField("metric_value", DoubleType(), False),
    ])

    return spark.createDataFrame(rows, schema) \
                .withColumn("run_ts", current_timestamp())