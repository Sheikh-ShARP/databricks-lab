from utils.silver_structural import apply_structural_cleaning
from utils.dq_profiling import run_all_profiling_checks

def run_silver_pipeline(bronze_df, silver_config: dict, dq_config: dict, run_post_checks=True):
    """
    Bronze → Silver structural pipeline with profiling.
    """

    # Profile Bronze
    bronze_report = run_all_profiling_checks(bronze_df, dq_config)

    # Apply structural cleaning
    silver_df, metadata_df = apply_structural_cleaning(bronze_df, silver_config)

    # Optional post-clean profiling
    silver_report = None
    if run_post_checks:
        silver_report = run_all_profiling_checks(silver_df, dq_config)

    return {
        "silver_df": silver_df,
        "metadata_df": metadata_df,
        "bronze_report": bronze_report,
        "silver_report": silver_report
    }
