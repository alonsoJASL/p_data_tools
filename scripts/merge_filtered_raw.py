#!/usr/bin/env python3
"""
Match and merge filtered and raw files (CSV or Excel).
Matches rows by composite key (subject + polyp_id), normalizes ID formats,
then slices and interleaves columns according to spec.
"""
import argparse
import logging
import pandas as pd

from pathlib import Path
from typing import Set

# Imports from our own modules
from polyp_data_tools import (
    load_file, save_file,
    extract_composite_keys, 
    setup_logging,
    revert_to_csv_if_no_excel_support,
)

# Configure logging
setup_logging()
logger = logging.getLogger(__name__)

# ============================================================================
# LOGIC LAYER - Pure stateless transformations
# ============================================================================

def filter_raw_by_filtered_keys(
    raw_df: pd.DataFrame,
    filtered_keys: Set[str],
    raw_key_series: pd.Series
) -> pd.DataFrame:
    """
    Filter raw dataframe to only rows with keys present in filtered.
    Preserves raw_df original order (filtered order applied later).
    Logic layer - stateless.
    """
    mask = raw_key_series.isin(filtered_keys)
    matched_count = mask.sum()
    
    logger.info(f"Raw rows matching filtered keys: {matched_count}/{len(raw_df)}")
    
    return raw_df[mask].copy()


def reorder_by_key(
    df: pd.DataFrame,
    key_series: pd.Series,
    target_key_order: pd.Series
) -> pd.DataFrame:
    """
    Reorder dataframe rows to match target key ordering.
    Logic layer - stateless.
    """
    # Create temporary key column for merge
    df_with_key = df.copy()
    df_with_key['_merge_key'] = key_series
    
    # Create ordering dataframe
    order_df = pd.DataFrame({
        '_merge_key': target_key_order,
        '_order': range(len(target_key_order))
    })
    
    # Merge and sort
    merged = df_with_key.merge(order_df, on='_merge_key', how='inner')
    merged = merged.sort_values('_order')
    
    # Drop helper columns
    merged = merged.drop(columns=['_merge_key', '_order'])
    
    return merged.reset_index(drop=True)


def slice_and_interleave_columns(
    filtered_df: pd.DataFrame,
    raw_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Slice and interleave columns according to spec:
    [filtered: 0-10] | [raw: 20] | [filtered: 11-end] | [raw: 21-24]
    
    Logic layer - stateless.
    """
    # Verify row counts match
    if len(filtered_df) != len(raw_df):
        logger.error(f"Row count mismatch: filtered={len(filtered_df)}, raw={len(raw_df)}")
        raise ValueError("Filtered and raw dataframes must have same row count after matching")
    
    # Extract slices
    filtered_start = filtered_df.iloc[:, 0:11]   # Columns A-K (0-10 inclusive)
    raw_col_u = raw_df.iloc[:, [20]]             # Column U (20)
    filtered_mid_end = filtered_df.iloc[:, 11:]  # Columns L-end
    raw_cols_v_y = raw_df.iloc[:, 21:25]         # Columns V-Y (21-24 inclusive)
    
    # Concatenate horizontally
    result = pd.concat(
        [filtered_start, raw_col_u, filtered_mid_end, raw_cols_v_y],
        axis=1
    )
    
    logger.info(f"Merged dataframe shape: {result.shape}")
    
    return result


# ============================================================================
# VALIDATION LAYER
# ============================================================================

def validate_unmatched_rows(
    filtered_keys: Set[str],
    raw_keys: Set[str]
) -> None:
    """
    Log any keys present in filtered but not in raw (should not happen).
    """
    unmatched = filtered_keys - raw_keys
    
    if unmatched:
        logger.warning(f"UNMATCHED: {len(unmatched)} keys in filtered not found in raw")
        for key in sorted(unmatched):
            logger.warning(f"  Missing key: {key}")
    else:
        logger.info("All filtered keys found in raw")

def main(args) -> None:
    """
    Main workflow:
    1. Load filtered and raw files
    2. Build composite keys for both
    3. Filter raw to only rows in filtered
    4. Reorder raw to match filtered row order
    5. Slice and interleave columns
    6. Save result
    """
    args.output_file = revert_to_csv_if_no_excel_support(args.output_file)

    # Load files
    logger.info("=== Loading files ===")
    filtered_df = load_file(args.filtered_file)
    raw_df = load_file(args.raw_file)
    
    # Extract composite keys
    logger.info("=== Building composite keys ===")
    
    # Filtered: subject_id (col 0), polyp_id (col 1)
    filtered_keys_series = extract_composite_keys(
        filtered_df,
        subject_col='subject_id',
        polyp_col='polyp_id'
    )
    filtered_keys_set = set(filtered_keys_series.dropna().unique())
    logger.info(f"Unique filtered keys: {len(filtered_keys_set)}")
    
    # Raw: subject (col 1), id (col 0) - note reversed order
    raw_keys_series = extract_composite_keys(
        raw_df,
        subject_col='subject',
        polyp_col='id'
    )
    raw_keys_set = set(raw_keys_series.dropna().unique())
    logger.info(f"Unique raw keys: {len(raw_keys_set)}")
    
    # Validate
    logger.info("=== Validating keys ===")
    validate_unmatched_rows(filtered_keys_set, raw_keys_set)
    
    # Filter raw to matched rows
    logger.info("=== Filtering raw dataframe ===")
    raw_filtered = filter_raw_by_filtered_keys(
        raw_df,
        filtered_keys_set,
        raw_keys_series
    )
    
    # Extract keys for filtered raw (for reordering)
    raw_filtered_keys_series = extract_composite_keys(
        raw_filtered,
        subject_col='subject',
        polyp_col='id'
    )
    
    # Reorder raw_filtered to match filtered row order
    logger.info("=== Reordering raw to match filtered ===")
    raw_filtered_ordered = reorder_by_key(
        raw_filtered,
        raw_filtered_keys_series,
        filtered_keys_series
    )
    
    # Slice and interleave
    logger.info("=== Slicing and interleaving columns ===")
    result_df = slice_and_interleave_columns(filtered_df, raw_filtered_ordered)
    
    # Save
    logger.info("=== Saving result ===")
    save_file(result_df, args.output_file)
    
    logger.info("=== Complete ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Match and merge filtered and raw Excel files by composite key.", 
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        usage="python merge_filtered_raw.py "
    )
    parser.add_argument(
        "--filtered-file",
        type=Path,
        required=True,
        # default="earthscan_merged_polyp_data.csv", 
        help="Path to filtered (trustworthy) Excel file"
    )
    parser.add_argument(
        "--raw-file",
        type=Path,
        required=True,
        # default="output.csv", # BAPTISTE FILE
        help="Path to raw (less trustworthy) Excel file"
    )
    parser.add_argument(
        "--output-file",
        type=Path,
        default="MERGED_FILTER_RAW.csv",
        help="Path to output merged Excel file"
    )
    
    args = parser.parse_args()
    main(args)