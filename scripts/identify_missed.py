#!/usr/bin/env python3
"""
Filter file to only include missed polyps (polyp IDs with letter in third segment).
Missed polyps have format like: 1-001-A, 1-001-B, etc.
"""
import sys
import argparse
import logging
import pandas as pd

from pathlib import Path

from polyp_data_tools import (
    is_missed_polyp,
    load_file,
    save_file,
    revert_to_csv_if_no_excel_support,
    setup_logging,
)

# Configure logging
setup_logging()
logger = logging.getLogger(__name__)


# ============================================================================
# LOGIC LAYER - Filtering
# ============================================================================

def filter_missed_polyps(df: pd.DataFrame, polyp_id_column: str) -> pd.DataFrame:
    """
    Filter dataframe to only rows with missed polyps.
    Logic layer - stateless.
    """
    if polyp_id_column not in df.columns:
        logger.error(f"Column '{polyp_id_column}' not found in dataframe")
        logger.error(f"Available columns: {list(df.columns)}")
        sys.exit(1)
    
    # Apply filter
    mask = df[polyp_id_column].apply(is_missed_polyp)
    filtered_df = df[mask].copy()
    
    logger.info(f"Original rows: {len(df)}")
    logger.info(f"Missed polyps: {len(filtered_df)}")
    logger.info(f"Filtered out: {len(df) - len(filtered_df)}")
    
    return filtered_df


# ============================================================================
# ORCHESTRATION LAYER - Main workflow
# ============================================================================

def main(args) -> None:
    """
    Main workflow:
    1. Load input file
    2. Filter to missed polyps only
    3. Save result
    """
    args.output_file = revert_to_csv_if_no_excel_support(args.output_file)

    logger.info("=== Loading file ===")
    df = load_file(args.input_file)
    
    logger.info(f"=== Filtering missed polyps (column: '{args.polyp_id_column}') ===")
    filtered_df = filter_missed_polyps(df, args.polyp_id_column)
    
    if len(filtered_df) == 0:
        logger.warning("No missed polyps found - output will be empty")
    
    logger.info("=== Saving result ===")
    save_file(filtered_df, args.output_file)
    
    logger.info("=== Complete ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Filter file to only include missed polyps (IDs with letter in third segment).", 
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        usage="python identify_missed.py --input-file BAPTISTE_FILE"
    )
    parser.add_argument(
        "--input-file",
        type=Path,
        required=True,
        help="Path to input file (.csv, .xlsx, or .xls)"
    )
    parser.add_argument(
        "--output-file",
        type=Path,
        default="IDENTIFIED_MISSED_POLYPS.csv",
        help="Path to output file (.csv, .xlsx, or .xls)"
    )
    parser.add_argument(
        "--polyp-id-column",
        type=str,
        default="id",
        help="Name of column containing polyp IDs (default: 'id')"
    )
    
    args = parser.parse_args()
    main(args)