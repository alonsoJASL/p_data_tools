#!/usr/bin/env python3
"""
Transform wide-format polyp data to long format.

TASK 2a: Convert patient-rows with multiple polyps (polypId-1, polypId-2, ...)
to polyp-rows (one row per polyp).
"""
import argparse
import logging
from pathlib import Path

from polyp_data_tools import (
    setup_logging,
    load_excel_sheet,
    save_excel_sheet,
)
from polyp_data_tools.wide_to_long import (
    detect_polyp_column_groups,
    transform_to_long_format,
)

setup_logging()
logger = logging.getLogger(__name__)


def main(args) -> None:
    """
    Main workflow:
    1. Load wide-format sheet
    2. Detect polyp column groups
    3. Transform to long format
    4. Save result
    """
    # Load sheet
    logger.info("=== Loading sheet ===")
    df = load_excel_sheet(
        args.input_file,
        args.sheet_name,
        header_row=args.header_row
    )
    
    logger.info(f"Input shape: {df.shape}")
    
    # Detect polyp column groups
    logger.info("=== Detecting polyp column groups ===")
    column_groups = detect_polyp_column_groups(
        df,
        polyp_id_pattern=args.polyp_id_pattern,
        max_polyps=args.max_polyps
    )
    
    if not column_groups:
        logger.error("No polyp column groups detected")
        logger.error(f"Columns in dataframe: {df.columns.tolist()}")
        logger.error(f"Looking for pattern: {args.polyp_id_pattern}")
        return
    
    logger.info(f"Detected {len(column_groups)} polyp column groups")
    
    # Show sample of detected groups
    for group in column_groups[:3]:  # First 3 groups
        logger.info(
            f"  Polyp {group.polyp_number}: {group.polyp_id_col} + "
            f"{len(group.info_columns)} info columns"
        )
    if len(column_groups) > 3:
        logger.info(f"  ... and {len(column_groups) - 3} more groups")
    
    # Transform to long format
    logger.info("=== Transforming to long format ===")
    
    # Parse preserve columns (comma-separated)
    preserve_cols = []
    if args.preserve_cols:
        preserve_cols = [col.strip() for col in args.preserve_cols.split(',')]
        logger.info(f"Preserving columns: {preserve_cols}")
    
    long_df = transform_to_long_format(
        df,
        subject_col=args.subject_col,
        column_groups=column_groups,
        preserve_cols=preserve_cols,
        invalid_markers=['.b', '.h', '.n']
    )
    
    logger.info(f"Output shape: {long_df.shape}")
    logger.info(f"Columns: {long_df.columns.tolist()}")
    
    # Save result
    logger.info("=== Saving result ===")
    save_excel_sheet(long_df, args.output, sheet_name=args.output_sheet_name)
    
    logger.info("=== Complete ===")
    
    # Summary
    logger.info("Summary:")
    logger.info(f"  Input: {len(df)} subjects")
    logger.info(f"  Output: {len(long_df)} polyps")
    logger.info(f"  Average polyps per subject: {len(long_df) / len(df):.2f}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Transform wide-format polyp data to long format.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--input-file",
        type=Path,
        required=True,
        help="Path to input Excel file"
    )
    parser.add_argument(
        "--sheet-name",
        type=str,
        required=True,
        help="Name of sheet to transform (e.g., 'Diagnosis', 'Sizing', 'Histology')"
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Path to output Excel file"
    )
    parser.add_argument(
        "--output-sheet-name",
        type=str,
        default="Transformed",
        help="Name of output sheet"
    )
    parser.add_argument(
        "--subject-col",
        type=str,
        default="subjectId",
        help="Name of subject ID column"
    )
    parser.add_argument(
        "--polyp-id-pattern",
        type=str,
        default="Q1_R{n}_C1",
        help="Pattern for polyp ID columns (use {n} for number placeholder). "
             "Examples: 'polypId-{n}', 'Q1_R{n}_C1'"
    )
    parser.add_argument(
        "--header-row",
        type=int,
        default=0,
        help="Row index to use as column headers (0-indexed)"
    )
    parser.add_argument(
        "--max-polyps",
        type=int,
        help="Maximum number of polyps to process (default: auto-detect all)"
    )
    parser.add_argument(
        "--preserve-cols",
        type=str,
        default="randomizationGroup",
        help="Comma-separated list of subject-level columns to preserve (default: randomizationGroup)"
    )
    
    args = parser.parse_args()
    main(args)