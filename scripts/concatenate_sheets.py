#!/usr/bin/env python3
"""
Concatenate sheets from multiple Excel files with duplicate column handling.

TASK 1: Merge ColonoscopyProcedure + StudyExit (partial) + AnnotationTracking (partial)
by subject ID, handling duplicate columns and generating warnings.
"""
import argparse
import json
import logging
from pathlib import Path
from typing import List, Dict, Any

import pandas as pd

from polyp_data_tools import (
    setup_logging,
    load_excel_sheet,
    save_excel_sheet,
)
from polyp_data_tools.excel_ops import slice_dataframe_by_range
from polyp_data_tools.merge_ops import (
    detect_duplicate_columns,
    merge_dataframes_on_key,
    generate_mismatch_warnings,
    detect_missing_subjects,
)

setup_logging()
logger = logging.getLogger(__name__)


# Default configuration for the merge
DEFAULT_CONFIG = [
    {
        'file': 'file_a',
        'sheet': 'ColonoscopyProcedure',
        'columns': None,  # All columns
        'header_row': 2,
    },
    {
        'file': 'file_a',
        'sheet': 'StudyExit',
        'columns': 'C:',  # From column G to end
        'header_row': 2,
    },
    {
        'file': 'file_b',
        'sheet': 'Annotation tracking',
        'columns': 'A:O',  # Columns B through O
        'header_row': 0,
    },
]


def load_config(config_path: Path) -> List[Dict[str, Any]]:
    """Load configuration from JSON file."""
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        logger.info(f"Loaded config from {config_path}")
        return config
    except Exception as e:
        logger.error(f"Failed to load config from {config_path}: {e}")
        raise


def load_and_slice_sheet(
    filepath: Path,
    sheet_name: str,
    columns: str,
    header_row: int
) -> pd.DataFrame:
    """
    Load a sheet and apply column slicing if specified.
    
    Args:
        filepath: Path to Excel file
        sheet_name: Name of sheet
        columns: Excel-style column range (e.g., "G:", "B:O") or None for all
        header_row: Row index for headers (0-indexed)
    
    Returns:
        Loaded and sliced DataFrame
    """
    df = load_excel_sheet(filepath, sheet_name, header_row)
    
    if columns is not None:
        logger.info(f"Slicing columns: {columns}")
        df = slice_dataframe_by_range(df, columns)
        logger.info(f"After slicing: {df.shape}")
    
    return df


def main(args) -> None:
    """
    Main workflow:
    1. Load configuration
    2. Load and slice sheets from each file
    3. Detect duplicate columns
    4. Merge on subject ID
    5. Generate warnings
    6. Save result
    """
    # Load config
    if args.config:
        config = load_config(args.config)
    else:
        config = DEFAULT_CONFIG
        logger.info("Using default configuration")
    
    # Map file identifiers to actual file paths
    file_map = {
        'file_a': args.file_a,
        'file_b': args.file_b,
    }
    
    # Load all sheets according to config
    logger.info("=== Loading sheets ===")
    dataframes = []
    df_names = []

    # save config for reference
    config_output_path = args.output.with_suffix('.config.json')
    with open(config_output_path, 'w') as f:
        json.dump(config, f, indent=4)
    
    for item in config:
        file_id = item['file']
        sheet_name = item['sheet']
        columns = item.get('columns', None)
        file_header_row = item.get('header_row', 0)

        filepath = file_map[file_id]
        
        logger.info(f"Loading {file_id}:{sheet_name} from {filepath.name}")
        df = load_and_slice_sheet(filepath, sheet_name, columns, file_header_row)
        
        dataframes.append(df)
        df_names.append(f"{file_id}:{sheet_name}")
    
    # Detect duplicate columns
    logger.info("=== Detecting duplicate columns ===")
    duplicate_cols = detect_duplicate_columns(dataframes, df_names)
    
    if duplicate_cols:
        logger.info(f"Found {len(duplicate_cols)} duplicate columns")
        for col, sources in duplicate_cols.items():
            logger.info(f"  '{col}' in: {', '.join(sources)}")
    
    # Check for missing subjects
    logger.info("=== Checking for missing subjects ===")
    missing_subjects_df = detect_missing_subjects(dataframes, df_names, args.subject_col)
    
    if not missing_subjects_df.empty:
        logger.warning(f"{len(missing_subjects_df)} subjects missing from some sheets")
    
    # Merge all dataframes
    logger.info("=== Merging dataframes ===")
    merged_df = merge_dataframes_on_key(
        dataframes,
        df_names,
        key_col=args.subject_col,
        how='outer'
    )
    
    logger.info(f"Merged dataframe shape: {merged_df.shape}")
    
    # Generate warnings
    logger.info("=== Generating warnings ===")
    merged_df = generate_mismatch_warnings(merged_df, duplicate_cols, args.subject_col)
    
    # Add missing subject warnings
    if not missing_subjects_df.empty:
        missing_map = dict(zip(
            missing_subjects_df[args.subject_col],
            missing_subjects_df['missing_in']
        ))
        merged_df['subject_missing_in_sheets'] = merged_df[args.subject_col].map(
            lambda x: missing_map.get(x, '')
        )
    
    # Save result
    logger.info("=== Saving result ===")
    save_excel_sheet(merged_df, args.output, sheet_name='Merged')
    
    logger.info("=== Complete ===")
    
    # Summary
    warning_counts = {
        'subjects_with_missing_sheets': (merged_df['subject_missing_in_sheets'] != '').sum(),
        'subjects_with_column_mismatches': (merged_df['column_value_mismatch'] != '').sum(),
        'duplicate_column_count': len(duplicate_cols),
    }
    
    logger.info("Summary:")
    logger.info(f"  Total rows: {len(merged_df)}")
    logger.info(f"  Subjects missing from some sheets: {warning_counts['subjects_with_missing_sheets']}")
    logger.info(f"  Subjects with column value mismatches: {warning_counts['subjects_with_column_mismatches']}")
    logger.info(f"  Duplicate columns found: {warning_counts['duplicate_column_count']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Concatenate sheets from multiple Excel files with duplicate handling.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--file-a",
        type=Path,
        required=True,
        help="Path to first Excel file (contains ColonoscopyProcedure, StudyExit)"
    )
    parser.add_argument(
        "--file-b",
        type=Path,
        required=True,
        help="Path to second Excel file (contains AnnotationTracking)"
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Path to output Excel file"
    )
    parser.add_argument(
        "--subject-col",
        type=str,
        default="subjectId",
        help="Name of subject ID column for matching"
    )
    parser.add_argument(
        "--config",
        type=Path,
        help="Optional JSON config file to override default sheet configuration"
    )
    
    args = parser.parse_args()
    main(args)