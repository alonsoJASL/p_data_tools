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

# Cleaned names used as column suffixes in the merged output
SHEET_CLEAN_NAMES = {
    'ColonoscopyProcedure': 'ColonoscopyProcedure',
    'StudyExit': 'StudyExit',
    'Annotation tracking': 'AnnotationTracking',
}


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


def _clean_sheet_name(sheet_name: str) -> str:
    """Convert a sheet name to a clean, space-free suffix."""
    return SHEET_CLEAN_NAMES.get(
        sheet_name,
        ''.join(word.capitalize() for word in sheet_name.split())
    )


def _categorise_missing_subjects(
    missing_subjects_df: pd.DataFrame,
    cp_name: str,
    se_name: str,
    at_name: str,
    subject_col: str,
) -> Dict[str, List]:
    """
    Split missing-subject rows into categories based on which sheets they
    are absent from.

    Returns a dict with keys:
        both_cp_and_se, only_cp, only_se, only_at, other
    Each value is a list of subject IDs.
    """
    categories: Dict[str, List] = {
        'both_cp_and_se': [],
        'only_cp': [],
        'only_se': [],
        'only_at': [],
        'other': [],
    }

    if missing_subjects_df.empty:
        return categories

    for _, row in missing_subjects_df.iterrows():
        missing_in = row['missing_in']
        has_cp = cp_name in missing_in
        has_se = se_name in missing_in
        has_at = at_name in missing_in

        subject = row[subject_col]

        if has_cp and has_se:
            categories['both_cp_and_se'].append(subject)
        elif has_cp and not has_se:
            categories['only_cp'].append(subject)
        elif has_se and not has_cp:
            categories['only_se'].append(subject)
        elif has_at:
            categories['only_at'].append(subject)
        else:
            categories['other'].append(subject)

    return categories


def main(args) -> None:
    """
    Main workflow:
    1. Load configuration
    2. Load and slice sheets from each file
    3. Detect duplicate columns
    4. Merge on synthetic key (preserving per-source subjectId columns)
    5. Generate warnings
    6. Save result + summary
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
    df_names = []  # Cleaned sheet names used as merge suffixes

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
        df_names.append(_clean_sheet_name(sheet_name))

    # Resolved sheet names (for categorisation later)
    cp_name, se_name, at_name = df_names[0], df_names[1], df_names[2]

    # --- subjects_available stats (computed before merge) ---
    cp_df = dataframes[0]
    at_df = dataframes[2]

    cp_total = len(cp_df)
    linked_col = 'linked'
    if linked_col in at_df.columns:
        linked_count = (at_df[linked_col].astype(str).str.upper() == 'YES').sum()
    else:
        logger.warning(f"Column '{linked_col}' not found in AnnotationTracking — defaulting linked_count to 0")
        linked_count = 0
    needing_annotation = cp_total - linked_count

    # Detect duplicate columns (before adding synthetic key)
    logger.info("=== Detecting duplicate columns ===")
    duplicate_cols = detect_duplicate_columns(dataframes, df_names)

    if duplicate_cols:
        logger.info(f"Found {len(duplicate_cols)} duplicate columns")
        for col, sources in duplicate_cols.items():
            logger.info(f"  '{col}' in: {', '.join(sources)}")

    # Check for missing subjects (before adding synthetic key)
    logger.info("=== Checking for missing subjects ===")
    missing_subjects_df = detect_missing_subjects(dataframes, df_names, args.subject_col)

    if not missing_subjects_df.empty:
        logger.warning(f"{len(missing_subjects_df)} subjects missing from some sheets")

    # Add synthetic merge key to each df so that subjectId is preserved
    # as a separate column per source in the merged output:
    #   subjectId (ColonoscopyProcedure), subjectId_StudyExit, subjectId_AnnotationTracking
    for df in dataframes:
        df['_merge_key'] = df[args.subject_col]

    # Merge all dataframes on the synthetic key
    logger.info("=== Merging dataframes ===")
    merged_df = merge_dataframes_on_key(
        dataframes,
        df_names,
        key_col='_merge_key',
        how='outer'
    )

    logger.info(f"Merged dataframe shape: {merged_df.shape}")

    # Generate mismatch warnings
    logger.info("=== Generating warnings ===")
    merged_df = generate_mismatch_warnings(merged_df, duplicate_cols)

    # Populate subject_missing_in_sheets — this script owns this column
    merged_df['subject_missing_in_sheets'] = ''
    if not missing_subjects_df.empty:
        missing_map = dict(zip(
            missing_subjects_df[args.subject_col],
            missing_subjects_df['missing_in']
        ))
        merged_df['subject_missing_in_sheets'] = merged_df['_merge_key'].map(
            lambda x: missing_map.get(x, '')
        )

    # Drop the synthetic key — no longer needed
    merged_df = merged_df.drop(columns=['_merge_key'])

    # Save result
    logger.info("=== Saving result ===")
    save_excel_sheet(merged_df, args.output, sheet_name='Merged')

    logger.info("=== Complete ===")

    # --- Build summary ---
    missing_categories = _categorise_missing_subjects(
        missing_subjects_df, cp_name, se_name, at_name, args.subject_col
    )

    warning_counts = {
        'subjects_with_missing_sheets': (merged_df['subject_missing_in_sheets'] != '').sum(),
        'subjects_with_column_mismatches': (merged_df['column_value_mismatch'] != '').sum(),
        'duplicate_column_count': len(duplicate_cols),
    }

    summary_lines = [
        "=== Summary ===",
        f"Total rows in merged output: {len(merged_df)}",
        "",
        "--- Subjects available ---",
        f"  Total ColonoscopyProcedures: {cp_total}",
        f"  AnnotationTracking linked (linked = YES): {linked_count}",
        f"  Procedures needing annotation: {needing_annotation}",
        "",
        "--- Subjects missing from sheets ---",
        f"  Total subjects with any gap: {warning_counts['subjects_with_missing_sheets']}",
        f"  Missing from ColonoscopyProcedure AND StudyExit: {len(missing_categories['both_cp_and_se'])}",
        f"  Missing only from ColonoscopyProcedure: {len(missing_categories['only_cp'])}",
        f"  Missing only from StudyExit: {len(missing_categories['only_se'])}",
        f"  Missing only from AnnotationTracking: {len(missing_categories['only_at'])}",
    ]

    if missing_categories['both_cp_and_se']:
        summary_lines.append(f"    Subjects: {', '.join(str(s) for s in missing_categories['both_cp_and_se'])}")
    if missing_categories['only_cp']:
        summary_lines.append(f"    Only missing CP: {', '.join(str(s) for s in missing_categories['only_cp'])}")
    if missing_categories['only_se']:
        summary_lines.append(f"    Only missing SE: {', '.join(str(s) for s in missing_categories['only_se'])}")
    if missing_categories['only_at']:
        summary_lines.append(f"    Only missing AT: {', '.join(str(s) for s in missing_categories['only_at'])}")

    summary_lines += [
        "",
        "--- Column warnings ---",
        f"  Subjects with column value mismatches: {warning_counts['subjects_with_column_mismatches']}",
        f"  Duplicate columns found: {warning_counts['duplicate_column_count']}",
    ]

    summary_text = "\n".join(summary_lines)

    for line in summary_lines:
        logger.info(line)

    summary_path = args.output.with_suffix('.summary.txt')
    with open(summary_path, 'w') as f:
        f.write(summary_text + "\n")
    logger.info(f"Summary saved to {summary_path}")


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
