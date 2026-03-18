#!/usr/bin/env python3
"""
Merge transformed polyp data sheets with dropout information and warnings.

TASK 2b: Merge Diagnosis + Sizing + Histology (all in long format)
plus StudyExit dropout data, with warnings for mismatches.
"""
import argparse
import logging
from pathlib import Path

import pandas as pd

from polyp_data_tools import (
    setup_logging,
    load_file,
    load_excel_sheet,
    save_excel_sheet,
)
from polyp_data_tools.merge_ops import (
    detect_duplicate_columns,
    merge_on_composite_key,
    generate_mismatch_warnings,
    add_dropout_info,
)

setup_logging()
logger = logging.getLogger(__name__)


def main(args) -> None:
    """
    Main workflow:
    1. Load three transformed sheets (diagnosis, sizing, histology)
    2. Load StudyExit sheet for dropout info
    3. Merge on composite key (subject + polyp)
    4. Add randomOrganisation column
    5. Add dropout info
    6. Generate warnings
    7. Save result
    """
    # Load transformed sheets
    logger.info("=== Loading transformed sheets ===")
    diagnosis_df = load_file(args.diagnosis)
    sizing_df = load_file(args.sizing)
    histology_df = load_file(args.histology)
    
    logger.info(f"Diagnosis: {diagnosis_df.shape}")
    logger.info(f"Sizing: {sizing_df.shape}")
    logger.info(f"Histology: {histology_df.shape}")
    
    # Load StudyExit for dropout info
    logger.info("=== Loading StudyExit sheet ===")
    study_exit_df = load_excel_sheet(
        args.study_exit_file,
        args.study_exit_sheet,
        header_row=args.header_row
    )
    logger.info(f"StudyExit: {study_exit_df.shape}")
    
    # Detect duplicate columns before merge
    logger.info("=== Detecting duplicate columns ===")
    dataframes = [diagnosis_df, sizing_df, histology_df]
    df_names = ['Diagnosis', 'Sizing', 'Histology']
    
    duplicate_cols = detect_duplicate_columns(dataframes, df_names)
    
    if duplicate_cols:
        logger.info(f"Found {len(duplicate_cols)} duplicate columns")
        for col, sources in duplicate_cols.items():
            logger.info(f"  '{col}' in: {', '.join(sources)}")
    
    # Merge on composite key
    logger.info("=== Merging sheets on composite key ===")
    merged_df = merge_on_composite_key(
        dataframes,
        df_names,
        subject_col=args.subject_col,
        polyp_col=args.polyp_col,
        how='outer'
    )
    
    logger.info(f"Merged shape: {merged_df.shape}")
    
    # Add randomOrganisation column if it exists in any sheet
    logger.info("=== Adding randomOrganisation column ===")
    
    # Check which dataframe has randomOrganisation (or similar)
    org_col_candidates = [args.random_org_col]
    
    for df, name in zip(dataframes, df_names):
        for col in df.columns:
            if 'random' in col.lower() and 'org' in col.lower():
                logger.info(f"Found organisation column '{col}' in {name}")
                if col not in org_col_candidates:
                    org_col_candidates.append(col)
    
    # Use the first available column
    org_col_found = None
    for col in org_col_candidates:
        if col in merged_df.columns:
            org_col_found = col
            break
    
    if org_col_found:
        if org_col_found != 'randomOrganisation':
            merged_df = merged_df.rename(columns={org_col_found: 'randomOrganisation'})
        logger.info("Added randomOrganisation column")
    else:
        logger.warning("randomOrganisation column not found - adding empty column")
        merged_df['randomOrganisation'] = ''
    
    # Add dropout info from StudyExit
    logger.info("=== Adding dropout information ===")
    logger.info(f"StudyExit columns: {study_exit_df.columns.tolist()}")
    logger.info(f"Looking for dropout indicator at column: {args.dropout_indicator_col}")
    
    merged_df = add_dropout_info(
        merged_df,
        study_exit_df,
        subject_col=args.subject_col,
        dropout_indicator_col=args.dropout_indicator_col,
        dropout_info_cols=None  # Will auto-detect columns 15-20 (P:U)
    )
    
    # Generate warnings for mismatches
    logger.info("=== Generating warnings ===")
    merged_df = generate_mismatch_warnings(merged_df, duplicate_cols, args.subject_col)
    
    # Additional warning: Check if subject+polyp combinations exist in all sheets
    logger.info("=== Checking for missing polyps across sheets ===")
    
    # Create composite key for checking
    for df, name in zip(dataframes, df_names):
        df['_check_key'] = df[args.subject_col].astype(str) + '::' + df[args.polyp_col].astype(str)
    
    keys_diagnosis = set(diagnosis_df['_check_key'])
    keys_sizing = set(sizing_df['_check_key'])
    keys_histology = set(histology_df['_check_key'])
    
    # Find polyps missing from each sheet
    merged_df['_check_key'] = merged_df[args.subject_col].astype(str) + '::' + merged_df[args.polyp_col].astype(str)
    
    def check_missing_polyps(row_key):
        missing = []
        if row_key not in keys_diagnosis:
            missing.append('Diagnosis')
        if row_key not in keys_sizing:
            missing.append('Sizing')
        if row_key not in keys_histology:
            missing.append('Histology')
        return ', '.join(missing) if missing else ''
    
    merged_df['polyp_missing_in_sheets'] = merged_df['_check_key'].apply(check_missing_polyps)
    merged_df = merged_df.drop(columns=['_check_key'])
    
    missing_count = (merged_df['polyp_missing_in_sheets'] != '').sum()
    if missing_count > 0:
        logger.warning(f"{missing_count} polyps missing from some sheets")
    
    # Save result
    logger.info("=== Saving result ===")
    save_excel_sheet(merged_df, args.output, sheet_name='MergedPolyps')
    
    logger.info("=== Complete ===")
    
    # Summary
    logger.info("Summary:")
    logger.info(f"  Total polyps: {len(merged_df)}")
    logger.info(f"  Unique subjects: {merged_df[args.subject_col].nunique()}")
    logger.info(f"  Polyps missing from some sheets: {missing_count}")
    
    dropout_count = (merged_df['dropout_P'] != '').sum() if 'dropout_P' in merged_df.columns else 0
    logger.info(f"  Polyps from dropout subjects: {dropout_count}")
    
    mismatch_count = (merged_df['column_value_mismatch'] != '').sum()
    logger.info(f"  Polyps with column value mismatches: {mismatch_count}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Merge transformed polyp data sheets with dropout info and warnings.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--diagnosis",
        type=Path,
        required=True,
        help="Path to transformed Diagnosis file"
    )
    parser.add_argument(
        "--sizing",
        type=Path,
        required=True,
        help="Path to transformed Sizing file"
    )
    parser.add_argument(
        "--histology",
        type=Path,
        required=True,
        help="Path to transformed Histology file"
    )
    parser.add_argument(
        "--study-exit-file",
        type=Path,
        required=True,
        help="Path to Excel file containing StudyExit sheet"
    )
    parser.add_argument(
        "--study-exit-sheet",
        type=str,
        default="StudyExit",
        help="Name of StudyExit sheet"
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
        help="Name of subject ID column"
    )
    parser.add_argument(
        "--polyp-col",
        type=str,
        default="polypId",
        help="Name of polyp ID column"
    )
    parser.add_argument(
        "--random-org-col",
        type=str,
        default="randomOrganisation",
        help="Name of randomisation/organisation column"
    )
    parser.add_argument(
        "--header-row",
        type=int,
        default=0,
        help="Row index to use as column headers for StudyExit (0-indexed)"
    )
    parser.add_argument(
        "--dropout-indicator-col",
        type=str,
        default="O",
        help="Column name or Excel position (e.g., 'O') for dropout indicator in StudyExit"
    )
    
    args = parser.parse_args()
    main(args)