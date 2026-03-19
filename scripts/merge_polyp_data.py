#!/usr/bin/env python3
"""
Merge transformed polyp data sheets with dropout information and warnings.

TASK 2b: Merge Diagnosis + Sizing + Histology (all in long format)
plus StudyExit dropout data, with warnings for mismatches.

Polyp counts in the summary are based on the composite key subjectId::polypId.
Each unique (subjectId, polypId) pair seen in any sheet is one row in the merged
output. Symmetric differences report pairs present in one sheet but absent in
another. Orphan pairs (missing from at least one sheet) are saved as a separate
file.
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
    build_composite_key_set,
    detect_duplicate_columns,
    merge_on_composite_key,
    generate_mismatch_warnings,
    add_dropout_info,
)

setup_logging()
logger = logging.getLogger(__name__)


def _build_orphan_df(
    keys_diagnosis: set,
    keys_sizing: set,
    keys_histology: set,
    subject_col: str,
    polyp_col: str,
) -> pd.DataFrame:
    """
    Build a DataFrame of all (subjectId, polypId) pairs that are missing from
    at least one sheet, with YES/NO presence columns for each sheet.
    """
    all_keys = keys_diagnosis | keys_sizing | keys_histology
    orphan_keys = {
        k for k in all_keys
        if k not in keys_diagnosis or k not in keys_sizing or k not in keys_histology
    }

    rows = []
    for key in sorted(orphan_keys):
        subject, polyp = key.split('::', 1)
        rows.append({
            subject_col: subject,
            polyp_col: polyp,
            'in_Diagnosis': 'YES' if key in keys_diagnosis else 'NO',
            'in_Sizing': 'YES' if key in keys_sizing else 'NO',
            'in_Histology': 'YES' if key in keys_histology else 'NO',
        })

    return pd.DataFrame(rows)


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
    8. Compute symmetric differences and save orphan pairs file
    9. Save summary txt
    """
    # Load transformed sheets
    logger.info("=== Loading transformed sheets ===")
    diagnosis_df = load_file(args.diagnosis)
    sizing_df = load_file(args.sizing)
    histology_df = load_file(args.histology)

    logger.info(f"Diagnosis: {diagnosis_df.shape}")
    logger.info(f"Sizing: {sizing_df.shape}")
    logger.info(f"Histology: {histology_df.shape}")

    # Compute composite key sets before merge (original, unmodified dfs)
    keys_diagnosis = build_composite_key_set(diagnosis_df, args.subject_col, args.polyp_col)
    keys_sizing = build_composite_key_set(sizing_df, args.subject_col, args.polyp_col)
    keys_histology = build_composite_key_set(histology_df, args.subject_col, args.polyp_col)

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

    # Add randomizationGroup column if it exists in any sheet
    logger.info("=== Adding randomizationGroup column ===")

    org_col_candidates = [args.random_group_col]

    for df, name in zip(dataframes, df_names):
        for col in df.columns:
            if 'random' in col.lower() and 'group' in col.lower():
                logger.info(f"Found randomization group column '{col}' in {name}")
                if col not in org_col_candidates:
                    org_col_candidates.append(col)

    org_col_found = None
    for col in org_col_candidates:
        if col in merged_df.columns:
            org_col_found = col
            break

    if org_col_found:
        if org_col_found != args.random_group_col:
            merged_df = merged_df.rename(columns={org_col_found: args.random_group_col})
        logger.info(f"Added {args.random_group_col} column")
    else:
        logger.warning(f"{args.random_group_col} column not found - adding empty column")
        merged_df[args.random_group_col] = ''

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
    merged_df = generate_mismatch_warnings(merged_df, duplicate_cols)

    # Mark which sheets each polyp is missing from
    logger.info("=== Checking for missing polyps across sheets ===")
    merged_df['_check_key'] = (
        merged_df[args.subject_col].astype(str) + '::' +
        merged_df[args.polyp_col].astype(str)
    )

    def _missing_sheets(key: str) -> str:
        missing = []
        if key not in keys_diagnosis:
            missing.append('Diagnosis')
        if key not in keys_sizing:
            missing.append('Sizing')
        if key not in keys_histology:
            missing.append('Histology')
        return ', '.join(missing)

    merged_df['polyp_missing_in_sheets'] = merged_df['_check_key'].apply(_missing_sheets)
    merged_df = merged_df.drop(columns=['_check_key'])

    missing_count = (merged_df['polyp_missing_in_sheets'] != '').sum()
    if missing_count > 0:
        logger.warning(f"{missing_count} polyps missing from some sheets")

    # Save merged result
    logger.info("=== Saving result ===")
    save_excel_sheet(merged_df, args.output, sheet_name='MergedPolyps')

    # --- Symmetric differences ---
    diag_not_sizing = keys_diagnosis - keys_sizing
    sizing_not_diag = keys_sizing - keys_diagnosis
    diag_not_histology = keys_diagnosis - keys_histology
    histology_not_diag = keys_histology - keys_diagnosis
    sizing_not_histology = keys_sizing - keys_histology
    histology_not_sizing = keys_histology - keys_sizing

    # --- Orphan pairs ---
    logger.info("=== Building orphan pairs file ===")
    orphan_df = _build_orphan_df(
        keys_diagnosis, keys_sizing, keys_histology,
        args.subject_col, args.polyp_col,
    )
    orphan_path = args.output.with_suffix('.orphans.xlsx')
    save_excel_sheet(orphan_df, orphan_path, sheet_name='Orphans')
    logger.info(f"Orphan pairs saved to {orphan_path}")

    logger.info("=== Complete ===")

    # --- Build summary ---
    dropout_count = (merged_df['dropout_P'] != '').sum() if 'dropout_P' in merged_df.columns else 0
    mismatch_count = (merged_df['column_value_mismatch'] != '').sum()

    summary_lines = [
        "=== Summary ===",
        "",
        "Note: all polyp counts are based on the composite key subjectId::polypId.",
        "Each unique (subjectId, polypId) pair seen in any sheet is one row.",
        "",
        "--- Polyp counts per source sheet ---",
        f"  Diagnosis:  {len(keys_diagnosis)}",
        f"  Sizing:     {len(keys_sizing)}",
        f"  Histology:  {len(keys_histology)}",
        f"  Total rows in merged output: {len(merged_df)}",
        f"  Unique subjects: {merged_df[args.subject_col].nunique()}",
        "",
        "--- Symmetric differences (pairs in one sheet but not the other) ---",
        f"  In Diagnosis, not in Sizing:    {len(diag_not_sizing)}",
        f"  In Sizing, not in Diagnosis:    {len(sizing_not_diag)}",
        f"  In Diagnosis, not in Histology: {len(diag_not_histology)}",
        f"  In Histology, not in Diagnosis: {len(histology_not_diag)}",
        f"  In Sizing, not in Histology:    {len(sizing_not_histology)}",
        f"  In Histology, not in Sizing:    {len(histology_not_sizing)}",
        "",
        "--- Orphan pairs (missing from at least one sheet) ---",
        f"  Total orphan pairs: {len(orphan_df)}",
        f"  Saved to: {orphan_path}",
        "",
        "--- Other warnings ---",
        f"  Polyps missing from some sheets: {missing_count}",
        f"  Polyps from dropout subjects: {dropout_count}",
        f"  Polyps with column value mismatches: {mismatch_count}",
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
        "--random-group-col",
        type=str,
        default="randomizationGroup",
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
