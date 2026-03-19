"""
Multi-dataframe merging operations with duplicate column handling and warnings.
Stateless logic layer - no I/O.
"""
import logging
import pandas as pd
import numpy as np
from typing import List, Dict, Set, Tuple, Optional

logger = logging.getLogger(__name__)


def detect_duplicate_columns(
    dataframes: List[pd.DataFrame],
    df_names: List[str]
) -> Dict[str, List[str]]:
    """
    Detect columns that appear in multiple dataframes.
    
    Args:
        dataframes: List of DataFrames to check
        df_names: Names/labels for each DataFrame (for reporting)
    
    Returns:
        Dict mapping column name to list of dataframe names where it appears
        Only includes columns that appear in 2+ dataframes
    """
    column_sources = {}
    
    for df, name in zip(dataframes, df_names):
        for col in df.columns:
            if col not in column_sources:
                column_sources[col] = []
            column_sources[col].append(name)
    
    # Filter to only duplicates
    duplicates = {
        col: sources 
        for col, sources in column_sources.items() 
        if len(sources) > 1
    }
    
    if duplicates:
        logger.info(f"Found {len(duplicates)} duplicate column names across dataframes")
        for col, sources in duplicates.items():
            logger.debug(f"  '{col}' appears in: {', '.join(sources)}")
    
    return duplicates


def merge_dataframes_on_key(
    dataframes: List[pd.DataFrame],
    df_names: List[str],
    key_col: str,
    how: str = 'outer'
) -> pd.DataFrame:
    """
    Merge multiple dataframes on a single key column.
    Handles duplicate columns by keeping first occurrence and suffixing others.
    
    Args:
        dataframes: List of DataFrames to merge
        df_names: Names for each DataFrame (used for suffixes)
        key_col: Column name to merge on
        how: Merge type ('inner', 'outer', 'left', 'right')
    
    Returns:
        Merged DataFrame
    """
    if not dataframes:
        raise ValueError("No dataframes provided for merging")
    
    if len(dataframes) != len(df_names):
        raise ValueError("Number of dataframes must match number of names")
    
    # Start with first dataframe
    result = dataframes[0].copy()
    logger.info(f"Starting merge with {df_names[0]}: {result.shape}")
    
    # Merge each subsequent dataframe
    for i, (df, name) in enumerate(zip(dataframes[1:], df_names[1:]), start=1):
        # Find columns that would be duplicated
        duplicate_cols = set(result.columns) & set(df.columns) - {key_col}
        
        if duplicate_cols:
            logger.info(
                f"Merging {name}: {len(duplicate_cols)} duplicate columns will be suffixed"
            )
        
        result = result.merge(
            df,
            on=key_col,
            how=how,
            suffixes=('', f'_{name}')
        )
        
        logger.info(f"After merging {name}: {result.shape}")
    
    return result


def merge_on_composite_key(
    dataframes: List[pd.DataFrame],
    df_names: List[str],
    subject_col: str,
    polyp_col: str,
    how: str = 'outer'
) -> pd.DataFrame:
    """
    Merge multiple dataframes on composite key (subject + polyp).
    
    Args:
        dataframes: List of DataFrames to merge
        df_names: Names for each DataFrame
        subject_col: Subject ID column name
        polyp_col: Polyp ID column name
        how: Merge type
    
    Returns:
        Merged DataFrame
    """
    # Create composite key in each dataframe
    keyed_dfs = []
    for df in dataframes:
        df_copy = df.copy()
        df_copy['_merge_key'] = (
            df_copy[subject_col].astype(str) + '::' + 
            df_copy[polyp_col].astype(str)
        )
        keyed_dfs.append(df_copy)
    
    # Merge on composite key
    merged = merge_dataframes_on_key(keyed_dfs, df_names, '_merge_key', how)
    
    # Drop the temporary merge key
    merged = merged.drop(columns=['_merge_key'])
    
    return merged


def build_composite_key_set(
    df: pd.DataFrame,
    subject_col: str,
    polyp_col: str,
) -> Set[str]:
    """
    Build the set of composite keys ('subjectId::polypId') from a DataFrame.

    Uses the same raw-string format as merge_on_composite_key — no polyp ID
    normalisation is applied. Use this when you need a key set to compare
    against the keys produced by that merge.

    Args:
        df: DataFrame containing subject and polyp columns
        subject_col: Subject ID column name
        polyp_col: Polyp ID column name

    Returns:
        Set of 'subject::polyp' strings
    """
    return set(
        df[subject_col].astype(str) + '::' + df[polyp_col].astype(str)
    )


def generate_mismatch_warnings(
    df: pd.DataFrame,
    duplicate_cols: Dict[str, List[str]],
) -> pd.DataFrame:
    """
    Generate warnings for rows where duplicate columns have mismatched values.

    Adds two columns to the DataFrame:
      - column_value_mismatch: semicolon-separated list of "<col>: A vs B" for
        each duplicate column whose non-null values differ across suffixed variants.
      - duplicate_column_names: semicolon-separated list of all duplicate column
        names with their source sheets, regardless of whether values match.

    Args:
        df: Merged DataFrame (may have suffixed duplicate columns)
        duplicate_cols: Dict of column -> list of source dataframes

    Returns:
        DataFrame with warning columns added
    """
    value_mismatch_col = []
    duplicate_names_col = []

    for _, row in df.iterrows():
        value_mismatches = []
        duplicate_names = []

        for col, sources in duplicate_cols.items():
            if len(sources) > 1:
                duplicate_names.append(f"{col} ({', '.join(sources)})")

                col_variants = [col] + [f"{col}_{src}" for src in sources[1:]]
                existing_variants = [v for v in col_variants if v in df.columns]

                if len(existing_variants) > 1:
                    values = [row[v] for v in existing_variants if pd.notna(row[v])]
                    unique_values = set(str(v) for v in values)

                    if len(unique_values) > 1:
                        value_mismatches.append(
                            f"{col}: {' vs '.join(unique_values)}"
                        )

        value_mismatch_col.append('; '.join(value_mismatches) if value_mismatches else '')
        duplicate_names_col.append('; '.join(duplicate_names) if duplicate_names else '')

    df['column_value_mismatch'] = value_mismatch_col
    df['duplicate_column_names'] = duplicate_names_col

    return df


def add_dropout_info(
    df: pd.DataFrame,
    study_exit_df: pd.DataFrame,
    subject_col: str,
    dropout_indicator_col: str = 'O',
    dropout_info_cols: Optional[List[str]] = None
) -> pd.DataFrame:
    """
    Add dropout information from StudyExit sheet to main dataframe.
    
    Args:
        df: Main DataFrame
        study_exit_df: StudyExit DataFrame
        subject_col: Subject ID column name
        dropout_indicator_col: Column indicating dropout status (name or Excel position like 'O')
        dropout_info_cols: Columns to extract for dropouts (names or Excel positions like ['P', 'Q', ...])
                          If None, defaults to columns 15-20 (Excel P:U)
    
    Returns:
        DataFrame with dropout info added
    """
    if dropout_info_cols is None:
        # Default to columns P through U (Excel positions 15-20, 0-indexed 15-20)
        # If column names not provided, try to use positions
        dropout_info_cols = []
        if len(study_exit_df.columns) > 20:
            dropout_info_cols = study_exit_df.columns[15:21].tolist()
        else:
            logger.warning("StudyExit has fewer columns than expected for dropout info")
    
    # Handle dropout indicator column - try name first, then position
    if dropout_indicator_col not in study_exit_df.columns:
        # Try to interpret as Excel column position (e.g., 'O' = position 14)
        if len(dropout_indicator_col) == 1 and dropout_indicator_col.isalpha():
            from polyp_data_tools.excel_ops import excel_col_to_index
            col_idx = excel_col_to_index(dropout_indicator_col)
            if col_idx < len(study_exit_df.columns):
                dropout_indicator_col = study_exit_df.columns[col_idx]
                logger.info(f"Using column at position {col_idx}: '{dropout_indicator_col}'")
            else:
                logger.error(f"Column position {dropout_indicator_col} out of range")
                return df
        else:
            logger.error(f"Dropout indicator column '{dropout_indicator_col}' not found")
            logger.error(f"Available columns: {study_exit_df.columns.tolist()}")
            return df
    
    # Filter StudyExit to only dropouts
    dropout_mask = study_exit_df[dropout_indicator_col].astype(str).str.lower() == 'yes'
    dropouts = study_exit_df[dropout_mask].copy()
    
    logger.info(f"Found {len(dropouts)} dropout subjects in StudyExit")
    
    if len(dropouts) == 0:
        # No dropouts - add empty columns
        for col in dropout_info_cols:
            df[f'dropout_{col}'] = ''
        return df
    
    # Select relevant columns
    cols_to_merge = [subject_col] + [
        col for col in dropout_info_cols if col in study_exit_df.columns
    ]
    dropout_data = dropouts[cols_to_merge]
    
    # Rename dropout info columns to avoid conflicts
    rename_map = {col: f'dropout_{col}' for col in dropout_info_cols if col in dropout_data.columns}
    dropout_data = dropout_data.rename(columns=rename_map)
    
    # Merge dropout info
    df = df.merge(dropout_data, on=subject_col, how='left')
    
    # Fill NaN in dropout columns with empty string (subjects who aren't dropouts)
    for col in rename_map.values():
        if col in df.columns:
            df[col] = df[col].fillna('')
    
    logger.info(f"Added {len(rename_map)} dropout info columns")
    
    return df


def detect_missing_subjects(
    dataframes: List[pd.DataFrame],
    df_names: List[str],
    subject_col: str
) -> pd.DataFrame:
    """
    Detect subjects that are present in some dataframes but not others.
    
    Args:
        dataframes: List of DataFrames
        df_names: Names for each DataFrame
        subject_col: Subject ID column name
    
    Returns:
        DataFrame with subject_id and missing_in columns
    """
    # Collect all unique subjects from each dataframe
    subject_sets = {}
    for df, name in zip(dataframes, df_names):
        if subject_col in df.columns:
            subject_sets[name] = set(df[subject_col].dropna().unique())
        else:
            logger.warning(f"Subject column '{subject_col}' not found in {name}")
            subject_sets[name] = set()
    
    # Find union of all subjects
    all_subjects = set().union(*subject_sets.values())
    
    # For each subject, find which dataframes it's missing from
    missing_info = []
    for subject in all_subjects:
        missing_from = [name for name, subjects in subject_sets.items() if subject not in subjects]
        if missing_from:
            missing_info.append({
                subject_col: subject,
                'missing_in': ', '.join(missing_from)
            })
    
    if missing_info:
        logger.warning(f"Found {len(missing_info)} subjects missing from some dataframes")
    
    return pd.DataFrame(missing_info)