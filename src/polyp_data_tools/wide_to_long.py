"""
Wide-to-long transformation for polyp data.
Handles conversion from patient-rows with multiple polyps to polyp-rows.
Stateless logic layer - no I/O.
"""
import re
import logging
import pandas as pd
import numpy as np
from dataclasses import dataclass
from typing import List, Optional

logger = logging.getLogger(__name__)


@dataclass
class PolypColumnGroup:
    """
    Represents a group of columns for a single polyp.
    
    Example:
        polyp_number: 1
        polyp_id_col: "polypId-1"
        info_columns: ["size-1", "location-1", "diagnosis-1"]
    """
    polyp_number: int
    polyp_id_col: str
    info_columns: List[str]


def detect_polyp_column_groups(
    df: pd.DataFrame,
    polyp_id_pattern: str = "polypId-{n}",
    max_polyps: Optional[int] = None
) -> List[PolypColumnGroup]:
    """
    Detect polyp column groups by scanning DataFrame columns.
    
    Strategy:
    1. Find all columns matching polyp ID pattern (e.g., "polypId-1", "polypId-2")
    2. For each polyp ID column, gather all columns between it and the next polyp ID
    3. Return list of PolypColumnGroup objects
    
    Args:
        df: DataFrame to scan
        polyp_id_pattern: Pattern for polyp ID columns (e.g., "polypId-{n}")
        max_polyps: Maximum number of polyps to detect (None = auto-detect all)
    
    Returns:
        List of PolypColumnGroup objects, sorted by polyp_number
    """
    # Convert pattern to regex
    # "polypId-{n}" -> r"polypId-(\d+)"
    pattern_regex = polyp_id_pattern.replace("{n}", r"(\d+)")
    
    columns = df.columns.tolist()
    groups = []
    
    # Find all polyp ID columns and their positions
    polyp_id_cols = []
    for idx, col in enumerate(columns):
        match = re.fullmatch(pattern_regex, str(col), re.IGNORECASE)
        if match:
            polyp_num = int(match.group(1))
            polyp_id_cols.append((idx, polyp_num, col))
    
    if not polyp_id_cols:
        logger.warning(f"No columns matching pattern '{polyp_id_pattern}' found")
        return []
    
    # Sort by polyp number
    polyp_id_cols.sort(key=lambda x: x[1])
    
    logger.info(f"Detected {len(polyp_id_cols)} polyp ID columns")
    
    # For each polyp ID column, collect info columns until next polyp ID
    for i, (col_idx, polyp_num, polyp_col) in enumerate(polyp_id_cols):
        # Determine range of info columns
        start_idx = col_idx + 1
        
        if i + 1 < len(polyp_id_cols):
            # Next polyp ID exists - stop before it
            end_idx = polyp_id_cols[i + 1][0]
        else:
            # Last polyp - take all remaining columns
            end_idx = len(columns)
        
        # Collect info columns
        info_cols = columns[start_idx:end_idx]
        
        group = PolypColumnGroup(
            polyp_number=polyp_num,
            polyp_id_col=polyp_col,
            info_columns=info_cols
        )
        groups.append(group)
        
        logger.debug(
            f"Polyp {polyp_num}: ID col '{polyp_col}', "
            f"{len(info_cols)} info columns"
        )
    
    # Filter by max_polyps if specified
    if max_polyps is not None:
        groups = [g for g in groups if g.polyp_number <= max_polyps]
        logger.info(f"Limited to {max_polyps} polyps: {len(groups)} groups")
    
    return groups


def filter_invalid_entries(
    df: pd.DataFrame,
    polyp_id_col: str,
    invalid_markers: List[str] = ['.b', '.h', '.n']
) -> pd.DataFrame:
    """
    Filter out rows with invalid polyp ID markers.
    
    Args:
        df: DataFrame with polyp IDs
        polyp_id_col: Name of polyp ID column
        invalid_markers: List of strings indicating invalid/missing polyps
    
    Returns:
        Filtered DataFrame
    """
    original_count = len(df)
    
    # Convert to string and strip whitespace
    polyp_ids = df[polyp_id_col].astype(str).str.strip()
    
    # Filter out:
    # 1. Invalid markers (.b, .h, .n)
    # 2. NaN/null values
    # 3. Empty strings
    mask = ~polyp_ids.isin(invalid_markers)
    mask = mask & df[polyp_id_col].notna()
    mask = mask & (polyp_ids != '')
    
    filtered_df = df[mask].copy()
    
    removed_count = original_count - len(filtered_df)
    if removed_count > 0:
        logger.info(
            f"Filtered out {removed_count} invalid/empty entries "
            f"(markers: {invalid_markers}, plus empty strings)"
        )
    
    return filtered_df


def transform_to_long_format(
    df: pd.DataFrame,
    subject_col: str,
    column_groups: List[PolypColumnGroup],
    preserve_cols: Optional[List[str]] = None,
    invalid_markers: List[str] = ['.b', '.h', '.n']
) -> pd.DataFrame:
    """
    Transform wide-format polyp data to long format.
    
    Strategy:
    1. For each polyp column group, extract subject + polyp ID + info columns
    2. Stack all groups vertically
    3. Filter out invalid entries
    
    Args:
        df: Wide-format DataFrame
        subject_col: Name of subject ID column
        column_groups: List of PolypColumnGroup objects
        preserve_cols: Additional subject-level columns to preserve (e.g., randomizationGroup)
        invalid_markers: Markers for invalid/missing polyps
    
    Returns:
        Long-format DataFrame with columns: [subject_col, polyp_id, preserve_cols..., info_cols...]
    """
    if not column_groups:
        raise ValueError("No column groups provided for transformation")
    
    if preserve_cols is None:
        preserve_cols = []
    
    # Validate that preserve_cols exist in dataframe
    missing_preserve = [col for col in preserve_cols if col not in df.columns]
    if missing_preserve:
        logger.warning(f"Preserve columns not found in dataframe: {missing_preserve}")
        preserve_cols = [col for col in preserve_cols if col in df.columns]
    
    long_dfs = []
    
    for group in column_groups:
        # Extract relevant columns for this polyp
        # Order: subject_col, polyp_id_col, preserve_cols, info_columns
        cols_to_extract = [subject_col, group.polyp_id_col] + preserve_cols + group.info_columns
        
        # Ensure all columns exist
        missing_cols = [c for c in cols_to_extract if c not in df.columns]
        if missing_cols:
            logger.warning(
                f"Polyp {group.polyp_number}: Missing columns {missing_cols}, skipping"
            )
            continue
        
        polyp_df = df[cols_to_extract].copy()
        
        # Rename polyp ID column to standard name
        polyp_df = polyp_df.rename(columns={group.polyp_id_col: 'polypId'})
        
        # Rename info columns to remove polyp number suffix
        # e.g., "size-1" -> "size", "Q1_R1_C2" -> "Q1_C2"
        rename_map = {}
        for col in group.info_columns:
            # Remove trailing "-N" pattern (e.g., "size-1" -> "size")
            base_name = re.sub(r'-\d+$', '', col)
            # Also handle _RN_ pattern (e.g., "Q1_R1_C2" -> "Q1_C2")
            base_name = re.sub(r'_R\d+_', '_', base_name)
            rename_map[col] = base_name
        
        polyp_df = polyp_df.rename(columns=rename_map)
        
        long_dfs.append(polyp_df)
    
    # Concatenate all polyp dataframes
    if not long_dfs:
        raise ValueError("No valid polyp data found after processing column groups")
    
    combined_df = pd.concat(long_dfs, axis=0, ignore_index=True)
    
    logger.info(f"Combined {len(long_dfs)} polyp groups into {len(combined_df)} rows")
    
    # Filter out invalid entries
    combined_df = filter_invalid_entries(combined_df, 'polypId', invalid_markers)
    
    return combined_df