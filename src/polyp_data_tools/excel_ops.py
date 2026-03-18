# src/polly_data_tools/excel_ops.py

"""
Excel-specific operations for column range parsing and slicing.
Stateless logic layer - no I/O.
"""
import re
import pandas as pd
from typing import Optional, Tuple


def excel_col_to_index(col: str) -> int:
    """
    Convert Excel column letter to 0-based index.
    Examples: A -> 0, B -> 1, Z -> 25, AA -> 26
    """
    col = col.upper().strip()
    result = 0
    for char in col:
        result = result * 26 + (ord(char) - ord('A') + 1)
    return result - 1


def parse_excel_range(range_str: Optional[str]) -> Tuple[Optional[int], Optional[int]]:
    """
    Parse Excel-style column range to 0-based indices.
    
    Examples:
        None -> (None, None)  # All columns
        "G:" -> (6, None)     # From G to end
        "B:O" -> (1, 14)      # From B to O inclusive
        "A:A" -> (0, 0)       # Single column A
    
    Returns:
        (start_idx, end_idx) where end_idx is inclusive
        None values mean unbounded
    """
    if range_str is None:
        return (None, None)
    
    range_str = range_str.strip()
    
    # Match pattern like "A:", "G:", "B:O"
    match = re.match(r'^([A-Z]+):([A-Z]*)$', range_str, re.IGNORECASE)
    
    if not match:
        raise ValueError(
            f"Invalid Excel range format: '{range_str}'. "
            f"Expected format: 'A:', 'G:', or 'B:O'"
        )
    
    start_col = match.group(1)
    end_col = match.group(2)
    
    start_idx = excel_col_to_index(start_col)
    end_idx = excel_col_to_index(end_col) if end_col else None
    
    return (start_idx, end_idx)


def slice_dataframe_by_range(
    df: pd.DataFrame, 
    range_str: Optional[str]
) -> pd.DataFrame:
    """
    Slice DataFrame columns using Excel-style range.
    
    Args:
        df: DataFrame to slice
        range_str: Excel range like "G:", "B:O", or None for all columns
    
    Returns:
        Sliced DataFrame
    """
    start_idx, end_idx = parse_excel_range(range_str)
    
    if start_idx is None and end_idx is None:
        # No slicing - return all columns
        return df
    
    if end_idx is None:
        # Open-ended range like "G:"
        return df.iloc[:, start_idx:]
    else:
        # Bounded range like "B:O" (end_idx is inclusive, so +1 for Python slicing)
        return df.iloc[:, start_idx:end_idx + 1]


def get_sheet_names(filepath) -> list:
    """
    Get list of sheet names from an Excel file.
    
    Args:
        filepath: Path to Excel file
    
    Returns:
        List of sheet names
    """
    import openpyxl
    from pathlib import Path
    
    wb = openpyxl.load_workbook(Path(filepath), read_only=True, data_only=True)
    sheet_names = wb.sheetnames
    wb.close()
    
    return sheet_names