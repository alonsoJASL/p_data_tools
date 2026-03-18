# src/polyp_data_tools/io_utils.py
# io.py - Deals with input/outputs 
import sys
import logging
import pandas as pd
from pathlib import Path
from typing import Dict, List

logger = logging.getLogger(__name__)


def load_file(filepath: Path) -> pd.DataFrame:
    """
    Load CSV or Excel file based on extension.
    Orchestration layer.
    """
    try:
        suffix = filepath.suffix.lower()
        
        if suffix == '.csv':
            df = pd.read_csv(filepath, dtype=str)
            logger.info(f"Loaded CSV {filepath.name}: {df.shape}")
        elif suffix in ['.xlsx', '.xls']:
            df = pd.read_excel(filepath, dtype=str)
            logger.info(f"Loaded Excel {filepath.name}: {df.shape}")
        else:
            logger.error(f"Unsupported file format: {suffix}. Use .csv, .xlsx, or .xls")
            sys.exit(1)
        
        return df
    except Exception as e:
        logger.error(f"Failed to load {filepath}: {e}")
        sys.exit(1)


def save_file(df: pd.DataFrame, filepath: Path) -> None:
    """
    Save to CSV or Excel based on extension.
    Orchestration layer.
    """
    try:
        filepath.parent.mkdir(parents=True, exist_ok=True)
        suffix = filepath.suffix.lower()
        
        if suffix == '.csv':
            df.to_csv(filepath, index=False)
            logger.info(f"Saved CSV to {filepath}")
        elif suffix in ['.xlsx', '.xls']:
            df.to_excel(filepath, index=False)
            logger.info(f"Saved Excel to {filepath}")
        else:
            logger.error(f"Unsupported output format: {suffix}. Use .csv, .xlsx, or .xls")
            sys.exit(1)
    except Exception as e:
        logger.error(f"Failed to save {filepath}: {e}")
        sys.exit(1)


def revert_to_csv_if_no_excel_support(path_to_file: Path) -> Path:
    """
    Checks if the openpyxl library is available for handling Excel files.
    Returns True if supported, False otherwise.
    """
    try:
        import openpyxl  # noqa: F401
        return path_to_file
    except ImportError:
        logging.warning(f"Excel support not available. Changing output to CSV.")
        return path_to_file.with_suffix('.csv')


def load_excel_sheet(
    filepath: Path,
    sheet_name: str,
    header_row: int = 0,
    dtype: str = 'str'
) -> pd.DataFrame:
    """
    Load a specific sheet from an Excel file.
    
    Args:
        filepath: Path to Excel file
        sheet_name: Name of sheet to load
        header_row: Row index to use as column headers (0-indexed)
        dtype: Data type to use for all columns
    
    Returns:
        DataFrame with loaded data
    """
    try:
        df = pd.read_excel(
            filepath,
            sheet_name=sheet_name,
            header=header_row,
            dtype=dtype
        )
        logger.info(f"Loaded sheet '{sheet_name}' from {filepath.name}: {df.shape}")
        return df
    except Exception as e:
        logger.error(f"Failed to load sheet '{sheet_name}' from {filepath}: {e}")
        sys.exit(1)


def load_excel_sheets(
    filepath: Path,
    sheet_names: List[str],
    header_row: int = 0,
    dtype: str = 'str'
) -> Dict[str, pd.DataFrame]:
    """
    Load multiple sheets from an Excel file.
    
    Args:
        filepath: Path to Excel file
        sheet_names: List of sheet names to load
        header_row: Row index to use as column headers (0-indexed)
        dtype: Data type to use for all columns
    
    Returns:
        Dictionary mapping sheet name to DataFrame
    """
    sheets = {}
    for sheet_name in sheet_names:
        sheets[sheet_name] = load_excel_sheet(filepath, sheet_name, header_row, dtype)
    
    return sheets


def save_excel_sheet(
    df: pd.DataFrame,
    filepath: Path,
    sheet_name: str = 'Sheet1'
) -> None:
    """
    Save DataFrame to a specific sheet in an Excel file.
    
    Args:
        df: DataFrame to save
        filepath: Path to output Excel file
        sheet_name: Name of sheet to create
    """
    try:
        filepath.parent.mkdir(parents=True, exist_ok=True)
        
        with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        logger.info(f"Saved sheet '{sheet_name}' to {filepath}")
    except Exception as e:
        logger.error(f"Failed to save sheet to {filepath}: {e}")
        sys.exit(1)