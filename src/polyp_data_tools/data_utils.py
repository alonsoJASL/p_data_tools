"""
Data utilities for polyp ID normalization and classification.
Stateless logic layer - no I/O or orchestration.
"""
import re

import pandas as pd
import numpy as np
from typing import Optional


def normalize_polyp_id(polyp_id: str) -> str:
    """
    Normalize polyp ID from X-XXX-XXX to X-XXX-XX format.
    Example: "1-001-001" -> "1-001-01"
    
    Logic:
    - Part 1 (a): integer, no leading zeros
    - Part 2 (b): 3-digit zero-padded
    - Part 3 (c): if numeric -> 2-digit zero-padded; if letter -> keep as-is
    
    Returns normalized ID or original string if format is unexpected.
    """
    if pd.isna(polyp_id):
        return np.nan
    
    s = str(polyp_id).strip()
    parts = s.split("-")
    
    if len(parts) != 3:
        return s
    
    a, b, c = parts
    
    # Normalize first two parts
    if not (re.fullmatch(r"\d+", a) and re.fullmatch(r"\d+", b)):
        return s
    
    a_norm = str(int(a))
    b_norm = f"{int(b):03d}"
    
    # Normalize third part: numeric -> 2-digit, letter -> keep
    if re.fullmatch(r"\d+", c):
        c_norm = f"{int(c):02d}"
    else:
        c_norm = c.strip()
    
    return f"{a_norm}-{b_norm}-{c_norm}"


def is_missed_polyp(polyp_id: str) -> bool:
    """
    Check if polyp ID represents a missed polyp.
    Missed polyps have a letter (A, B, C, etc.) in the third segment.
    
    Examples:
        "1-001-A" -> True (missed)
        "1-001-002" -> False (not missed)
        "1-001-01" -> False (not missed)
    
    Returns False for malformed IDs.
    """
    if pd.isna(polyp_id):
        return False
    
    s = str(polyp_id).strip()
    parts = s.split("-")
    
    if len(parts) != 3:
        return False
    
    third_segment = parts[2].strip()
    
    # If third segment is NOT purely numeric, it's a missed polyp
    return not re.fullmatch(r"\d+", third_segment)


def build_composite_key(subject: str, polyp_id: str) -> str:
    """
    Build composite key from subject and normalized polyp_id.
    Returns: "subject::polyp_id"
    """
    if pd.isna(subject) or pd.isna(polyp_id):
        return np.nan
    
    normalized_id = normalize_polyp_id(polyp_id)
    return f"{str(subject).strip()}::{normalized_id}"

def extract_composite_keys(
    df: pd.DataFrame,
    subject_col: str,
    polyp_col: str
) -> pd.Series:
    """
    Extract composite keys from dataframe.
    Logic layer - stateless.
    """
    return df.apply(
        lambda row: build_composite_key(row[subject_col], row[polyp_col]),
        axis=1
    )