# src/polyp_data_tools/__init__.py

from polyp_data_tools.config import (
    setup_logging, 
)

from polyp_data_tools.data_utils import (
    is_missed_polyp,
    normalize_polyp_id, 
    build_composite_key,
    extract_composite_keys,
)

from polyp_data_tools.io_utils import (
    load_file, 
    save_file,
    revert_to_csv_if_no_excel_support,
    load_excel_sheet,
    load_excel_sheets,
    save_excel_sheet,
)

from polyp_data_tools.excel_ops import (
    excel_col_to_index,
    parse_excel_range,
    slice_dataframe_by_range,
    get_sheet_names,
)

from polyp_data_tools.wide_to_long import (
    PolypColumnGroup,
    detect_polyp_column_groups,
    filter_invalid_entries,
    transform_to_long_format,
)

from polyp_data_tools.merge_ops import (
    build_composite_key_set,
    detect_duplicate_columns,
    merge_dataframes_on_key,
    merge_on_composite_key,
    generate_mismatch_warnings,
    add_dropout_info,
    detect_missing_subjects,
)

__all__ = [
    # Config
    "setup_logging",
    # Data utilities
    "is_missed_polyp",
    "normalize_polyp_id",
    "build_composite_key",
    "extract_composite_keys",
    # IO utilities
    "load_file",
    "save_file",
    "revert_to_csv_if_no_excel_support",
    "load_excel_sheet",
    "load_excel_sheets",
    "save_excel_sheet",
    # Excel operations
    "excel_col_to_index",
    "parse_excel_range",
    "slice_dataframe_by_range",
    "get_sheet_names",
    # Wide-to-long transformation
    "PolypColumnGroup",
    "detect_polyp_column_groups",
    "filter_invalid_entries",
    "transform_to_long_format",
    # Merge operations
    "build_composite_key_set",
    "detect_duplicate_columns",
    "merge_dataframes_on_key",
    "merge_on_composite_key",
    "generate_mismatch_warnings",
    "add_dropout_info",
    "detect_missing_subjects",
]