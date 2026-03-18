# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2025-02-09

### Added

#### Core Utilities
- **Polyp ID normalization**: `normalize_polyp_id()` function handles format conversion (X-XXX-XXX → X-XXX-XX)
- **Composite key generation**: `build_composite_key()` creates unique identifiers from subject + polyp_id
- **Missed polyp detection**: `is_missed_polyp()` identifies polyps with letter-based IDs (e.g., 1-001-A)
- **Composite key extraction**: `extract_composite_keys()` applies key generation across dataframes
- **File I/O utilities**: Format-agnostic `load_file()` and `save_file()` supporting CSV and Excel
- **Excel fallback**: Automatic CSV conversion when openpyxl not available

#### Scripts
- **merge_filtered_raw.py**: Merges filtered and raw datasets with composite key matching
  - Row-level matching using subject + polyp_id
  - Preserves filtered dataset row ordering
  - Column interleaving: [filtered: A-K | raw: U | filtered: L-end | raw: V-Y]
  - Validates unmatched keys with detailed logging
- **identify_missed.py**: Filters datasets to only missed polyps
  - Configurable polyp ID column name
  - Letter-based third segment detection

#### Infrastructure
- **Logging system**: Centralized `setup_logging()` with console and optional file output
- **Package structure**: Installable `polyp_data_tools` with reusable utilities
- **Type hints**: Full type annotation coverage
- **Development tools**: Black, Ruff configuration

### Design Principles

This release follows the Jose Alonso Developer's Manifest:
- Radical separation of orchestration (scripts) and logic (utilities)
- Stateless logic engines with no side effects
- Explicit dependency injection (no globals or singletons)
- Clear functional boundaries with inline documentation

### Dependencies

- Python ≥ 3.10
- pandas ≥ 2.0.0
- numpy ≥ 1.24.0
- openpyxl ≥ 3.1.0 (optional, for Excel support)

### Known Limitations

- Column positions for interleaving are hardcoded (positions 0-10, 20, 11-end, 21-24)
- Assumes specific column names (`subject_id`, `polyp_id` vs `subject`, `id`)
- No automated testing infrastructure yet (planned for future releases)
- Scripts must be run directly (no CLI entry points)

### Notes

- Scripts are designed for direct execution from `scripts/` directory
- Excel support requires manual `openpyxl` installation
- Default output format is CSV for maximum compatibility