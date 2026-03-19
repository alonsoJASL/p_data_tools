# Script Reference

Covers the three main pipeline scripts and the design decisions behind how each
handles data quality problems: duplicate columns, missing subjects, value mismatches,
and invalid entries.

---

## concatenate_sheets.py

**Purpose.** Merge three sheets — ColonoscopyProcedure, StudyExit (partial), and
AnnotationTracking (partial) — into a single wide record keyed by subject.

**Invocation.**
```
python scripts/concatenate_sheets.py \
  --file-a <colonoscopy+studyexit.xlsx> \
  --file-b <annotation.xlsx> \
  --output <merged.xlsx> \
  --subject-col subjectId
```

**Sheet and column selection.** By default the script loads:

| Source | Sheet | Columns |
|---|---|---|
| file_a | ColonoscopyProcedure | all |
| file_a | StudyExit | C onward |
| file_b | Annotation tracking | A:O |

This is controlled by `DEFAULT_CONFIG` in the script. To override, pass `--config
<path.json>` with the same structure. The active config is always written alongside the
output as `<output>.config.json` so the run is reproducible.

**Merge strategy.** Outer join on a synthetic `_merge_key` (a copy of `subjectId`)
rather than on `subjectId` directly. This preserves the subject ID column from each
source as a separate column in the output — `subjectId` (ColonoscopyProcedure),
`subjectId_StudyExit`, `subjectId_AnnotationTracking` — so they can be compared during
auditing. A subject present in any sheet will appear in the output; columns from absent
sheets are NaN.

**Outputs.** Three files are written alongside the main Excel output:

| File | Contents |
|---|---|
| `<output>.xlsx` | Merged sheet |
| `<output>.config.json` | Active config snapshot for reproducibility |
| `<output>.summary.txt` | Human-readable summary (also logged to stdout) |

### Collision and issue handling

**Duplicate columns across sheets.**
Before merging, `detect_duplicate_columns` scans all three DataFrames and records
every column name that appears in more than one source. When pandas merges these, the
first occurrence keeps its original name and subsequent occurrences get a `_<sheet>`
suffix (e.g., `date_StudyExit`). The set of duplicates is passed to
`generate_mismatch_warnings` after the merge. Note: `subjectId` will always appear as
a duplicate (intentionally — it is present in all three sheets by design).

**Value mismatch warnings.**
`generate_mismatch_warnings` iterates every row and, for each duplicate column,
compares the values across all suffixed variants. If the non-null values differ, the
discrepancy is written into the `column_value_mismatch` column as a human-readable
string: `<column>: <value_a> vs <value_b>`. Rows without any mismatch get an empty
string. No data is modified or dropped — the warning is purely additive.

**Missing subjects.**
`detect_missing_subjects` computes the union of all subject IDs across the three sheets
and records which sheets each subject is absent from. This is written into the
`subject_missing_in_sheets` column of the output. Sheet names in this column use the
cleaned form (`ColonoscopyProcedure`, `StudyExit`, `AnnotationTracking`).

**Summary at completion.**
The summary is both logged and written to `<output>.summary.txt`. It includes:

```
Total rows in merged output: N

--- Subjects available ---
  Total ColonoscopyProcedures: N
  AnnotationTracking linked (linked = YES): N
  Procedures needing annotation: N

--- Subjects missing from sheets ---
  Total subjects with any gap: N
  Missing from ColonoscopyProcedure AND StudyExit: N
  Missing only from ColonoscopyProcedure: N
  Missing only from StudyExit: N
  Missing only from AnnotationTracking: N
  [Subject IDs listed inline if non-zero]

--- Column warnings ---
  Subjects with column value mismatches: N
  Duplicate columns found: N
```

---

## transform_wide_to_long.py

**Purpose.** Convert a patient-row sheet (one row per subject, many polyp columns) into
a polyp-row sheet (one row per polyp).

**Invocation.**
```
python scripts/transform_wide_to_long.py \
  --input-file <wide.xlsx> \
  --sheet-name Diagnosis \
  --output <long.xlsx> \
  --subject-col subjectId \
  --polyp-id-pattern "Q1_R{n}_C1"
```

**Column group detection.** The pattern argument (default `Q1_R{n}_C1`) is compiled
into a regex by replacing `{n}` with `(\d+)`. All columns matching this pattern become
polyp anchor columns. The info columns for polyp N are every column between the
anchor for polyp N and the anchor for polyp N+1 (or end of sheet for the last polyp).
This relies entirely on column order in the source sheet.

**Renaming during unpivot.** Each info column has its polyp-number suffix stripped so
all polyps share consistent column names in the output:
- Trailing `-N` removed: `size-1` → `size`
- `_RN_` pattern collapsed: `Q1_R1_C2` → `Q1_C2`

### Collision and issue handling

**Invalid polyp entries.**
After stacking all groups, `filter_invalid_entries` removes rows where the polyp ID
column contains `.b`, `.h`, or `.n` (placeholder markers used in the source data), is
null, or is an empty string. The count of removed rows is logged.

**Missing preserve columns.**
Columns passed via `--preserve-cols` (default: `randomizationGroup`) that are absent
from the sheet are logged as a warning and silently skipped rather than raising an
error. The transformation continues with whatever preserve columns are available.

**No polyp columns detected.**
If no columns match the pattern the script logs the full column list and the pattern
being searched, then exits cleanly without writing any output file.

**Summary logged at completion.**
```
Input: N subjects
Output: N polyps
Average polyps per subject: N.NN
```

---

## merge_polyp_data.py

**Purpose.** Merge three long-format sheets — Diagnosis, Sizing, Histology — on the
composite key `subjectId::polypId`, then attach dropout information from the StudyExit
sheet.

**Invocation.**
```
python scripts/merge_polyp_data.py \
  --diagnosis <diagnosis_long.xlsx> \
  --sizing <sizing_long.xlsx> \
  --histology <histology_long.xlsx> \
  --study-exit-file <source.xlsx> \
  --output <merged_polyps.xlsx>
```

**Merge strategy.** Outer join on the composite key `subjectId::polypId`. A polyp
present in any of the three sheets will appear in the output; columns from absent
sheets will be NaN. `subjectId` and `polypId` are each carried in full from all three
inputs — duplicate key columns are suffixed the same way as other duplicate columns.

**Counting note.** All polyp quantities in the summary are based on the composite key.
Each unique `(subjectId, polypId)` pair seen in any sheet is one row in the merged
output.

**Outputs.** Two files are written alongside the main Excel output:

| File | Contents |
|---|---|
| `<output>.xlsx` | Merged polyp sheet |
| `<output>.orphans.xlsx` | All pairs missing from at least one sheet |
| `<output>.summary.txt` | Human-readable summary (also logged to stdout) |

### Collision and issue handling

**Duplicate columns across sheets.**
Same mechanism as `concatenate_sheets`: `detect_duplicate_columns` before merge,
pandas suffix-on-collision during merge, `generate_mismatch_warnings` after. The
`column_value_mismatch` column is written to output with the same `<col>: A vs B`
format.

**Polyps missing from some sheets.**
Composite key sets are built from each source sheet before the merge. After merging,
every row's key is checked against those sets. Which sheets a polyp is absent from is
written into the `polyp_missing_in_sheets` column (e.g., `"Sizing, Histology"`).

**Symmetric differences.**
The summary reports all six pairwise symmetric differences between Diagnosis, Sizing,
and Histology — i.e., how many pairs appear in one sheet but not the other, in both
directions for each pair.

**Orphan pairs file.**
Every `(subjectId, polypId)` pair that is absent from at least one sheet is written to
`<output>.orphans.xlsx` with `in_Diagnosis`, `in_Sizing`, and `in_Histology` columns
(YES/NO) to pinpoint exactly where each pair is and is not present.

**Dropout information.**
`add_dropout_info` joins dropout subjects from StudyExit into the polyp-level output.
The dropout indicator column defaults to Excel column O (index 14) and is resolved by
letter-to-index conversion if no column by that name exists. Only subjects whose
indicator value is `"yes"` (case-insensitive) are considered dropouts. Their data from
columns P–U (indices 15–20) is left-joined onto every polyp row belonging to that
subject and prefixed `dropout_`. Non-dropout rows get empty strings in those columns.
If StudyExit has fewer than 21 columns the script logs a warning and skips the
dropout column extraction.

**randomizationGroup column.**
The script searches all three input DataFrames for any column whose name contains both
`random` and `group` (case-insensitive). The first match found is renamed to the value
of `--random-group-col` (default: `randomizationGroup`) in the output. If no such
column exists, an empty column is added with a logged warning.

**Summary at completion.**
The summary is both logged and written to `<output>.summary.txt`. It includes:

```
Note: all polyp counts are based on the composite key subjectId::polypId.

--- Polyp counts per source sheet ---
  Diagnosis:  N
  Sizing:     N
  Histology:  N
  Total rows in merged output: N
  Unique subjects: N

--- Symmetric differences (pairs in one sheet but not the other) ---
  In Diagnosis, not in Sizing:    N
  In Sizing, not in Diagnosis:    N
  In Diagnosis, not in Histology: N
  In Histology, not in Diagnosis: N
  In Sizing, not in Histology:    N
  In Histology, not in Sizing:    N

--- Orphan pairs (missing from at least one sheet) ---
  Total orphan pairs: N
  Saved to: <path>

--- Other warnings ---
  Polyps missing from some sheets: N
  Polyps from dropout subjects: N
  Polyps with column value mismatches: N
```
