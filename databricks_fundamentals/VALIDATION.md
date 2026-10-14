# Validation and rehearsal status

## Completed locally

- Parsed all 17 Python notebook code cells with Python's AST parser.
- Verified notebook JSON structure and empty execution outputs.
- Generated fixtures from the notebook's deterministic generator.
- Independently checked 10 customers, 5 products and 23 order rows.
- Checked the two invalid order IDs and the one exact duplicate.
- Checked unique valid order keys and reference-key membership.
- Recomputed daily totals: 8 / 4100.00 and 8 / 3700.00.
- Reconciled 23 = 20 + 2 + 1; completed orders = 16; total = 7800.00.
- Verified all three intentional failure flags default to False.

## Not executed here

- Databricks SQL parser/runtime or Spark execution.
- Unity Catalog object creation and permission enforcement.
- Volume filesystem calls and managed Delta writes.
- SQL warehouse query execution or actual workspace navigation.

No Databricks credentials or workspace session was used to validate this pack. These local checks do not substitute for a workspace integration test.

## Required before recording

1. Confirm your workspace edition is appropriate for the intended use.
2. Confirm the target catalog and storage path; select compatible compute.
3. Run Cells 01–11 and obtain PASS.
4. Confirm table type/provider in DESCRIBE TABLE EXTENDED.
5. Query Gold from the SQL editor if you plan to show that segment.
6. Run each failure and its repair, then reset its flag to False.
7. Re-run the success path to ensure stable business results.

The guide explicitly labels the external-table command as a conceptual template, not an executable prerequisite. Error messages and UI labels can differ across editions and runtime versions.
