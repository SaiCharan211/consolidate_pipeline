# SCD2_02_Implementation_Clean — Cell-by-Cell Guide

## 1. Purpose of this notebook

This notebook develops and tests a Slowly Changing Dimension Type 2 (SCD Type 2, or SCD2) process using Databricks, PySpark, and Delta Lake.

SCD Type 2 preserves history when tracked attributes change. Instead of overwriting an old customer record, it closes the old version and inserts a new current version.

Example:

| customer_code | market | effective_start_date | effective_end_date | is_current |
|---|---|---|---|---|
| 789101 | USA | 2024-01-01 | 2026-10-21 | false |
| 789101 | India | 2026-10-21 | null | true |

The first row remains as history; the second row represents the current value.

## 2. Tables and safety settings

- **Source (read-only):** `fmcg.gold.sb_dim_customers`
- **Practice target:** `fmcg.gold.dim_customers_scd2_practice`
- **Production target (protected):** `fmcg.gold.dim_customers`
- **Backup (protected):** `fmcg.gold.dim_customers_backup`
- **Source key:** `customer_id`
- **Target key:** `customer_code`
- **Tracked attributes:** `customer`, `market`, `platform`, `channel`
- **Execution flag:** `EXECUTE_SCD2 = False` by default

The source also has `customer_name` and `city`, but the current comparison logic does not treat those as tracked SCD2 attributes. If they should create new versions, they must be added to the tracked-column list.

## 3. Cell-by-cell explanation

### Cell 1 — Notebook purpose and safety rules (Markdown)
**What it does:** Introduces the notebook, its objective, and safety constraints.

**Why we added it:** SCD2 code can change persistent history. The purpose and boundaries should be visible before anyone runs code.

**Writes data?** No.

### Cell 2 — Configuration and table safety checks
**What it does:** Defines source and target names, key columns, tracked columns, and the execution flag. Checks that required tables exist and that the practice table is selected while execution is disabled.

**Why we added it:** Centralized configuration reduces accidental use of the wrong table. Assertions fail early if the notebook is pointed at production or a required table is missing.

**Writes data?** No; configuration and checks only.

### Cell 3 — Schema inspection
**What it does:** Inspects source and target schemas and asserts that required columns are present.

**Why we added it:** SCD2 logic depends on exact key, tracked-attribute, and history columns. A renamed or missing column should be detected before comparisons or writes.

**Writes data?** No.

### Cell 4 — Read-only data-quality checks
**What it does:** Checks source and target structure, including null keys and duplicate keys in the source, and other basic structural conditions.

**Why we added it:** Duplicate source keys can make a MERGE ambiguous; null keys cannot reliably identify a customer. Early checks prevent avoidable errors and bad history.

**Writes data?** No.

### Cell 5 — Classify source records
**What it does:** Compares source records with the target and labels them by status, initially finding the source rows unchanged.

**Why we added it:** Before building a write plan, we need to know which records are new, changed, or unchanged. Unchanged rows should not create new versions.

**Writes data?** No.

### Cell 6 — Define `build_scd2_plan(source_table, target_table)`
**What it does:** Defines a reusable, read-only function that compares source and target data and classifies records, including `NEW`, `CHANGED`, `UNCHANGED`, `RECOVERY_REQUIRED`, and `INVALID_TARGET`. It also identifies target-only keys.

**Why we added it:** Putting comparison logic in one function avoids duplicating it across preview and validation cells. Explicit exception states help stop unsafe changes instead of silently guessing.

**Writes data?** Defining the function does not write data. It reads data when called.

### Cell 7 — Inspect target-only records
**What it does:** Reviews records that exist in the target but not in the current source snapshot. The inspection found 18 target-only keys.

**Why we added it:** A source that omits a customer does not necessarily mean that customer was deleted. The source might be filtered, partial, or delivered from a different scope. Automatically expiring target-only records could destroy valid current-state information.

**Writes data?** No. The 18 target-only records are preserved.

### Cell 8 — Write strategy and safety notes (Markdown)
**What it does:** Documents the planned write approach and its safeguards.

**Why we added it:** Separating the design explanation from executable code makes it easier to review the plan before any mutation.

**Writes data?** No.

### Cell 9 — Prepare write sets
**What it does:** Prepares the records that would need expiration or insertion based on the comparison plan. At the initial baseline, no rows required either action.

**Why we added it:** The comparison result is not itself a write operation. Preparing explicit write sets lets us inspect intended changes before applying them.

**Writes data?** No.

### Cell 10 — Define the execution safety gate
**What it does:** Defines checks that must pass before execution can proceed, including the intended target and the explicit execution setting.

**Why we added it:** A dry-run-first workflow reduces the risk of accidentally modifying a persistent table.

**Writes data?** Defining the gate does not write data.

### Cell 11 — Define execution preview
**What it does:** Defines a helper that displays the proposed changes before execution.

**Why we added it:** A reviewer should be able to see which keys would expire and which versions would be inserted before a MERGE is allowed.

**Writes data?** Defining the helper does not write data.

### Cell 12 — Run the execution preview
**What it does:** Runs the preview. At this point it showed 35 unchanged source records, zero expirations, zero insertions, and 18 target-only records retained.

**Why we added it:** This is a human-readable checkpoint between planning and writing.

**Writes data?** No.

### Cell 13 — Define pre-write state validation
**What it does:** Defines checks for the source and target state before any write, including current-version consistency and unresolved classifications.

**Why we added it:** A correct plan is not enough if the target already has broken history. The target should be in a known, valid state before a merge.

**Writes data?** Defining the function does not write data.

### Cell 14 — Run pre-write validation
**What it does:** Validates the current baseline: source had 35 rows, target had 57 rows, there were no multiple-current or zero-current problems, and no recovery/invalid records. The 18 target-only keys were preserved.

**Why we added it:** This confirms the baseline is safe enough for a controlled test and makes exceptions visible.

**Writes data?** No.

### Cell 15 — Failure recovery strategy (Markdown)
**What it does:** Documents the intended response to failure or an unexpected target state.

**Why we added it:** A data pipeline needs a recovery approach, not just a happy path. Operators should know when to stop and investigate rather than rerun blindly.

**Writes data?** No.

### Cell 16 — Implementation design (Markdown)
**What it does:** Explains the implementation approach, which ultimately uses a single Delta MERGE for expiration and insertion actions.

**Why we added it:** The design section records why the merge staging rows are shaped in a particular way and makes the implementation easier to reason about.

**Writes data?** No.

### Cell 17 — Define `expire_changed_versions()` placeholder
**What it does:** Defines a placeholder for the idea of closing the old current version when tracked values change.

**Why we added it:** It represented a possible step in a two-step design while the approach was being developed.

**Important:** This placeholder is not the actual expiration implementation used by the final test. The later guarded Delta MERGE is the operative write path.

**Writes data?** Defining this placeholder does not write data.

### Cell 18 — Define `insert_new_versions()` placeholder
**What it does:** Defines a placeholder for inserting new current versions for new or changed customers.

**Why we added it:** It represented the insert half of the earlier two-step design.

**Important:** This placeholder is not the actual insert implementation used by the final test. The later guarded Delta MERGE is the operative write path.

**Writes data?** Defining this placeholder does not write data.

### Cell 19 — Define `validate_scd2_target()`
**What it does:** Defines target validation checks for null keys, valid current flags and start dates, multiple or missing current versions, current rows with end dates, historical rows without end dates, and end dates earlier than start dates.

**Why we added it:** SCD2 history relies on invariants. For example, a customer should normally have exactly one current row, and a historical row should have an end date. Validation catches structural damage that simple row counts would miss.

**Writes data?** Defining the function does not write data.

### Cell 20 — Read-only baseline inspection
**What it does:** Checks that the target is Delta format, records the baseline row counts, and runs SCD2 validations. The baseline was 57 rows: 53 current and 4 historical; the checks passed.

**Why we added it:** Delta format supports the MERGE operation used later. Baseline counts make it possible to compare the state before and after the controlled test.

**Writes data?** No.

### Cell 21 — Build controlled test input
**What it does:** Creates a temporary test source named `scd2_controlled_test_source`. It changes the market for customer `789101` to `India_SCD2_TEST` and adds synthetic customer `SCD2_TEST_NEW_001`. The test input has 36 rows.

**Why we added it:** A controlled test should prove both SCD2 cases: (1) a changed customer gets a historical version plus a new current version, and (2) a new customer gets its first current version.

**Writes data?** It creates test data in the notebook's temporary/in-memory workflow; it does not write the target Delta table.

### Cell 22 — Build the test plan
**What it does:** Uses `build_scd2_plan` on the controlled test source and practice target. The plan classified 1 customer as `CHANGED`, 1 as `NEW`, and 34 as `UNCHANGED`; 18 target-only records remained preserved. The code uses the column name `scd2_action`.

**Why we added it:** The plan confirms that the test input produces exactly the cases we intended to test before creating merge staging rows.

**Writes data?** No target-table writes.

### Cell 23 — Prepare the write sets
**What it does:** Prepares one expiration for customer `789101` and two insert records: the new version of `789101` with market `India_SCD2_TEST`, and new customer `SCD2_TEST_NEW_001`.

**Why we added it:** The changed customer needs two actions: close the previous current row and insert a replacement current row. The new customer only needs an insert.

**Writes data?** No target-table writes.

### Cell 24 — Build staging rows for a single Delta MERGE
**What it does:** Builds a staging DataFrame containing an `EXPIRE` action for the existing key and `INSERT` actions for the changed customer's new version and the new customer. Insert rows use a null merge key so they do not match existing target rows.

**Why we added it:** The MERGE can process both expiration and insertion actions in one operation. A null merge key on insert actions is a deliberate pattern to make those rows take the not-matched insert path.

**Writes data?** No target-table writes. This cell initially used different timestamps for different actions.

### Cell 25 — Rebuild staging with one shared timestamp
**What it does:** Rebuilds the staging rows using one shared effective timestamp for the expiration and inserted versions. The recorded timestamp was `2026-10-21 11:07:41.097997`.

**Why we added it:** The previous version could produce slightly different times for the end of the old version and start of the new version. A shared timestamp makes the version boundary consistent for the same change.

**Writes data?** No target-table writes.

### Cell 26 — Define guarded `execute_scd2_merge()`
**What it does:** Defines the function that performs the actual Delta MERGE. It requires `execute=True`, restricts the target to the practice table, validates staging columns/actions, checks that expiration keys are unique and current in the target, and requires insert rows to have null merge keys. The MERGE expires matching current rows for `EXPIRE` actions and inserts new current rows for `INSERT` actions.

**Why we added it:** This is the central implementation, combining the planned actions into a single Delta transaction and placing explicit safeguards around the write.

**Important:** Defining the function does not run it. The function writes only when called with the execution flag enabled and when its guards pass.

**Writes data?** Function definition: no. Calling it with `execute=True`: yes.

### Cell 27 — Run read-only precondition checks
**What it does:** Confirms the practice target still has 57 rows, customer `789101` has exactly one current version, the synthetic new key is absent, and the staging contains one expiration and two insert actions.

**Why we added it:** This is the final stop/go checkpoint immediately before the actual write. It checks that the target has not changed unexpectedly since the plan was prepared.

**Writes data?** No.

### Cell 28 — Execute the controlled Delta MERGE
**What it does:** Calls `execute_scd2_merge(..., execute=True)` against `fmcg.gold.dim_customers_scd2_practice`. The MERGE succeeded and the target grew from 57 to 59 rows.

**Why we added it:** This proves the implementation can persist both the changed-customer history and a new customer's first version in the practice table.

**Observed result:** The practice target now has 59 rows. Customer `789101` has 3 historical versions and 1 current version. Synthetic customer `SCD2_TEST_NEW_001` has one current version. The post-merge validation passed.

**Writes data?** Yes — this is the cell that actually changed the practice Delta table.

**Do not rerun blindly.** The current implementation has not been made fully idempotent/retry-safe. Re-running the same test can create additional versions or fail checks. Review the target and reset/prepare a fresh controlled test before running again.

### Cell 29 — Final verification
**What it does:** Rechecks the SCD2 invariants and the test records after the merge. The final result was 59 total rows, 54 current rows, and 5 historical rows; customer `789101` had exactly one current row and three historical rows, and the synthetic customer existed once as current.

**Why we added it:** A successful MERGE message alone is not proof that the result is correct. Post-write validation verifies the expected row counts and history rules.

**Writes data?** No; this is read-only verification.

## 4. How the SCD2 process works in this notebook

1. Read the source and target.
2. Validate schemas and keys.
3. Compare tracked attributes to classify rows as new, changed, or unchanged.
4. Preserve target-only records because source absence is not assumed to mean deletion.
5. Prepare an explicit list of actions.
6. Validate the target and review the preview.
7. Build staging rows for expiration and insertion.
8. Run a guarded Delta MERGE against the practice table.
9. Validate the persisted result.

### What happens to each record type?

- **Unchanged:** No new version is inserted.
- **New:** Insert one current version with `is_current = true` and `effective_end_date = null`.
- **Changed:** Mark the old current version as historical (`is_current = false`, end date set), then insert the new current version.
- **Target-only:** Preserve it unless the business confirms the source is a complete snapshot and deletion/expiration is intended.
- **Invalid or recovery-required:** Stop and investigate rather than guessing.

## 5. Important terms

- **SCD Type 2:** A dimensional modelling technique that preserves historical versions of a record.
- **Current row:** The version representing the latest known values; normally `is_current = true` and `effective_end_date = null`.
- **Historical row:** A previous version; normally `is_current = false` and has an end date.
- **Tracked columns:** Attributes whose changes trigger a new version.
- **Staging DataFrame:** A prepared set of actions that tells the MERGE which current row to expire and which versions to insert.
- **Delta MERGE:** A Delta Lake operation that updates matching rows and inserts nonmatching rows in one transaction.
- **Dry run / preview:** Review the planned changes without changing the target.

## 6. Current test result and caveats

The controlled test passed. It tested one changed customer and one new synthetic customer.

Current practice-table state after the test:
- Table: `fmcg.gold.dim_customers_scd2_practice`
- Total rows: 59
- Current rows: 54
- Historical rows: 5
- Customer `789101`: one current row and three historical rows
- Synthetic customer `SCD2_TEST_NEW_001`: one current row

The practice table now contains test data: the current market for `789101` is `India_SCD2_TEST`, and the synthetic customer remains in the practice table. Keep this in mind when planning cleanup or further tests. Do not delete history casually.

## 7. Before production use

This notebook is a successful controlled practice implementation, **not yet a production-hardened pipeline**. Before switching the target to `fmcg.gold.dim_customers`:

- Add robust idempotency and retry/recovery handling so reruns cannot create duplicate versions.
- Test multiple changed customers, multiple source rows per key, null-to-value and value-to-null changes, empty source, duplicate keys, schema drift, and invalid target history.
- Decide how to handle source deletions and target-only records based on whether the source is a complete snapshot.
- Add run identifiers, audit logging, metrics, alerting, and a recovery procedure.
- Protect against concurrent runs and ensure source/target consistency.
- Test cleanup on a disposable practice table before removing synthetic data.
- Keep production and backup targets protected until the hardening and review are complete.

**Safety reminder:** Cells 1–27 and 29 are documentation, setup, validation, planning, staging, or function definitions/checks. Cell 28 is the cell that actually executed the test MERGE and modified the practice table. Do not point the notebook at production or rerun Cell 28 without a reviewed execution plan.
