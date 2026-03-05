# ETL for Budget Reconciliation

## Purpose

- **Extract** Budget Allocation data from `Google Spreadsheets`

- **Transform** raw spreadsheet records into `normalized analytical schema`

- **Load** transformed data into `Google BigQuery` using idempotent `UPSERT` strategy

---

## Extract

- The extractor retrieves Budget Allocation data from Google Spreadsheets using `service account credentials`

- The extractor uses google.auth.default() with scope `https://www.googleapis.com/auth/spreadsheets.readonly`

- The extractor opens spreadsheet using `open_by_key(spreadsheet_id)`

- The extractor accesses worksheet using `worksheet(worksheet_name)`

- The extractor extracts data using `get_all_records()`

- The extractor converts records into `pandas.DataFrame`

- The extractor classifies errors into retryable errors which are marked as `error.retryable = True`

- The extractor classifies errors into non-retryable errors which are marked as `error.retryable = False`

---

## Transform

- The transformer standardizes and enriches raw Budget Allocation records

- The transformer normalizes numeric columns by using `pd.to_numeric(errors="coerce")`

- The transformer normalizes numeric columns by filling with `0` if invalid

- The transformer normalizes numeric columns by rouding and converting to `nullable INT64`

- The transformer normalizes year from `YYYY-MM` month format by trimming white space

- The transformer normalizes date type by localizing to `Asia/Ho_Chi_Minh` timezone

- The transformer computes `actual_budget` from `initial_budget`, `adjusted_budget` and `additional_budget`

- The transformer computes various grouped budget columns based on `budget_group`

- The transformer calculates `total_effective_time` which is the difference between `start_date` and `end_date`

- The transformer calculates `total_passed_time` which is the difference between current date and `start_date`

---

## Load

- The loader uses `mode="upsert"` and `keys=["month"]` to delete existing records

- The loader applies table clustering on `cluster=["month"]` to improve query performance