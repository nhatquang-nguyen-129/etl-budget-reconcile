# ETL for Budget Reconciliation

## Purpose

- **Extract** Budget Allocation data from `Google Spreadsheets`

- **Transform** raw spreadsheet records into `normalized analytical schema`

- **Load** transformed data into `Google BigQuery` using idempotent `UPSERT` strategy

## Extract

- The extractor retrieves Budget Allocation data from Google Spreadsheets using `service account credentials`

- The extractor uses google.auth.default() with scope `https://www.googleapis.com/auth/spreadsheets.readonly`

- The extractor opens spreadsheet using `open_by_key(spreadsheet_id)`

- The extractor accesses worksheet using `worksheet(worksheet_name)`

- The extractor extracts data using `get_all_records()`

- The extractor converts records into `pandas.DataFrame`

- The extractor classifies errors into retryable errors which are marked as `error.retryable = True`

- The extractor classifies errors into non-retryable errors which are marked as `error.retryable = False`

## Transform

- The transformer standardizes and enriches raw Budget Allocation records

- The transformer normalizes numeric columns by using `pd.to_numeric(errors="coerce")`

- The transformer normalizes numeric columns by filling with `0` if invalid

- The transformer normalizes numeric columns by rouding and converting to `nullable INT64`

- The transformer computes