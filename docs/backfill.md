# Backfill for Budget Reconciliation

## Purpose

- Execute reconciliation DAGs for **historical data** outside of default runtime schedule

- Allow **manual override** of time range instead of relying on `main.py` predefined `MODE` mappings

- Require standard environment variables `COMPANY`, `PROJECT`, `DEPARTMENT`, `ACCOUNT` to be provided

- Primarily designed to run in **local environment** for controlled historical reprocessing

- Accept `--input_month` via argparse for flexible time-based backfill execution

## Execution

- Ensure Google Cloud Platform's Application Default Credentials already configured

- Run Backfill for specific month using CLI
```bash
$env:PROJECT="seer-digital-ads"
$env:COMPANY="kids"
$env:DEPARTMENT="marketing"
$env:ACCOUNT="main"
$env:MODE="thismonth"

python backfill.py --input_month 2025-01
```