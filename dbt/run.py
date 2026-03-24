import os
import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[0]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import subprocess

def dbt_budget_reconcile(
    *,
    google_cloud_project: str,
    select: str
):
    """
    Run dbt for Budget Reconciliation
    ---
    Principles:
        1. Initialize dbt execution environment
        2. Initialize Python subprocess to execute CLI
        3. Execute dbt build command with environment variables
        4. Execute dbt build command for dbt models stg/int/mart
        5. Capture dbt execution status with stdout and stderr
    ---
    Returns:
        None
    """

    cmd = [
        "dbt",
        "build",
        "--project-dir", ".",
        "--profiles-dir", ".",
        "--select", select,
    ]

    print(
        "🔄 [DBT] Executing dbt build for Budget Reconciliation "
        f"{select} insights to Google Cloud Project "
        f"{google_cloud_project}..."
    )

    try:
        
        result = subprocess.run(
            cmd,
            cwd="dbt",
            env=os.environ,
            check=True,
            capture_output=True,
            text=True,
        )

        print(result.stdout)

        if result.stderr:
            print(result.stderr)

        print(
            "✅ [DBT] Successfully executed dbt build for Budget Reconciliation "
            f"{select} selector to Google Cloud Project "
            f"{google_cloud_project}."
        )

    except subprocess.CalledProcessError as e:
        
        raise RuntimeError(
            "❌ [DBT] Failed to execute dbt build for Budget Reconciliation "
            f"{select} selector to Google Cloud Project "
            f"{google_cloud_project} due to "
            f"{e}."
        ) from e