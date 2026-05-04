import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import pandas as pd

from plugins.google_bigquery import internalGoogleBigqueryLoader

def load_budget_allocation(
    *,
    df: pd.DataFrame,
    direction: str,
) -> None:
    """
    Load Budget Allocation
    ---
    Principles:
        1. Validate input DataFrame
        2. Validate output direction for Google BigQuery
        3. Set primary key(s) to month
        4. Use UPSERT mode
        5. Make internalGoogleBigQueryLoader API call
    ---
    Returns:
        None
    """    

    if df.empty:
        
        print(
            "⚠️ [LOADER] Failed to load Budget Allocation due to no input DataFrame then loading will be suspended."
        )

        return

    print(
        "🔄 [LOADER] Loading Budget Allocation with "
        f"{len(df)} row(s) to Google BigQuery table "
        f"{direction}..."
    )

    loader = internalGoogleBigqueryLoader()

    loader.load(
        df=df,
        direction=direction,
        mode="upsert",
        keys=["month"],
        partition=None,
        cluster=[
            "month"
        ],
    )