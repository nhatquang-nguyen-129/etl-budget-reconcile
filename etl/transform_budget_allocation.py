import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import pandas as pd

from zoneinfo import ZoneInfo

def transform_budget_allocation(
    df: pd.DataFrame
) -> pd.DataFrame:
    """
    Transform Budget Allocation
    ---
    Principles:
        1. Validate input
        2. Enrich budget columns
        3. Normalize date columns
        4. Calculate time range columns
        5. Enforce schema
    ---
    Returns:
        1. DataFrame:
            Enforced budget allocation records
    """

    print(
        "🔄 [TRANSFORM] Transforming "
        f"{len(df)} row(s) of Budget Allocation..."
    )

    try:
        
    # Validate input
        if df.empty:
            
            print(
                "⚠️ [TRANSFORM] Empty Budget Allocation input then transformation will be suspended."
            )
            
            return df
        
        required_cols = {
            "budget_group",
            "category_level_1",
            "region",
            "details",
            "track",
            "pillar",
            "group",
            "month",
            "start_date",
            "end_date",
            "platform",
            "objective",
            "initial_budget",
            "adjusted_budget",
            "additional_budget",
        }

        missing = required_cols - set(df.columns)
        
        if missing:
        
            raise ValueError(
                "❌ [TRANSFORM] Failed to transform Budget Allocation due to missing columns "
                f"{missing} then transformation will be suspended."
            )
        
    # Safe cast numeric columns
        for col in [
            "initial_budget",
            "adjusted_budget",
            "additional_budget",
        ]:
            
            df[col] = (
                pd.to_numeric(df[col], errors="coerce")
                .fillna(0)
                .round(0)
                .astype("Int64")
            )

    # Transform derived columns
        df["actual_budget"] = (
            df["initial_budget"]
            + df["adjusted_budget"]
            + df["additional_budget"]
        ).astype("Int64")

        df["grouped_marketing_budget"] = (
            (df["budget_group"] == "KIDS").astype("Int64")
        ) * df["actual_budget"]

        df["grouped_supplier_budget"] = (
            (df["budget_group"] == "SUP").astype("Int64")
        ) * df["actual_budget"]

        df["grouped_store_budget"] = (
            (df["budget_group"] == "STORE").astype("Int64")
        ) * df["actual_budget"]

        df["grouped_ecommerce_budget"] = (
            (df["budget_group"] == "ECOM").astype("Int64")
        ) * df["actual_budget"]        

        df["grouped_recruitment_budget"] = (
            (df["budget_group"] == "HR").astype("Int64")
        ) * df["actual_budget"]

        df["grouped_customer_budget"] = (
            (df["budget_group"] == "CS").astype("Int64")
        ) * df["actual_budget"]

        df["grouped_festival_budget"] = (
            (df["budget_group"] == "FES").astype("Int64")
        ) * df["actual_budget"]

    # Transform time columns
        df["month"] = df["month"].astype(str).str.strip()

        df["year"] = (
            pd.to_datetime(
            df["month"] + "-01",
            errors="coerce"
            )
        .dt
        .year
        .fillna(0)
        .astype("Int64")
        )

        df["start_date"] = pd.to_datetime(
            df["start_date"], errors="coerce"
        ).dt.tz_localize(ZoneInfo("Asia/Ho_Chi_Minh"))

        df["end_date"] = pd.to_datetime(
            df["end_date"], errors="coerce"
        ).dt.tz_localize(ZoneInfo("Asia/Ho_Chi_Minh"))       

        df["total_effective_time"] = (
            (
                df["end_date"] - df["start_date"]
            )
            .dt.days
            .fillna(0)
            .astype("Int64")        
        )

        df["total_passed_time"] = (
            (
                pd.Timestamp.now(tz=ZoneInfo("Asia/Ho_Chi_Minh")).normalize()
                - df["start_date"]
            )
            .dt.days
            .fillna(0)
            .astype("Int64")
        )

        print(
            "✅ [TRANSFORM] Successfully transformed "
            f"{len(df)} row(s) of Budget Allocation."
        )

        return df

    except Exception as e:
        
        raise RuntimeError(
            "❌ [TRANSFORM] Failed to transform Budget Allocation due to "
            f"{e}."
        )