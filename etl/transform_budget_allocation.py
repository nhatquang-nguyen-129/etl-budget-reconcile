import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

import pandas as pd
import traceback

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
        1. pandas.DataFrame:
            Transformed Budget Allocation DataFrame
    """

    try:

        print(
            "🔄 [TRANSFORM] Validating column(s) for "
            f"{len(df)} row(s) of Budget Allocation..."
        )

        if df.empty:

            raise ValueError(
                "❌ [TRANSFORM] Failed to validate column(s) for Budget Allocation due to empty input DataFrame."
            )

        print(
            "🔍 [TRANSFORM][VALIDATE][STEP 3] DataFrame not empty"
        )

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
            "optimization",
            "initial_budget",
            "adjusted_budget",
            "additional_budget",
        }


        for idx, col in enumerate(df.columns):

            print(
                f"  [{idx}] {repr(col)}"
            )

        actual = {
            str(col).strip()
            for col in df.columns
        }

        missing = required_cols - actual

        extra = actual - required_cols

        print(
            "🔍 [TRANSFORM] Successfully validated DataFrame for Budget Allocation with "
            f"{df.shape} shape with total column(s) "
            f"{len(actual)}/{len(required_cols)} total column including "
            f"{len(missing)} mising column(s) and "
            f"{len(extra)} extra column(s)."
        )

        if missing:

            raise ValueError(
                f"❌ [TRANSFORM] Failed to transform transform validated DataFrame for Budget Allocation due to missing required column(s) "
                f"{sorted(missing)}"
            )

    except Exception as e:

        traceback.print_exc()

        raise RuntimeError(
            "❌ [TRANSFORM][VALIDATE] Failed to validate column(s) for Budget Allocation due to "
            f"{str(e)}."
        ) from e
            
# Normalize numeric columns
    for col in [
        "initial_budget",
        "adjusted_budget",
        "additional_budget",
    ]:

        print(
            "🔄 [TRANSFORM] Normalizing numeric column "
            f"{col} Budget Allocation..."
        )

        series = df[col]

        cleaned = (
            series.astype("string")
            .str.strip()
        )

        is_null = cleaned.isna() | (cleaned == "")

        has_comma = cleaned.str.contains(",", regex=False)
        
        has_dot = cleaned.str.contains(".", regex=False)

        is_double_format = (
            ~is_null &
            has_comma &
            has_dot
        )

        if is_double_format.any():

            samples = cleaned[is_double_format].head(5).tolist()

            raise ValueError(
                "❌ [TRANSFORM] Failed to transform numeric column "
                f"{col} with sample "
                f"{samples} due to this column contains double format values mixed thousand/decimal separators which may be valid float but INVALID for integer-only budget."
            )

        normalized = (
            cleaned
            .str.replace(",", "", regex=False)
            .str.replace(".", "", regex=False)
        )

        is_numeric = normalized.str.match(r"^-?\d+$")

        is_invalid_format = (
            ~is_null &
            ~is_numeric
        )

        if is_invalid_format.any():

            samples = cleaned[is_invalid_format].head(5).tolist()

            raise ValueError(
                "❌ [TRANSFORM] Failed to transform numeric column "
                f"{col} with sample "
                f"{samples} due to this column contains invalid numeric format."
            )

        numeric = pd.to_numeric(normalized, errors="raise")

        df[col] = numeric.fillna(0).astype("Int64")

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
    today = pd.Timestamp.now(tz="Asia/Ho_Chi_Minh").date()        
    
    df["month"] = df["month"].astype(str).str.strip()

    df["year"] = (
        pd.to_datetime(df["month"] + "-01", errors="coerce")
        .dt.year
        .fillna(0)
        .astype("Int64")
    )

    df["start_date"] = pd.to_datetime(
        df["start_date"], errors="coerce"
    ).dt.date

    df["end_date"] = pd.to_datetime(
        df["end_date"], errors="coerce"
    ).dt.date

    df["total_effective_time"] = (
        (df["end_date"] - df["start_date"])
        .apply(lambda x: x.days if pd.notnull(x) else 0)
        .astype("Int64")
    )

    df["total_passed_time"] = (
        (today - df["start_date"])
        .apply(lambda x: x.days if pd.notnull(x) else 0)
        .astype("Int64")
    )

    print(
        "✅ [TRANSFORM] Successfully transformed "
        f"{len(df)} row(s) of Budget Allocation."
    )

    return df