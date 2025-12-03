
import os
import pandas as pd
from io import StringIO

# ---------- SVG -> DataFrame ----------
def convert_svg_columns_to_dataframe(values, column_names=None):
    try:
        headers = ["Facility Type", "Visit Date", "Average Time Spent"]
        clean_values = [v for v in values if v not in headers]

        rows_count = len(clean_values) // 3
        col1 = clean_values[0:rows_count]
        col2 = clean_values[rows_count:rows_count * 2]
        col3 = clean_values[rows_count * 2:rows_count * 3]

        df = pd.DataFrame({
            column_names[0]: col1,
            column_names[1]: col2,
            column_names[2]: col3
        })

        return df
    except Exception as e:
        raise ValueError(f"Error converting SVG columns: {e}")


# ---------- Parquet (recursive, partition-aware) ----------
def read_parquet(folder_path: str, filter_date: str = None, date_column: str = "visit_date") -> pd.DataFrame:
    try:
        parquet_files = []
        for root, _, files in os.walk(folder_path):
            for f in files:
                if f.endswith(".parquet"):
                    parquet_files.append(os.path.join(root, f))

        if not parquet_files:
            raise FileNotFoundError(f"No Parquet files found in folder: {folder_path}")

        dfs = []
        for path in parquet_files:
            df = pd.read_parquet(path)

            partition_month = None
            parts = path.replace("\\", "/").split("/")
            for p in parts:
                if p.startswith("partition_date="):
                    partition_month = p.split("=", 1)[1]
                    break
            if partition_month and "partition_month" not in df.columns:
                df["partition_month"] = partition_month
            dfs.append(df)
        full_df = pd.concat(dfs, ignore_index=True)

        if filter_date:
            if len(filter_date) == 10:
                filter_month = filter_date[:7]
            elif len(filter_date) == 7:
                filter_month = filter_date
            else:
                raise ValueError(f"Unexpected FILTER_DATE format: {filter_date}")

            if date_column in full_df.columns:
                if pd.api.types.is_datetime64_any_dtype(full_df[date_column]):
                    df_datestr = full_df[date_column].dt.strftime("%Y-%m-%d")
                else:
                    df_datestr = full_df[date_column].astype(str).str.strip()
                if len(filter_date) == 10:
                    full_df = full_df[df_datestr == filter_date]
                else:
                    full_df = full_df[df_datestr.str.startswith(filter_month)]
            elif "partition_month" in full_df.columns:
                full_df = full_df[full_df["partition_month"] == filter_month]

        return full_df

    except Exception as e:
        raise ValueError(f"Error reading Parquet data: {e}")


# ---------- Normalization ----------
def normalize_dataframe(df: pd.DataFrame, date_column: str = "visit_date") -> pd.DataFrame:
    df = df.copy()

    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].astype(str).str.strip()

    if date_column in df.columns:
        ser = pd.to_datetime(df[date_column], errors="coerce")
        df[date_column] = ser.dt.strftime("%Y-%m-%d")

    for col in df.columns:
        if col != date_column:
            try:
                df[col] = pd.to_numeric(df[col])
            except Exception:
                pass

    cols_sorted = sorted(df.columns)
    df = df[cols_sorted]
    df = df.sort_values(by=cols_sorted).reset_index(drop=True)

    return df


# ---------- Filter by dates ----------
def filter_dataframe_by_dates(df, date_column, dates):
    try:
        return df[df[date_column].isin(dates)].reset_index(drop=True)
    except Exception as e:
        raise ValueError(f"Error filtering DataFrame by dates: {e}")


# ---------- Comparison by date ----------
def compare_dataframes_on_date(df1: pd.DataFrame, df2: pd.DataFrame, date_column="visit_date"):
    try:
        df1n = normalize_dataframe(df1, date_column=date_column)
        df2n = normalize_dataframe(df2, date_column=date_column)

        common_dates = set(df1n[date_column]) & set(df2n[date_column])
        df1f = df1n[df1n[date_column].isin(common_dates)]
        df2f = df2n[df2n[date_column].isin(common_dates)]

        if df1f.empty and df2f.empty:
            return True, None

        common_cols = sorted(list(set(df1f.columns) & set(df2f.columns)))
        df1a = df1f[common_cols]
        df2a = df2f[common_cols]

        if df1a.equals(df2a):
            return True, None

        left_only = (
            df1a.merge(df2a, on=common_cols, how="left", indicator=True)
                .query("_merge == 'left_only'")
                .drop(columns=["_merge"])
        )
        right_only = (
            df2a.merge(df1a, on=common_cols, how="left", indicator=True)
                .query("_merge == 'left_only'")
                .drop(columns=["_merge"])
        )

        differences = {
            "common_dates": sorted(list(common_dates)),
            "row_in_df1_not_in_df2": left_only.to_dict(orient="records"),
            "row_in_df2_not_in_df1": right_only.to_dict(orient="records"),
        }
        return False, differences

    except Exception as e:
        raise ValueError(f"Error comparing DataFrames on date: {e}")


# ---------- Inspect helpers ----------
def get_dataframe_columns(df):
    return list(df.columns)

def get_dataframe_shape(df):
    return [df.shape[0], df.shape[1]]

def head_as_dicts(df, n=5):
    return df.head(n).to_dict(orient="records")
