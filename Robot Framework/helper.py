import os
import pandas as pd
from robot.libraries.BuiltIn import BuiltIn

def extract_table_by_locator(locator: str) -> dict:
    try:
        sl = BuiltIn().get_library_instance("SeleniumLibrary")
        element = sl.find_element(locator)
        js = """
        const el = arguments[0];
        const fd = el && (el._fullData || el.data) || [];
        const tableTrace = fd.find(t => (t.type || (t._module && t._module.name)) === 'table');
        if (tableTrace && tableTrace.header && tableTrace.cells) {
            const header = Array.isArray(tableTrace.header.values) ? tableTrace.header.values : [];
            const cells  = Array.isArray(tableTrace.cells.values)  ? tableTrace.cells.values  : [];
            return { header, cells };
        }
        return {};
        """
        return sl.driver.execute_script(js, element) or {}
    except Exception as e:
        BuiltIn().log(f"Failed to extract table by locator '{locator}': {e}", level="WARN")
        return {}

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

def filter_dataframe_by_dates(df, date_column, dates):
    try:
        return df[df[date_column].isin(dates)].reset_index(drop=True)

    except Exception as e:
        raise ValueError(f"Error filtering DataFrame by dates: {e}")

def compare_dataframes_on_date(df1: pd.DataFrame, df2: pd.DataFrame, date_column1, date_column2):
    try:
        df1n = normalize_dataframe(df1, date_column=date_column1)
        df2n = normalize_dataframe(df2, date_column=date_column2)
        common_dates = set(df1n[date_column1]) & set(df2n[date_column2])
        df1f = df1n[df1n[date_column1].isin(common_dates)]
        df2f = df2n[df2n[date_column2].isin(common_dates)]
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

def get_dataframe_columns(df):
    return list(df.columns)

def get_dataframe_shape(df):
    return [df.shape[0], df.shape[1]]

def head_as_dicts(df, n=5):
    return df.head(n).to_dict(orient="records")

def table_data_to_dataframe(table: dict):
    if not table or "header" not in table or "cells" not in table:
        return pd.DataFrame()
    header = [str(x).strip() for x in table.get("header", [])]
    cells = table.get("cells", [])
    rows = list(zip(*cells)) if cells else []
    return pd.DataFrame(rows, columns=header)

from robot.api.deco import keyword

@keyword("Compare Plotly Table With Parquet")
def compare_plotly_table_with_parquet(table_data, parquet_folder, filter_date, html_date_col, parquet_date_col, target_dates):
    df_html = table_data_to_dataframe(table_data)
    df_html = normalize_dataframe(df_html, html_date_col)
    df_html = filter_dataframe_by_dates(df_html, html_date_col, target_dates)

    df_parquet = read_parquet(parquet_folder, filter_date, parquet_date_col)
    df_parquet = normalize_dataframe(df_parquet, parquet_date_col)
    df_parquet = filter_dataframe_by_dates(df_parquet, parquet_date_col, target_dates)

    return compare_dataframes_on_date(df_html, df_parquet, html_date_col, parquet_date_col)

from robot.api.deco import keyword

@keyword("Assert All Parquet In Table")
def assert_all_parquet_in_table(table_data, parquet_folder, filter_date, html_date_col, parquet_date_col, target_dates=None):
    df_html = table_data_to_dataframe(table_data)
    df_html = normalize_dataframe(df_html, html_date_col)
    if target_dates:
        df_html = filter_dataframe_by_dates(df_html, html_date_col, target_dates)

    df_parquet = read_parquet(parquet_folder, filter_date, parquet_date_col)
    df_parquet = normalize_dataframe(df_parquet, parquet_date_col)
    if target_dates:
        df_parquet = filter_dataframe_by_dates(df_parquet, parquet_date_col, target_dates)

    if html_date_col != parquet_date_col:
        df_html = df_html.rename(columns={html_date_col: parquet_date_col})

    common_cols = sorted(list(set(df_html.columns) & set(df_parquet.columns)))
    df_html = df_html[common_cols]
    df_parquet = df_parquet[common_cols]

    missing = df_parquet.merge(df_html, how="left", indicator=True).query('_merge == "left_only"').drop(columns=["_merge"])
    if missing.empty:
        return True, None
    else:
        return False, missing.to_dict(orient="records")