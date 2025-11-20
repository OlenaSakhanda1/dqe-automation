import pandas as pd


class DataQualityLibrary:
    """
    A library of static methods for performing data quality checks on pandas DataFrames.

    This class is intended to be used in a PyTest-based testing framework to validate
    the quality of data in DataFrames. Each method performs a specific data quality
    check and uses assertions to ensure that the data meets the expected conditions.
    """

   def check_dataset_is_not_empty(self, df: pd.DataFrame):
        assert not df.empty, "❌ Dataset is empty!"
        print(f"✅ Dataset contains {len(df)} rows.")

    def check_duplicates(self, df: pd.DataFrame):
        duplicates = df.duplicated().sum()
        assert duplicates == 0, f"❌ Found {duplicates} duplicate rows!"
        print("✅ No duplicate rows found.")

    def check_not_null_values(self, df: pd.DataFrame, columns: list):
        for col in columns:
            null_count = df[col].isnull().sum()
            assert null_count == 0, f"❌ Column '{col}' contains {null_count} NULL values!"
        print(f"✅ Columns {columns} have no NULL values.")

    def check_count(self, source_df: pd.DataFrame, target_df: pd.DataFrame):
        assert len(source_df) == len(target_df), (
            f"❌ Row count mismatch: source={len(source_df)}, target={len(target_df)}"
        )
        print("✅ Row counts match between source and target datasets.")

    def check_data_completeness(self, source_df: pd.DataFrame, target_df: pd.DataFrame, key_columns: list):
        missing = set(source_df[key_columns].dropna().apply(tuple, axis=1)) - \
                  set(target_df[key_columns].dropna().apply(tuple, axis=1))
        assert not missing, f"❌ Missing {len(missing)} key records in target dataset!"
        print("✅ All key records from source exist in target dataset.")
