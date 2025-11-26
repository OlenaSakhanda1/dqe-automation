import pytest

TABLE_NAME = "facility_type_avg_time_spent_per_visit_date"

@pytest.mark.parquet_data
def test_dataset_is_not_empty(parquet_reader, data_quality_library):
    """Check that the dataset is not empty."""
    df = parquet_reader.read_table(TABLE_NAME)
    data_quality_library.check_dataset_is_not_empty(df)

@pytest.mark.parquet_data
def test_no_duplicates(parquet_reader, data_quality_library):
    """Check that there are no duplicate rows."""
    df = parquet_reader.read_table(TABLE_NAME)
    data_quality_library.check_duplicates(df)

@pytest.mark.parquet_data
def test_key_columns_not_null(parquet_reader, data_quality_library):
    """Check that key columns do not contain NULL values."""
    df = parquet_reader.read_table(TABLE_NAME)
    key_columns = [col for col in df.columns if "id" in col]
    if key_columns:
        data_quality_library.check_not_null_values(df, key_columns)

@pytest.mark.parquet_data
def test_row_count_matches_source(parquet_reader, db_connection, data_quality_library):
    """Check that the row count matches the source table."""
    df = parquet_reader.read_table(TABLE_NAME)
    source_df = db_connection.get_data_sql(f"SELECT * FROM {TABLE_NAME};")
    data_quality_library.check_count(source_df, df)

@pytest.mark.parquet_data
def test_data_completeness(parquet_reader, db_connection, data_quality_library):
    """Check that all key records from the source are present in the target dataset."""
    df = parquet_reader.read_table(TABLE_NAME)
    source_df = db_connection.get_data_sql(f"SELECT * FROM {TABLE_NAME};")
    key_columns = [col for col in df.columns if "id" in col]
    if key_columns:
        data_quality_library.check_data_completeness(source_df, df, key_columns)
