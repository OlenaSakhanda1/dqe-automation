import pytest

TABLE_NAME = "facility_type_avg_time_spent_per_visit_date"

@pytest.mark.parquet_data
def test_dataset_is_not_empty(parquet_reader):
    """Check that the dataset is not empty."""
    df = parquet_reader.read_table(TABLE_NAME)
    assert not df.empty, "Dataset is empty"

@pytest.mark.parquet_data
def test_no_duplicates(parquet_reader):
    """Check that there are no duplicate rows."""
    df = parquet_reader.read_table(TABLE_NAME)
    assert df.duplicated().sum() == 0, "Dataset contains duplicate rows"

@pytest.mark.parquet_data
def test_key_columns_not_null(parquet_reader):
    """Check that key columns do not contain NULL values."""
    df = parquet_reader.read_table(TABLE_NAME)
    key_columns = [col for col in df.columns if "id" in col]
    for col in key_columns:
        assert df[col].isnull().sum() == 0, f"Key column '{col}' contains NULL values"

@pytest.mark.parquet_data
def test_row_count_matches_source(parquet_reader, db_connection):
    """Check that the row count matches the source table."""
    df = parquet_reader.read_table(TABLE_NAME)
    source_df = db_connection.get_data_sql(f"SELECT * FROM {TABLE_NAME};")
    assert len(df) == len(source_df), "Row count does not match source table"

@pytest.mark.parquet_data
def test_data_completeness(parquet_reader, db_connection):
    """Check that all key records from the source are present in the target dataset."""
    df = parquet_reader.read_table(TABLE_NAME)
    source_df = db_connection.get_data_sql(f"SELECT * FROM {TABLE_NAME};")
    key_columns = [col for col in df.columns if "id" in col]
    if key_columns:
        merged = source_df.merge(df, on=key_columns, how='left', indicator=True)
        missing = merged[merged['_merge'] == 'left_only']
        assert missing.empty, f"Missing key records: {missing[key_columns].to_dict(orient='records')}"
