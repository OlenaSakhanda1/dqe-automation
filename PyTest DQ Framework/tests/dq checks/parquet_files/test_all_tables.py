import pytest

@pytest.mark.parquet_data
@pytest.mark.parametrize("table_name", [
    "facilities",
    "patients",
    "src_generated_facilities",
    "src_generated_patients",
    "src_generated_visits",
    "visits"
])
def test_table_is_not_empty(parquet_reader, data_quality_library, table_name):
    df = parquet_reader.read_table(table_name)
    data_quality_library.check_dataset_is_not_empty(df)
    data_quality_library.check_duplicates(df)
    # Перевіримо, що ключові колонки не містять NULL (приклад)
    key_columns = [col for col in df.columns if "id" in col]  # або конкретні назви
    if key_columns:
        data_quality_library.check_not_null_values(df, key_columns)
