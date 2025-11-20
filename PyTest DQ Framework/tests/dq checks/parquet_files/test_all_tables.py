import pytest

TABLES = [
    "facilities",
    "patients",
    "src_generated_facilities",
    "src_generated_patients",
    "src_generated_visits",
    "visits"
]

@pytest.mark.parquet_data
@pytest.mark.parametrize("table_name", TABLES)
def test_table_quality(parquet_reader, data_quality_library, db_connection, table_name):
    # Завантажуємо дані з Parquet
    target_df = parquet_reader.read_table(table_name)

    # 1. Перевірка: дата-сет не порожній
    data_quality_library.check_dataset_is_not_empty(target_df)

    # 2. Перевірка: немає дублікатів
    data_quality_library.check_duplicates(target_df)

    # 3. Перевірка: ключові колонки не містять NULL
    key_columns = [col for col in target_df.columns if "id" in col]
    if key_columns:
        data_quality_library.check_not_null_values(target_df, key_columns)

    # 4. Перевірка: кількість рядків збігається з джерелом
    source_query = f"SELECT * FROM {table_name};"
    source_df = db_connection.get_data_sql(source_query)
    data_quality_library.check_count(source_df, target_df)

    # 5. Перевірка: всі ключові записи з джерела присутні у цільовому наборі
    if key_columns:
        data_quality_library.check_data_completeness(source_df, target_df, key_columns)
