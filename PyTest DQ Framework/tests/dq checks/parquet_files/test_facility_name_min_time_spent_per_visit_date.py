import pytest

TABLE_NAME = "facility_name_min_time_spent_per_visit_date"

TRANSFORM_SQL = """
SELECT
    f.facility_name,
    v.visit_timestamp::date AS visit_date,
    MIN(v.duration_minutes) AS min_time_spent
FROM
    visits v
JOIN facilities f 
    ON f.id = v.facility_id
GROUP BY
    f.facility_name,
    visit_date
UNION ALL
SELECT
    f.facility_name,
    v.visit_timestamp::date AS visit_date,
    MIN(v.duration_minutes) AS min_time_spent
FROM
    visits v
JOIN facilities f 
    ON f.id = v.facility_id
WHERE
    f.facility_type = 'Clinic' 
GROUP BY
    f.facility_name,
    visit_date;
"""

@pytest.mark.parquet_data
def test_dataset_is_not_empty(parquet_reader, data_quality_library):
    df = parquet_reader.read_table(TABLE_NAME)
    data_quality_library.check_dataset_is_not_empty(df)

@pytest.mark.parquet_data
def test_no_duplicates(parquet_reader, data_quality_library):
    df = parquet_reader.read_table(TABLE_NAME)
    data_quality_library.check_duplicates(df)

@pytest.mark.parquet_data
def test_key_columns_not_null(parquet_reader, data_quality_library):
    df = parquet_reader.read_table(TABLE_NAME)
    key_columns = [col for col in df.columns if "facility_name" in col or "visit_date" in col]
    if key_columns:
        data_quality_library.check_not_null_values(df, key_columns)

@pytest.mark.parquet_data
def test_count_matches_transformation(parquet_reader, db_connection, data_quality_library):
    df = parquet_reader.read_table(TABLE_NAME)
    source_df = db_connection.get_data_sql(TRANSFORM_SQL)
    data_quality_library.check_count(source_df, df)

@pytest.mark.parquet_data
def test_data_completeness_transformation(parquet_reader, db_connection, data_quality_library):
    df = parquet_reader.read_table(TABLE_NAME)
    source_df = db_connection.get_data_sql(TRANSFORM_SQL)
    key_columns = ["facility_name", "visit_date", "min_time_spent"]
    data_quality_library.check_data_completeness(source_df, df, key_columns)
