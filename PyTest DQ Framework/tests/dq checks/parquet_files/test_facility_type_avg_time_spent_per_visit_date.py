import pytest

TABLE_NAME = "facility_type_avg_time_spent_per_visit_date"

TRANSFORM_SQL = """
SELECT
    f.facility_type,
    v.visit_timestamp::date AS visit_date,
    ROUND(AVG(v.duration_minutes), 2) AS avg_time_spent
FROM
    visits v
JOIN
    facilities f 
    ON f.id = v.facility_id
WHERE
    v.visit_timestamp > '2000-11-01'
    AND f.facility_type IN ('Hospital', 'Clinic', 'Specialty Center')
GROUP BY
    f.facility_type,
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
    key_columns = [col for col in df.columns if "facility_type" in col or "visit_date" in col]
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
    key_columns = ["facility_type", "visit_date", "avg_time_spent"]
    data_quality_library.check_data_completeness(source_df, df, key_columns)
