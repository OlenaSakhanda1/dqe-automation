import pytest

TABLE_NAME = "patient_sum_treatment_cost_per_facility_type"

TRANSFORM_SQL = """
SELECT
    f.facility_type,
    CASE
        WHEN p.id <= 15 THEN 
            NULL
        ELSE
            CONCAT(p.first_name, ' ', p.last_name)
    END AS full_name,
    CASE 
        WHEN f.facility_type = 'Clinic' THEN 
            -SUM(v.treatment_cost)
        ELSE 
            SUM(v.treatment_cost)
    END AS sum_treatment_cost
FROM
    visits v
JOIN facilities f 
    ON f.id = v.facility_id
JOIN patients p
    ON p.id = v.patient_id
GROUP BY
    f.facility_type,
    full_name; 
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
    key_columns = [col for col in df.columns if "facility_type" in col or "full_name" in col]
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
    key_columns = ["facility_type", "full_name", "sum_treatment_cost"]
    data_quality_library.check_data_completeness(source_df, df, key_columns)
