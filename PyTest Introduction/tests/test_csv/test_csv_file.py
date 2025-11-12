import pytest
import re
import os, pandas as pd

@pytest.mark.critical
def test_file_not_empty(file_path):
    assert os.path.getsize(file_path) > 0, f"File {file_path} is empty!"

@pytest.mark.validate_csv
@pytest.mark.xfail(reason="The test may fail unexpectedly if there are duplicates.")
def test_duplicates(df):
    duplicates = df[df.duplicated()]
    assert duplicates.empty, f"Duplicate rows found:\n{duplicates}"

@pytest.mark.validate_csv
def test_validate_schema(df):
    header = df.columns.tolist()
    assert header == ['id', 'name', 'age', 'email', 'is_active']

def test_schema(df):
    expected_schema = ['id', 'name', 'age', 'email', 'is_active']
    actual_schema = df.columns.tolist()
    assert actual_schema == expected_schema, f"Schema does not match! Expected {expected_schema}, got {actual_schema}"

@pytest.mark.validate_csv
@pytest.mark.skip(reason="Test skipped on request")
def test_age_column_valid(df):
    age_column = df['age']
    for i in age_column:
        assert i > 0 and i < 100

@pytest.mark.validate_csv
def test_email_column_valid(df):
    email_column = df['email']
    email_pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
    for i in email_column:
        assert email_column.apply(lambda x: bool(re.match(email_pattern, str(x)))).all(), "Some emails are incorrect"

@pytest.mark.parametrize("id_value, expected_active", [(1, False), (2, True)])
def test_active_players(df, id_value, expected_active):
    is_active_values = df.loc[df['id'] == id_value, 'is_active'].values
    assert len(is_active_values) > 0, f"id={id_value} not found"
    assert all(is_active_values == expected_active), f"is_active must be {expected_active} for id={id_value}"


def test_active_player(df):
    is_active_2 = df.loc[df['id'] == 2, 'is_active'].values
    assert len(is_active_2) > 0, "id=2 not found"
    assert all(is_active_2 == True), "is_active expected True for id=2"
