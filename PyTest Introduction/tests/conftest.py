import pytest
import pandas as pd

# Fixture to read the CSV file
@pytest.fixture(scope="session")
def file_path():
    return "src/data/data.csv"

@pytest.fixture(scope="session")
def df():
    return pd.read_csv("src/data/data.csv")

# Fixture to validate the schema of the file
@pytest.fixture(scope="session")
def validate_schema(df):
    def _validate(actual_schema, expected_schema):
        return set(actual_schema) == set(expected_schema)


# Pytest hook to mark unmarked tests with a custom mark
def pytest_collection_modifyitems(items):
    for item in items:
        user_marks = [mark for mark in item.own_markers if mark.name not in ("skip", "xfail")]
        if not user_marks:
            item.add_marker(pytest.mark.unmarked)
