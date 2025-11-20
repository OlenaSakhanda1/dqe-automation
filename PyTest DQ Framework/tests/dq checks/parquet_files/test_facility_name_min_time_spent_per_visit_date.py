"""
Description: Data Quality checks for migration from PostgreSQL to parquet
Requirement(s): TICKET-1234
Author(s): Olena Sakhanda
"""

import pytest

@pytest.mark.parquet_data
def test_check_dataset_is_not_empty(target_data, data_quality_library):
    data_quality_library.check_dataset_is_not_empty(target_data)
