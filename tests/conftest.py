import os

import pytest

pytest_plugins = ["dbt.tests.fixtures.project"]


@pytest.fixture(scope="class")
def dbt_profile_target():
    return {
        "type": "exasol",
        "threads": 1,
        "dsn": os.getenv("DBT_DSN"),
        "user": os.getenv("DBT_USER"),
        "pass": os.getenv("DBT_PASS"),
        "dbname": "DB",
    }
