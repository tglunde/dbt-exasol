import os
from pathlib import Path
from dotenv import load_dotenv
import pytest

pytest_plugins = ["dbt.tests.fixtures.project"]

dotenv_path = Path('test.env')
load_dotenv(dotenv_path=dotenv_path)

@pytest.fixture(scope="class")
def dbt_profile_target():
    return {
       "type": "exasol",
       "threads": 1,
       "dsn": os.getenv('DBT_DSN',"localhost:8563"),
       "user": os.getenv('DBT_USER',"sys"),
       "pass": os.getenv('DBT_PASS',"exasol"),
       "dbname": "DB",
   }

