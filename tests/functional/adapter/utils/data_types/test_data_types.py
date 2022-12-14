import pytest, os
#from dbt.tests.adapter.utils.data_types.test_type_bigint import BaseTypeBigInt
from dbt.tests.adapter.utils.data_types.test_type_float import BaseTypeFloat
from dbt.tests.adapter.utils.data_types.test_type_int import BaseTypeInt
from dbt.tests.adapter.utils.data_types.test_type_numeric import BaseTypeNumeric
from dbt.tests.adapter.utils.data_types.test_type_string import BaseTypeString
from dbt.tests.adapter.utils.data_types.test_type_timestamp import BaseTypeTimestamp
from test_type_bigint import BaseTypeBigInt
#from dbt.tests.adapter.utils.data_types.test_type_boolean import BaseTypeBoolean


class TestTypeBigInt(BaseTypeBigInt):
    pass

    
class TestTypeFloat(BaseTypeFloat):
    pass

    
class TestTypeInt(BaseTypeInt):
    pass

    
class TestTypeNumeric(BaseTypeNumeric):
    pass

    
class TestTypeString(BaseTypeString):
    pass

    
class TestTypeTimestamp(BaseTypeTimestamp):
    @pytest.fixture(scope="class")
    def dbt_profile_target(self):
        return {
           "type": "exasol",
           "threads": 1,
           "dsn": os.getenv('DBT_DSN',"localhost:8563"),
           "user": os.getenv('DBT_USER',"sys"),
           "pass": os.getenv('DBT_PASS',"exasol"),
           "dbname": "DB",
           "timestamp_format": "YYYY-MM-DD HH:MI:SS.FF6"
        }


#class TestTypeBoolean(BaseTypeBoolean):
#    pass