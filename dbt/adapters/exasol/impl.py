from __future__ import absolute_import

from dbt.adapters.sql import SQLAdapter
from dbt.adapters.exasol import ExasolConnectionManager
from dbt.adapters.exasol import ExasolRelation
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.utils import filter_null_values
import dbt.flags
class ExasolAdapter(SQLAdapter):
    Relation = ExasolRelation
    ConnectionManager = ExasolConnectionManager
    
    @classmethod
    def date_function(cls):
        return 'current_timestamp()'

    @classmethod
    def is_cancelable(cls):
        return False

    def _make_match_kwargs(self, database, schema, identifier):
        quoting = self.config.quoting
        if identifier is not None and quoting['identifier'] is False:
            identifier = identifier.upper()

        if schema is not None and quoting['schema'] is False:
            schema = schema.upper()

        if database is not None and quoting['database'] is False:
            database = database.upper()

        return filter_null_values({'identifier': identifier,
                                   'schema': schema,
                                   'database': database})
