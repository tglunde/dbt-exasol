from __future__ import absolute_import

from dbt.adapters.sql import SQLAdapter
from dbt.adapters.exasol import ExasolConnectionManager
from dbt.adapters.exasol import ExasolRelation
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.utils import filter_null_values
import dbt.flags

LIST_RELATIONS_MACRO_NAME = 'list_relations_without_caching'
GET_COLUMNS_IN_RELATION_MACRO_NAME = 'get_columns_in_relation'
LIST_SCHEMAS_MACRO_NAME = 'list_schemas'
CHECK_SCHEMA_EXISTS_MACRO_NAME = 'check_schema_exists'
CREATE_SCHEMA_MACRO_NAME = 'create_schema'
DROP_SCHEMA_MACRO_NAME = 'drop_schema'
RENAME_RELATION_MACRO_NAME = 'rename_relation'
TRUNCATE_RELATION_MACRO_NAME = 'truncate_relation'
DROP_RELATION_MACRO_NAME = 'drop_relation'
ALTER_COLUMN_TYPE_MACRO_NAME = 'alter_column_type'

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
        logger.error('matching kwargs %s', identifier )
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

    def list_relations_without_caching(self, information_schema, schema):
        logger.error(information_schema)
        kwargs = {'information_schema': information_schema, 'schema': schema}
        results = self.execute_macro(
            LIST_RELATIONS_MACRO_NAME,
            kwargs=kwargs
        )
        logger.error(results)
        relations = []
        quote_policy = {
            'database': True,
            'schema': True,
            'identifier': True
        }
        for _database, name, _schema, _type in results:
            logger.error('{} {} {} {} '.format(_database,name,_schema,_type))
            relations.append(self.Relation.create(
                database=_database,
                schema=_schema,
                identifier=name,
                quote_policy=quote_policy,
                type=_type
            ))
        return relations
