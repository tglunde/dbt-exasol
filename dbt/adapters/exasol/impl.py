from __future__ import absolute_import

from dbt.adapters.sql import SQLAdapter
from dbt.adapters.exasol import ExasolConnectionManager
from dbt.adapters.exasol import ExasolRelation
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.utils import filter_null_values
import dbt.flags
from typing import Dict, List
import agate

LIST_RELATIONS_MACRO_NAME = 'list_relations_without_caching'


class ExasolAdapter(SQLAdapter):
    Relation = ExasolRelation
    ConnectionManager = ExasolConnectionManager

    @classmethod
    def date_function(cls):
        return 'current_timestamp()'

    @classmethod
    def is_cancelable(cls):
        return False

    @classmethod
    def convert_text_type(cls, agate_table, col_idx):
        column = agate_table.columns[col_idx]
        lens = (len(d.encode("utf-8")) for d in column.values_without_nulls())
        max_len = max(lens) if lens else 64
        return "varchar({})".format(max_len)

    def list_relations_without_caching(
        self, schema_relation: ExasolRelation,
    ) -> List[ExasolRelation]:
        kwargs = {'schema_relation': schema_relation}
        results = self.execute_macro(
            LIST_RELATIONS_MACRO_NAME,
            kwargs=kwargs
        )

        relations = []
        quote_policy = {
            'database': False,
            'schema': False,
            'identifier': False
        }
        for _database, name, _schema, _type in results:
            try:
                _type = self.Relation.get_relation_type(_type)
            except ValueError:
                _type = self.Relation.External
            relations.append(self.Relation.create(
                database=_database,
                schema=_schema,
                identifier=name,
                quote_policy=quote_policy,
                type=_type
            ))
        return relations

    def _make_match_kwargs(
        self, database: str, schema: str, identifier: str
    ) -> Dict[str, str]:
        quoting = self.config.quoting
        if identifier is not None and quoting['identifier'] is False:
            identifier = identifier.lower()

        if schema is not None and quoting['schema'] is False:
            schema = schema.lower()

        if database is not None and quoting['database'] is False:
            database = database.lower()

        return filter_null_values({
            'identifier': identifier,
            'schema': schema,
        })

    @classmethod
    def convert_number_type(
        cls, agate_table: agate.Table, col_idx: int
    ) -> str:
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        return "float" if decimals else "integer"
