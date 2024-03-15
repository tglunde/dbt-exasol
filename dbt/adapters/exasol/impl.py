"""dbt-exasol Adapter implementation extending SQLAdapter"""
from __future__ import absolute_import

from typing import Dict, Optional, List, Set

from itertools import chain
import agate
from dbt.adapters.base.relation import BaseRelation, InformationSchema
from dbt.contracts.graph.manifest import Manifest
from dbt.adapters.base.impl import GET_CATALOG_MACRO_NAME, ConstraintSupport, GET_CATALOG_RELATIONS_MACRO_NAME, _expect_row_value
from dbt.adapters.capability import CapabilityDict, CapabilitySupport, Support, Capability
from dbt.adapters.sql import SQLAdapter
from dbt.exceptions import CompilationError
from dbt.utils import filter_null_values
from dbt.adapters.base.meta import available
from dbt.adapters.base.impl import ConstraintSupport
from dbt.contracts.graph.nodes import ConstraintType


from dbt.adapters.exasol import (ExasolColumn, ExasolConnectionManager,
                                 ExasolRelation)




class ExasolAdapter(SQLAdapter):
    """Exasol SQLAdapter extension"""

    Relation = ExasolRelation
    Column = ExasolColumn
    ConnectionManager = ExasolConnectionManager

    CONSTRAINT_SUPPORT = {
        ConstraintType.check: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.not_null: ConstraintSupport.ENFORCED,
        ConstraintType.unique: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.primary_key: ConstraintSupport.ENFORCED,
        ConstraintType.foreign_key: ConstraintSupport.ENFORCED,
    }

    _capabilities = CapabilityDict(
        {Capability.SchemaMetadataByRelations: CapabilitySupport(support=Support.Full)}
    )

    @classmethod
    def date_function(cls):
        return "current_timestamp()"

    @classmethod
    def is_cancelable(cls):
        return False

    @classmethod
    def convert_text_type(cls, agate_table, col_idx):
        return f"varchar({2000000})"

    def _make_match_kwargs(
        self, database: str, schema: str, identifier: str
    ) -> Dict[str, str]:
        quoting = self.config.quoting
        if identifier is not None and quoting["identifier"] is False:
            identifier = identifier.lower()

        if schema is not None and quoting["schema"] is False:
            schema = schema.lower()

        if database is not None and quoting["database"] is False:
            database = database.lower()

        return filter_null_values(
            {
                "identifier": identifier,
                "schema": schema,
            }
        )

    @classmethod
    def convert_number_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        return "float" if decimals else "integer"

    def timestamp_add_sql(
        self, add_to: str, number: int = 1, interval: str = "hour"
    ) -> str:
        """
        Overriding BaseAdapter default method because Exasol's syntax expects
        the number in quotes without the interval
        """
        return f"{add_to} + interval '{number}' {interval}"

    def quote_seed_column(self, column: str, quote_config: Optional[bool]) -> str:  # type: ignore
        quote_columns: bool = False
        if isinstance(quote_config, bool):
            quote_columns = quote_config
        elif quote_config is None:
            pass
        else:
            raise CompilationError(
                f'The seed configuration value of "quote_columns" has an '
                f"invalid type {type(quote_config)}"
            )

        if quote_columns:
            return self.quote(column)
        return column

    def valid_incremental_strategies(self):
        """The set of standard builtin strategies which this adapter supports out-of-the-box.
        Not used to validate custom strategies defined by end users.
        """
        return ["append", "merge", "delete+insert"]
    
    @staticmethod
    def is_valid_identifier(identifier) -> bool:
        # The first character should be alphabetic
        if not identifier[0].isalpha():
            return False
        # Rest of the characters is either alphanumeric or any one of the literals '#', '$', '_'
        idx = 1
        while idx < len(identifier):
            identifier_chr = identifier[idx]
            if not identifier_chr.isalnum() and identifier_chr not in ('#', '$', '_'):
                return False
            idx += 1
        return True
    
    @available
    def should_identifier_be_quoted(self,
                                    identifier,
                                    models_column_dict=None) -> bool:
        
        #Check if the naming is valid
        if not self.is_valid_identifier(identifier):
            return True
        #check if the column is set to be quoted in the model config
        elif models_column_dict and identifier in models_column_dict:
            return models_column_dict[identifier].get('quote', False)
        elif models_column_dict and self.quote(identifier) in models_column_dict:
            return models_column_dict[self.quote(identifier)].get('quote', False)
        return False
    
    @available
    def check_and_quote_identifier(self, identifier, models_column_dict=None) -> str:
        if self.should_identifier_be_quoted(identifier, models_column_dict):
            return self.quote(identifier)
        else:
            return identifier
        
    def _get_one_catalog(
        self,
        information_schema: InformationSchema,
        schemas: Set[str],
        manifest: Manifest,
    ) -> agate.Table:

        kwargs = {
            "information_schema": information_schema,
            "schemas": schemas
        }
        table = self.execute_macro(
            GET_CATALOG_MACRO_NAME,
            kwargs=kwargs,
            manifest=manifest,
        )
        # Use database from credentials if no other given
        for node in chain(manifest.nodes.values(), manifest.sources.values()):
            if not node.database or node.database == 'None':
                node.database = self.config.credentials.database

        results = self._catalog_filter_table(table, manifest)
        return results
        
    def _get_one_catalog_by_relations(
        self,
        information_schema: InformationSchema,
        relations: List[BaseRelation],
        manifest: Manifest,
    ) -> agate.Table:

        kwargs = {
            "information_schema": information_schema,
            "relations": relations,
        }
        table = self.execute_macro(
            GET_CATALOG_RELATIONS_MACRO_NAME,
            kwargs=kwargs,
            manifest=manifest,
        )

        # Use database from credentials if no other given
        for node in chain(manifest.nodes.values(), manifest.sources.values()):
            if not node.database or node.database == 'None':
                node.database = self.config.credentials.database

        results = self._catalog_filter_table(table, manifest)  # type: ignore[arg-type]
        return results

    def get_filtered_catalog(
        self, manifest: Manifest, relations: Optional[Set[BaseRelation]] = None
    ):
        catalogs: agate.Table
        if (
            relations is None
            or len(relations) > 100
            or not self.supports(Capability.SchemaMetadataByRelations)
        ):
            # Do it the traditional way. We get the full catalog.
            catalogs, exceptions = self.get_catalog(manifest)
        else:
            # Do it the new way. We try to save time by selecting information
            # only for the exact set of relations we are interested in.
            catalogs, exceptions = self.get_catalog_by_relations(manifest, relations)

        if relations and catalogs:
            relation_map = {
                (
                    r.schema.casefold() if r.schema else None,
                    r.identifier.casefold() if r.identifier else None,
                )
                for r in relations
            }

            def in_map(row: agate.Row):
                s = _expect_row_value("table_schema", row)
                i = _expect_row_value("table_name", row)
                s = s.casefold() if s is not None else None
                i = i.casefold() if i is not None else None
                return (s, i) in relation_map

            catalogs = catalogs.where(in_map)

        return catalogs, exceptions
