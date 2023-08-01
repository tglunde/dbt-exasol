"""dbt-exasol Adapter implementation extending SQLAdapter"""
from __future__ import absolute_import

from typing import Dict, Optional

import agate
from dbt.adapters.base.meta import available
from dbt.adapters.sql import SQLAdapter
from dbt.exceptions import CompilationError
from dbt.utils import filter_null_values
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
        return ["append", "delete+insert"]
    
    # @available
    # @classmethod
    # def render_raw_columns_constraints(cls, raw_columns: Dict[str, Dict[str, Any]]) -> List:
    #     rendered_column_constraints = []

    #     for v in raw_columns.values():
    #         rendered_column_constraint = [f"{v['name']}"]
    #         for con in v.get("constraints", None):
    #             constraint = cls._parse_column_constraint(con)
    #             c = cls.process_parsed_constraint(constraint, cls.render_column_constraint)
    #             if c is not None:
    #                 rendered_column_constraint.append(c)
    #         rendered_column_constraints.append(" ".join(rendered_column_constraint))

    #     return rendered_column_constraints
    