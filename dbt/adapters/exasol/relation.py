"""dbt-exasol adapter relation module"""
from dataclasses import dataclass, field
from typing import Optional, Type, TypeVar

from dbt.adapters.base.relation import BaseRelation, Policy
from dbt.contracts.relation import RelationType


@dataclass
class ExasolQuotePolicy(Policy):
    """quote policy - not quotes"""

    database: bool = False
    schema: bool = False
    identifier: bool = False


Self = TypeVar("Self", bound="BaseRelation")


@dataclass(frozen=True, eq=False, repr=False)
class ExasolRelation(BaseRelation):
    """Relation implementation for exasol"""

    quote_policy: ExasolQuotePolicy = field(default_factory=lambda: ExasolQuotePolicy())

    @classmethod
    # pylint: disable=too-many-arguments
    # pylint: disable=redefined-builtin
    # pylint: disable=unused-argument
    def create(
        cls: Type[Self],
        database: Optional[str] = None,
        schema: Optional[str] = None,
        identifier: Optional[str] = None,
        type: Optional[RelationType] = None,
        quote_policy: Type[ExasolQuotePolicy] = quote_policy,
        **kwargs,
    ) -> Self:
        """updating kwargs to set schema and identifier
        Args:
            cls (Type[Self]): _description_
            database (Optional[str], optional): _description_. Defaults to None.
            schema (Optional[str], optional): _description_. Defaults to None.
            identifier (Optional[str], optional): _description_. Defaults to None.
            type (Optional[RelationType], optional): _description_. Defaults to None.

        Returns:
            Self: _description_
        """
        kwargs.update(
            {
                "path": {
                    "schema": schema,
                    "identifier": identifier,
                },
                "type": type,
            }
        )
        return cls.from_dict(kwargs)

    @staticmethod
    def add_ephemeral_prefix(name: str):
        return f"dbt__CTE__{name}"
