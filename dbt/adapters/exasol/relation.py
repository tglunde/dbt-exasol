from dataclasses import dataclass
from dbt.adapters.base.relation import BaseRelation, Policy
from typing import Type, TypeVar, Optional
from hologram.helpers import StrEnum


class RelationType(StrEnum):
    Table = 'table'
    View = 'view'
    CTE = 'cte'
    MaterializedView = 'materializedview'
    External = 'external'

@dataclass
class ExasolQuotePolicy(Policy):
    database: bool = False
    schema: bool = False
    identifier: bool = False

Self = TypeVar('Self', bound='BaseRelation')


@dataclass(frozen=True, eq=False, repr=False)
class ExasolRelation(BaseRelation):
    quote_policy: ExasolQuotePolicy = ExasolQuotePolicy()
    
    @classmethod
    def create(
        cls: Type[Self],
        database: Optional[str] = None,
        schema: Optional[str] = None,
        identifier: Optional[str] = None,
        type: Optional[RelationType] = None,
        quote_policy: Type[ExasolQuotePolicy] = quote_policy,
        **kwargs,
    ) -> Self:
        kwargs.update({
            'path': {
                'schema': schema,
                'identifier': identifier,
            },
            'type': type,
        })
        return cls.from_dict(kwargs)

    @staticmethod
    def add_ephemeral_prefix(name: str):
        return f'dbt__CTE__{name}'