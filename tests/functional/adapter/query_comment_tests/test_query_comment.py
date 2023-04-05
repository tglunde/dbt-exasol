from dbt.tests.adapter.query_comment.test_query_comment import (
    BaseEmptyQueryComments,
    BaseMacroArgsQueryComments,
    BaseMacroInvalidQueryComments,
    BaseMacroQueryComments,
    BaseNullQueryComments,
    BaseQueryComments,
)


class TestQueryCommentsExasol(BaseQueryComments):
    pass


class TestMacroQueryCommentsExasol(BaseMacroQueryComments):
    pass


class TestMacroArgsQueryCommentsExasol(BaseMacroArgsQueryComments):
    pass


class TestMacroInvalidQueryCommentsExasol(BaseMacroInvalidQueryComments):
    pass


class TestNullQueryCommentsExasol(BaseNullQueryComments):
    pass


class TestEmptyQueryCommentsExasol(BaseEmptyQueryComments):
    pass
