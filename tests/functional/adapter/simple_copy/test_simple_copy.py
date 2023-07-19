import pytest

from pathlib import Path

from dbt.tests.util import run_dbt, rm_file, write_file, check_relations_equal

from dbt.tests.adapter.simple_copy.test_simple_copy import (
    SimpleCopySetup,
    SimpleCopyBase,
    EmptyModelsArentRunBase,
)

from tests.functional.adapter.simple_copy.fixtures import (
    _MODELS__INCREMENTAL_OVERWRITE,
    _MODELS__INCREMENTAL_UPDATE_COLS,
    _SEEDS__SEED_MERGE_EXPECTED,
    _SEEDS__SEED_MERGE_INITIAL,
    _SEEDS__SEED_MERGE_UPDATE,
    _SEEDS__SEED_UPDATE,
    _TESTS__GET_RELATION_QUOTING,
)


class TestSimpleCopyBase(SimpleCopyBase):
    @pytest.fixture(scope="class")
    def tests(self):
        return {"get_relation_test.sql": _TESTS__GET_RELATION_QUOTING}

    @pytest.mark.xfail
    def test_simple_copy_with_materialized_views(self, project):
        pass




class TestEmptyModelsArentRun(EmptyModelsArentRunBase):
    pass

