import pytest

from pathlib import Path

from dbt.tests.util import run_dbt, rm_file, write_file, check_relations_equal

from dbt.tests.adapter.simple_copy.test_simple_copy import (
    SimpleCopySetup,
    SimpleCopyBase,
    EmptyModelsArentRunBase,
)



class TestSimpleCopyBase(SimpleCopyBase):
    @pytest.mark.xfail
    def test_simple_copy_with_materialized_views(self, project):
        pass




class TestEmptyModelsArentRun(EmptyModelsArentRunBase):
    pass

