from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod
from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import (
    BaseSingularTestsEphemeral,
)
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.adapter.basic.test_validate_connection import BaseValidateConnection


class TestSimpleMaterializationsExasol(BaseSimpleMaterializations):
    pass


class TestSingularTestsExasol(BaseSingularTests):
    pass


class TestSingularTestsEphemeralExasol(BaseSingularTestsEphemeral):
    pass


class TestEmptyExasol(BaseEmpty):
    pass


class TestEphemeralExasol(BaseEphemeral):
    pass


class TestIncrementalExasol(BaseIncremental):
    pass


class TestGenericTestsExasol(BaseGenericTests):
    pass


class TestSnapshotCheckColsExasol(BaseSnapshotCheckCols):
    pass


class TestSnapshotTimestampExasol(BaseSnapshotTimestamp):
    pass


class TestBaseAdapterMethod(BaseAdapterMethod):
    pass


class TestBaseCachingExasol(BaseAdapterMethod):
    pass


class TestValidateConnectionExasol(BaseValidateConnection):
    pass
