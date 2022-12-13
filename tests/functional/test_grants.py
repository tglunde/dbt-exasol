import pytest
from dbt.tests.adapter.grants.test_incremental_grants import BaseIncrementalGrants
from dbt.tests.adapter.grants.test_invalid_grants import BaseInvalidGrants
from dbt.tests.adapter.grants.test_model_grants import BaseModelGrants
from dbt.tests.adapter.grants.test_seed_grants import BaseSeedGrants
from dbt.tests.adapter.grants.test_snapshot_grants import BaseSnapshotGrants


class TestIncrementalGrantsExasol(BaseIncrementalGrants):
    pass


class TestInvalidGrantsExasol(BaseInvalidGrants):
    def grantee_does_not_exist_error(self):
        return "user or role 'INVALID_USER' does not exist"

    def privilege_does_not_exist_error(self):
        return "syntax error, unexpected ON_, expecting ',' or TO_ [line 3, column 34]"


class TestBaseModelGrantsExasol(BaseModelGrants):
    pass


class TestBaseSeedGrantsExasol(BaseSeedGrants):
    pass


class TestSnapshotGrantsExasol(BaseSnapshotGrants):
    pass
