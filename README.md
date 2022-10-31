<p align="center">
  <a href="https://github.com/tglunde/dbt-exasol/actions/workflows/main.yml">
    <img src="https://github.com/tglunde/dbt-exasol/actions/workflows/main.yml/badge.svg?event=push" alt="Unit Tests Badge"/>
  </a>
  <a href="https://github.com/tglunde/dbt-exasol/actions/workflows/integration.yml">
    <img src="https://github.com/tglunde/dbt-exasol/actions/workflows/integration.yml/badge.svg?event=push" alt="Integration Tests Badge"/>
  </a>
</p>

# dbt-exasol
**[dbt](https://www.getdbt.com/)** enables data analysts and engineers to transform their data using the same practices that software engineers use to build applications.

Please see the dbt documentation on **[Exasol setup](https://docs.getdbt.com/reference/warehouse-setups/exasol-setup)** for more information on how to start using the Exasol adapter.

# Known isues
## Timestamp compatibility
Currently we did not see any possible way to make Exasol accept the timestamp format ```1981-05-20T06:46:51``` with a 'T' as separator between date and time part. To pass the adapter tests we had to change the timestamps in the seed files to have a <space> character instead of the 'T' (```1981-05-20 06:46:51```).
## Default case of identifiers
By default Exasol identifiers are upper case. In order to use e.g. seeds or column name aliases - you need to use upper case column names. Alternatively one could add column_quote feature in order to have all columns case sensitive.
We changed the seeds_base in [files.py](tests/functional/files.py) and [fixtures.py](tests/functional/fixtures.py) in order to reflect this and successfully run the adapter test cases.
Also, this issue leads to problems for the [base tests for docs generation](https://github.com/dbt-labs/dbt-core/blob/8145eed603266951ce35858f7eef3836012090bd/tests/adapter/dbt/tests/adapter/basic/test_docs_generate.py), since the expected model is being checked case sensitive and fails therefor for Exasol. This will be the last task in [Issue #24](https://github.com/tglunde/dbt-exasol/issues/24) regarding dbt release 1.2. We will suggest a case insensitive version of this standard test or implement for a following minor release of dbt-exasol.

## Utilities shim package
In order to support packages like dbt-utils and dbt-audit-helper, we needed to create the [shim package exasol-utils](https://github.com/tglunde/exasol-utils). In this shim package we need to adapt to parts of the SQL functionality that is not compatible with Exasol - e.g. when 'final' is being used which is a keyword in Exasol. Please visit [Adaopter dispatch documentation](https://docs.getdbt.com/guides/advanced/adapter-development/3-building-a-new-adapter#adapter-dispatch) of dbt-labs for more information. 
# Reporting bugs and contributing code
- Please report bugs using the issues

# Release
## Release 1.2.0
- support for invalidate_hard_deletes option in snapshots added by jups23
- added persist_docs support by sti0
- added additional configuration keys that can be included in profiles.yml by johannes-becker-otto
- added cross-database macros introduced in 1.2 by sti0
- added support for connection retries by sti0
- added support for grants by sti0
- added pytest functional adapter tests by tglunde
- tox testing for python 3.7.2 through 3.10 added by tglunde
 
## Release 1.0.0
- pyexasol HTTP import csv feature implemented. Optimal performance and compatibility with Exasol CSV parsing
