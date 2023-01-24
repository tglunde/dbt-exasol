# dbt-exasol
**[dbt](https://www.getdbt.com/)** enables data analysts and engineers to transform their data using the same practices that software engineers use to build applications.

Please see the dbt documentation on **[Exasol setup](https://docs.getdbt.com/reference/warehouse-setups/exasol-setup)** for more information on how to start using the Exasol adapter.

# Current profile.yml settings
<File name='profiles.yml'>

```yaml
dbt-exasol:
  target: dev
  outputs:
    dev:
      type: exasol
      threads: 1
      dsn: HOST:PORT
      user: USERNAME
      password: PASSWORD
      dbname: db
      schema: SCHEMA
```

#### Optional parameters
<ul>
  <li><strong>connection_timeout</strong>: defaults to pyexasol default</li>
  <li><strong>socket_timeout</strong>: defaults to pyexasol default</li>
  <li><strong>query_timeout</strong>: defaults to pyexasol default</li>
  <li><strong>compression</strong>: default: False</li>
  <li><strong>encryption</strong>: default: False</li>
  <li><strong>protocol_version</strong>: default: v3</li>
  <li><strong>row_separator</strong>: default: CRLF for windows - LF otherwise</li>
  <li><strong>timestamp_format</strong>: default: YYYY-MM-DDTHH:MI:SS.FF6</li>
</ul>

# Known isues

## Breaking changes with release 1.2.2
- Timestamp format defaults to YYYY-MM-DDTHH:MI:SS.FF6

## SQL functions compatibility

### split_part
There is no equivalent SQL function in Exasol for split_part.

### listagg part_num
The SQL function listagg in Exasol does not support the num_part parameter.

## Utilities shim package
In order to support packages like dbt-utils and dbt-audit-helper, we needed to create the [shim package exasol-utils](https://github.com/tglunde/exasol-utils). In this shim package we need to adapt to parts of the SQL functionality that is not compatible with Exasol - e.g. when 'final' is being used which is a keyword in Exasol. Please visit [Adaopter dispatch documentation](https://docs.getdbt.com/guides/advanced/adapter-development/3-building-a-new-adapter#adapter-dispatch) of dbt-labs for more information. 
# Reporting bugs and contributing code
- Please report bugs using the issues

# Release History

## Release 1.2.2
- Added timestamp format parameter in profile.yml parameter file to set Exasol session parameter NLS_TIMESTAMP_FORMAT when opening a connection. Defaults to 'YYYY-MM-DDTHH:MI:SS.FF6' 
- Adding row_separator (LF/CRLF) parameter in profile.yml parameter file to be used in seed csv import. Defaults to operating system default (os.linesep in python).
- bugfix #36 regarding column quotes and case sensitivity of column names. 
- bugfix #42 regarding datatype change when using snapshot materialization. Added modify column statement in exasol__alter_column_type macro
- bugfix #17 number format datatype
- issue #24 - dbt-core v1.2.0 compatibility finished

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
