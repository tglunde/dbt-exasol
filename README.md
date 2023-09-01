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

#### Optional login credentials using OpenID for Exasol SaaS
OpenID login through access_token or refresh_token instead of user+password

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

## Model contracts
The following database constraints are implemented for Exasol:

| Constraint Type  | Status |
| ------------- | ------------- |
| check | NOT supported  |
| not null  | enforced  |
| unique  | NOT supported  |
| primary key  | enforced  |
| foreign key  | enforced  |

## >=1.5 Incrmental model update
Fallback to dbt-core implementation and supporting strategies 
- append
- merge
- delete+insert

## >=1.3 Python model not yet supported - WIP
- Please follow [this pull request](https://github.com/tglunde/dbt-exasol/pull/59) 

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

# Releases

[GitHub Releases](https://github.com/tglunde/dbt-exasol/releases)
