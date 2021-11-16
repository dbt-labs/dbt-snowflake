## dbt-snowflake 1.0.0 (Release TBD)

### Features
- Adds column-level comments to Snowflake views ([#17](https://github.com/dbt-labs/dbt-snowflake/issues/17))

### Contributors
- [@spencer-taylor-workrise](https://github.com/spencer-taylor-workrise) ([#17](https://github.com/dbt-labs/dbt-snowflake/issues/17))

## dbt-snowflake 1.0.0rc1 (November 10, 2021)

### Features
- Adds option to enable retries on errors encountered by the Snowflake connector ([#14](https://github.com/dbt-labs/dbt-snowflake/issues/14))

### Fixes
- When on_schema_change is set, pass common columns as dest_columns in incremental merge macros ([#4144](https://github.com/dbt-labs/dbt-core/issues/4144))

### Under the hood
- Add optional profile parameters for atypical local connection setups ([#21](https://github.com/dbt-labs/dbt-snowflake/issues/21), [#36](https://github.com/dbt-labs/dbt-snowflake/pull/36))
- Adds 4 optional profile parameters for configuring retries on Snowflake errors ([#14](https://github.com/dbt-labs/dbt-snowflake/issues/14), [#6](https://github.com/dbt-labs/dbt-snowflake/pull/6)) 
- Bump upper bound on `snowflake-connector-python` to `<2.8.0` ([#44](https://github.com/dbt-labs/dbt-snowflake/pull/44))
- Remove official support for python 3.6, which is reaching end of life on December 23, 2021 ([dbt-core#4134](https://github.com/dbt-labs/dbt-core/issues/4134), [#38](https://github.com/dbt-labs/dbt-snowflake/pull/45))
- Add support for structured logging [#42](https://github.com/dbt-labs/dbt-snowflake/pull/42)

### Contributors
- [@laxjesse](https://github.com/laxjesse) ([#36](https://github.com/dbt-labs/dbt-snowflake/pull/36))
- [@mhmcdonald](https://github.com/mhmcdonald) ([#6](https://github.com/dbt-labs/dbt-snowflake/pull/6))

## dbt-snowflake v1.0.0b2 (October 25, 2021)

### Under the hood
- Replace `sample_profiles.yml` with `profile_template.yml`, for use with new `dbt init` ([#32](https://github.com/dbt-labs/dbt-snowflake/pull/32))

### Contributors
- [@NiallRees](https://github.com/NiallRees) ([#32](https://github.com/dbt-labs/dbt-snowflake/pull/32))
- [@Kayrnt](https://github.com/Kayrnt) ([#38](https://github.com/dbt-labs/dbt-snowflake/pull/38))

## dbt-snowflake v1.0.0b1 (October 11, 2021)

### Under the hood

- Initial adapter split out
