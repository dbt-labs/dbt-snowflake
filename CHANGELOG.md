## dbt-snowflake 1.3.0b2 (Release TBD)

## dbt-snowflake 1.3.0b1 (July 29, 2022)

### Features
- support python model through create stored procedure with python in it, currently supported materializations are table and incremental. ([#182](https://github.com/dbt-labs/dbt-snowflake/pull/182))

### Under the Hood
- Reformat overridden macro location of grants work to a apply_grants.sql file in snowflake ([#193](https://github.com/dbt-labs/dbt-snowflake/issues/193), [#192](https://github.com/dbt-labs/dbt-snowflake/pull/192))
- Support dbt Core incremental materialization refactor ([#195](https://github.com/dbt-labs/dbt-snowflake/issues/195), [#196](https://github.com/dbt-labs/dbt-snowflake/pull/196))

## dbt-snowflake 1.2.0rc1 (July 12, 2022)

### Fixes
- In multi-query statements, prepend all queries with query comments. Use the last non-`COMMIT` query to store metadata about the model result. **Note:** this restores previous (pre-v0.21) behavior for incremental models and snapshots, which will again correctly reflect the number of rows modified in `adapter_response.rows_affected` ([#140](https://github.com/dbt-labs/dbt-snowflake/issues/140), [#147](https://github.com/dbt-labs/dbt-snowflake/issues147140), [#153](https://github.com/dbt-labs/dbt-snowflake/pull/153))
- Improve column comment handling when `persist_docs` is enabled ([#161](https://github.com/dbt-labs/dbt-snowflake/pull/161))

### Features
- Add grants to materializations ([#168](https://github.com/dbt-labs/dbt-snowflake/issues/168), [#178](https://github.com/dbt-labs/dbt-snowflake/pull/178))

### Contributors
- [@LewisDavies](https://github.com/LewisDavies) ([#161](https://github.com/dbt-labs/dbt-snowflake/pull/161))

## dbt-snowflake 1.2.0b1 (June 24, 2022)

### Features
- Add set_query_tag and unset_query_tag to the dbt macro namespace ([#133](https://github.com/dbt-labs/dbt-snowflake/issues/133), [#132](https://github.com/dbt-labs/dbt-snowflake/pull/132))

### Under the hood
- Add precommits for this repo ([#107](https://github.com/dbt-labs/dbt-snowflake/pull/107))
- Cleanup redundant precommit hook command ([#145](https://github.com/dbt-labs/dbt-snowflake/pull/145))
- File rename to match reference to core ([#152](https://github.com/dbt-labs/dbt-snowflake/pull/152))
- Bump cryptography restraint upper bound to <37.0.0 ([#171](https://github.com/dbt-labs/dbt-snowflake/pull/171))
- Migrate oauth refresh script from core ([#175](https://github.com/dbt-labs/dbt-snowflake/pull/175))
- Lift and shift cross-database macros from dbt-utils ([#162](https://github.com/dbt-labs/dbt-snowflake/pull/162)

### Contributors
- [@hhobson](https://github.com/hhobson) ([#171](https://github.com/dbt-labs/dbt-snowflake/pull/171))
- [@robscriva](https://github.com/robscriva) ([#132](https://github.com/dbt-labs/dbt-snowflake/pull/132))
- [@dbeatty10](https://github.com/dbeatty10) ([#162](https://github.com/dbt-labs/dbt-snowflake/pull/162)

## dbt-snowflake 1.1.0b1 (March 23, 2022)

### Features
- Adds tests for incremental model unique key parameter ([#91](https://github.com/dbt-labs/dbt-snowflake/issues/91))
- enables mfa token caching for linux when using the username_password_mfa authenticator ([#65](https://github.com/dbt-labs/dbt-snowflake/pull/65))

### Fixes
- Add unique\_id field to docs generation test catalogs; a follow-on PR to core PR ([#4168](https://github.com/dbt-labs/dbt-core/pull/4618))

### Under the hood
- Add `query_id` for a query to `run_result.json` ([#40](https://github.com/dbt-labs/dbt-snowflake/pull/40))
- Change logic for Post-failure job run ([#67](https://github.com/dbt-labs/dbt-snowflake/pull/67))
- Update to version bumping script ([#68](https://github.com/dbt-labs/dbt-snowflake/pull/68))
- Add contributing.md file for snowflake adapter repo ([#79](https://github.com/dbt-labs/dbt-snowflake/pull/79))
- Use dbt.tests.adapter.basic in test suite (new test framework) ([#105](https://github.com/dbt-labs/dbt-snowflake/issues/105), [#106](https://github.com/dbt-labs/dbt-snowflake/pull/106))

### Contributors
- [@joshuataylor](https://github.com/joshuataylor) ([#40](https://github.com/dbt-labs/dbt-snowflake/pull/40))
- [@devoted](https://github.com/devoted) ([#40](https://github.com/dbt-labs/dbt-snowflake/pull/40))

## dbt-snowflake 1.0.0 (December 3rd, 2021)

## dbt-snowflake 1.0.0rc2 (November 24, 2021)

### Fixes
- Apply query tags for Seed and Snapshot materialisations ([#20](https://github.com/dbt-labs/dbt-snowflake/issues/20), [#48](https://github.com/dbt-labs/dbt-snowflake/issues/48))
- Adds column-level comments to Snowflake views ([#17](https://github.com/dbt-labs/dbt-snowflake/issues/17))

### Under the hood
- Resolves an issue caused when the Snowflake OCSP server is not accessible, by exposing the `insecure_mode` boolean avalable in the Snowflake python connector ([#31](https://github.com/dbt-labs/dbt-snowflake/issues/31), [#49](https://github.com/dbt-labs/dbt-snowflake/pull/49))
- Fix test related to preventing coercion of boolean values (True, False) to numeric values (0, 1) in query results ([#76](https://github.com/dbt-labs/dbt-snowflake/issues/76))
- Add Stale messaging Github Action workflow ([#84](https://github.com/dbt-labs/dbt-snowflake/pull/84))


### Contributors
- [@anthu](https://github.com/anthu) ([#48](https://github.com/dbt-labs/dbt-snowflake/pull/48))
- [@JoshuaHuntley](https://github.com/JoshuaHuntley) ([#49](https://github.com/dbt-labs/dbt-snowflake/pull/49))
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
