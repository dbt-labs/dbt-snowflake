## dbt-snowflake next
Resolves an issue caused when the Snowflake OCSP server is not accessible. This change only exposes the insecure_mode boolean avalable in the Snowflake python connector to dbt. This allows dbt to pass insecure_mode=true to the connector.

### Features
N/A

### Fixes
N/A

### Under the hood
- Add optional profile parameters for atypical local connection setups ([#21](https://github.com/dbt-labs/dbt-snowflake/issues/21), [#36](https://github.com/dbt-labs/dbt-snowflake/pull/36))

### Contributors
- [@laxjesse](https://github.com/laxjesse) ([#36](https://github.com/dbt-labs/dbt-snowflake/pull/36))

## dbt-snowflake v1.0.0b2 (October 25, 2021)

### Under the hood
- Replace `sample_profiles.yml` with `profile_template.yml`, for use with new `dbt init` ([#32](https://github.com/dbt-labs/dbt-snowflake/pull/32))

### Contributors
- [@NiallRees](https://github.com/NiallRees) ([#32](https://github.com/dbt-labs/dbt-snowflake/pull/32))

## dbt-snowflake v1.0.0b1 (October 11, 2021)

### Under the hood

- Initial adapter split out
