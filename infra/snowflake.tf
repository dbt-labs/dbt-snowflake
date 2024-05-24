# Resources needed to run dbt-snowflake

resource "snowflake_database" "dbt_snowflake_db" {
  name                        = "DBT_SNOWFLAKE_DB"
  data_retention_time_in_days = 0
  comment                     = "Used by `dbt-snowflake` for CI"
}

resource "snowflake_warehouse" "dbt_snowflake_wh" {
  name           = "DBT_SNOWFLAKE_WH"
  warehouse_size = "XSMALL"
  auto_suspend   = 60
  comment        = "Used by `dbt-snowflake` for CI"
}

resource "snowflake_role" "dbt_snowflake_role" {
  provider = snowflake.security_admin
  name     = "DBT_SNOWFLAKE_ROLE"
  comment  = "Application role for `dbt_snowflake`"
}

resource "snowflake_grant_privileges_to_account_role" "dbt_snowflake_db" {
  provider          = snowflake.security_admin
  privileges        = ["USAGE", "MODIFY", "CREATE SCHEMA"]
  account_role_name = snowflake_role.dbt_snowflake_role.name

  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.dbt_snowflake_db.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "dbt_snowflake_wh" {
  provider          = snowflake.security_admin
  privileges        = ["USAGE"]
  account_role_name = snowflake_role.dbt_snowflake_role.name

  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.dbt_snowflake_wh.name
  }
}

resource "snowflake_user" "dbt_snowflake" {
  provider          = snowflake.security_admin
  name              = "DBT_SNOWFLAKE"
  display_name      = "dbt-snowflake"
  default_warehouse = snowflake_warehouse.dbt_snowflake_wh.name
  default_role      = snowflake_role.dbt_snowflake_role.name
  default_namespace = snowflake_database.dbt_snowflake_db.name
  comment           = "Application user for `dbt_snowflake`"
}

resource "snowflake_grant_account_role" "dbt_snowflake" {
  provider  = snowflake.security_admin
  role_name = snowflake_role.dbt_snowflake_role.name
  user_name = snowflake_user.dbt_snowflake.name
}

# Additional resources required for integration tests

resource "snowflake_database" "dbt_snowflake_db_alt" {
  name                        = "DBT_SNOWFLAKE_DB_ALT"
  data_retention_time_in_days = 0
  comment                     = "Used by `dbt-snowflake` for CI"
}

resource "snowflake_warehouse" "dbt_snowflake_wh_alt" {
  name           = "DBT_SNOWFLAKE_WH_ALT"
  warehouse_size = "XSMALL"
  auto_suspend   = 60
  comment        = "Used by `dbt-snowflake` for CI"
}
