provider "snowflake" {
  alias = "security_admin"
  role  = "SECURITYADMIN"
  # SNOWFLAKE_ACCOUNT
  # SNOWFLAKE_USER
  # SNOWFLAKE_AUTHENTICATOR
  # SNOWFLAKE_PRIVATE_KEY
}

# Resources needed to run dbt-snowflake

resource "snowflake_database" "database" {
  name                        = "DBT_SNOWFLAKE_DB"
  data_retention_time_in_days = 0
  comment                     = "Used by `dbt-snowflake` for CI"
}

resource "snowflake_warehouse" "warehouse" {
  name           = "DBT_SNOWFLAKE_WH"
  warehouse_size = "XSMALL"
  auto_suspend   = 60
  comment        = "Used by `dbt-snowflake` for CI"
}

resource "snowflake_role" "role" {
  provider = snowflake.security_admin
  name     = "DBT_SNOWFLAKE_ROLE"
  comment  = "Application role for `dbt_snowflake`"
}

resource "snowflake_grant_privileges_to_account_role" "database_grant" {
  provider          = snowflake.security_admin
  privileges        = ["USAGE", "MODIFY", "CREATE SCHEMA"]
  account_role_name = snowflake_role.role.name

  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.database.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "warehouse_grant" {
  provider          = snowflake.security_admin
  privileges        = ["USAGE"]
  account_role_name = snowflake_role.role.name

  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.warehouse.name
  }
}

resource "tls_private_key" "user" {
  algorithm = "RSA"
  rsa_bits  = 2048
}

resource "snowflake_user" "user" {
  provider          = snowflake.security_admin
  name              = "DBT_SNOWFLAKE"
  display_name      = "dbt-snowflake"
  rsa_public_key    = substr(tls_private_key.user.public_key_pem, 27, 398)
  default_warehouse = snowflake_warehouse.warehouse.name
  default_role      = snowflake_role.role.name
  default_namespace = snowflake_database.database.name
  comment           = "Application user for `dbt_snowflake`"
}

resource "snowflake_grant_account_role" "role_grant" {
  provider  = snowflake.security_admin
  role_name = snowflake_role.role.name
  user_name = snowflake_user.user.name
}

output "dbt_snowflake_user_public_key" {
  value = tls_private_key.user.public_key_pem
}

output "dbt_snowflake_user_private_key" {
  value     = tls_private_key.user.private_key_pem
  sensitive = true
}

# Additional resources required for integration tests

resource "snowflake_database" "database_quoted" {
  name                        = "DBT_SNOWFLAKE_DB_QUOTED"
  data_retention_time_in_days = 0
  comment                     = "Used by `dbt-snowflake` for CI"
}

resource "snowflake_database" "database_alt" {
  name                        = "DBT_SNOWFLAKE_DB_ALT"
  data_retention_time_in_days = 0
  comment                     = "Used by `dbt-snowflake` for CI"
}

resource "snowflake_warehouse" "warehouse_alt" {
  name           = "DBT_SNOWFLAKE_WH_ALT"
  warehouse_size = "XSMALL"
  auto_suspend   = 60
  comment        = "Used by `dbt-snowflake` for CI"
}
