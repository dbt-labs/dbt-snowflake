terraform {
  required_version = "1.8.3"

  required_providers {
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "0.91.0"
    }
  }
}

provider "snowflake" {
  alias = "security_admin"
  role  = "SECURITYADMIN"
  # SNOWFLAKE_ACCOUNT
  # SNOWFLAKE_USER
  # SNOWFLAKE_AUTHENTICATOR
  # SNOWFLAKE_PRIVATE_KEY
}
