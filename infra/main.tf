terraform {
  required_version = "1.8.3"

  required_providers {
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "0.91.0"
    }
  }
}
