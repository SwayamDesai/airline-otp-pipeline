terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = ">= 1.0"
    }
  }
}
