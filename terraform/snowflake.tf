# Snowflake estate: warehouses, database, roles, service user, storage
# integration, and the grants wiring them together. Mirrors
# scripts/snowflake_bootstrap.sql, which it supersedes.

resource "snowflake_warehouse" "load_wh" {
  name           = "LOAD_WH"
  warehouse_size = "XSMALL"
  auto_suspend   = 60
  auto_resume    = true
  comment        = "COPY INTO loads from S3"
}

resource "snowflake_warehouse" "transform_wh" {
  name           = "TRANSFORM_WH"
  warehouse_size = "XSMALL"
  auto_suspend   = 60
  auto_resume    = true
  comment        = "dbt transformations"
}

resource "snowflake_database" "airline_otp" {
  name = "AIRLINE_OTP"
}

resource "snowflake_schema" "raw" {
  database = snowflake_database.airline_otp.name
  name     = "RAW"
  comment  = "Landed flight records from the S3 lake, loaded via COPY INTO"
  # Must match the live schema exactly: this attribute is create-only, and
  # leaving it unset reads as a change -> forced replacement of a schema
  # holding 63M rows. Explicit beats implicit.
  is_transient = "false"
}

resource "snowflake_account_role" "loader" {
  name = "LOADER"
}

resource "snowflake_account_role" "transformer" {
  name = "TRANSFORMER"
}

resource "snowflake_service_user" "pipeline_svc" {
  name              = "PIPELINE_SVC"
  rsa_public_key    = var.pipeline_svc_public_key
  default_role      = snowflake_account_role.transformer.name
  default_warehouse = snowflake_warehouse.transform_wh.name
  default_namespace = "AIRLINE_OTP.RAW"
  comment           = "Pipeline service account: COPY loads + dbt runs"
}

# NOTE: deprecated in provider v2 in favor of
# snowflake_storage_integration_aws; kept for import compatibility, migrate
# on the next provider major version.
resource "snowflake_storage_integration" "s3_airline_otp" {
  name                      = "S3_AIRLINE_OTP"
  type                      = "EXTERNAL_STAGE"
  storage_provider          = "S3"
  enabled                   = true
  storage_aws_role_arn      = aws_iam_role.snowflake_s3_access.arn
  storage_allowed_locations = [
    "s3://${var.bucket_name}/raw/",
    "s3://${var.bucket_name}/lake/",
  ]
}

# --- role hierarchy -------------------------------------------------------

resource "snowflake_grant_account_role" "loader_to_sysadmin" {
  role_name        = snowflake_account_role.loader.name
  parent_role_name = "SYSADMIN"
}

resource "snowflake_grant_account_role" "transformer_to_sysadmin" {
  role_name        = snowflake_account_role.transformer.name
  parent_role_name = "SYSADMIN"
}

resource "snowflake_grant_account_role" "loader_to_pipeline_svc" {
  role_name = snowflake_account_role.loader.name
  user_name = snowflake_service_user.pipeline_svc.name
}

resource "snowflake_grant_account_role" "transformer_to_pipeline_svc" {
  role_name = snowflake_account_role.transformer.name
  user_name = snowflake_service_user.pipeline_svc.name
}

# --- loader privileges ----------------------------------------------------

resource "snowflake_grant_privileges_to_account_role" "loader_database" {
  account_role_name = snowflake_account_role.loader.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.airline_otp.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "loader_raw_schema" {
  account_role_name = snowflake_account_role.loader.name
  privileges        = ["USAGE", "CREATE TABLE", "CREATE STAGE", "CREATE FILE FORMAT"]
  on_schema {
    schema_name = "\"${snowflake_database.airline_otp.name}\".\"${snowflake_schema.raw.name}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "loader_warehouse" {
  account_role_name = snowflake_account_role.loader.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.load_wh.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "loader_integration" {
  account_role_name = snowflake_account_role.loader.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "INTEGRATION"
    object_name = snowflake_storage_integration.s3_airline_otp.name
  }
}

# --- transformer privileges -----------------------------------------------

resource "snowflake_grant_privileges_to_account_role" "transformer_database" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["USAGE", "CREATE SCHEMA"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.airline_otp.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_warehouse" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.transform_wh.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_raw_usage" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["USAGE"]
  on_schema {
    schema_name = "\"${snowflake_database.airline_otp.name}\".\"${snowflake_schema.raw.name}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_raw_select" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["SELECT"]
  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_schema          = "\"${snowflake_database.airline_otp.name}\".\"${snowflake_schema.raw.name}\""
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_raw_select_future" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["SELECT"]
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = "\"${snowflake_database.airline_otp.name}\".\"${snowflake_schema.raw.name}\""
    }
  }
}
