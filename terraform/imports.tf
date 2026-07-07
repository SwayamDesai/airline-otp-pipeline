# Declarative adoption of the resources that were hand-built in Phases 1-2
# (bootstrap SQL + CLI). After the first successful apply, the estate is
# fully Terraform-managed and this file documents its origin.
# Grants are not importable one-to-one; applying them re-issues idempotent
# GRANT statements, which is a safe no-op.

import {
  to = aws_s3_bucket.lake
  id = "swayam-airline-otp"
}

import {
  to = aws_s3_bucket_versioning.lake
  id = "swayam-airline-otp"
}

import {
  to = aws_iam_role.snowflake_s3_access
  id = "snowflake-s3-access"
}

import {
  to = aws_iam_role_policy.bucket_read
  id = "snowflake-s3-access:airline-otp-bucket-read"
}

import {
  to = snowflake_warehouse.load_wh
  id = "\"LOAD_WH\""
}

import {
  to = snowflake_warehouse.transform_wh
  id = "\"TRANSFORM_WH\""
}

import {
  to = snowflake_database.airline_otp
  id = "\"AIRLINE_OTP\""
}

import {
  to = snowflake_schema.raw
  id = "\"AIRLINE_OTP\".\"RAW\""
}

import {
  to = snowflake_account_role.loader
  id = "\"LOADER\""
}

import {
  to = snowflake_account_role.transformer
  id = "\"TRANSFORMER\""
}

import {
  to = snowflake_service_user.pipeline_svc
  id = "\"PIPELINE_SVC\""
}

import {
  to = snowflake_storage_integration.s3_airline_otp
  id = "S3_AIRLINE_OTP"
}
