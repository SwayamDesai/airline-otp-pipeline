-- Snowflake bootstrap — run once as ACCOUNTADMIN in a Snowsight worksheet.
-- Creates warehouses, database, roles, the key-pair-authenticated service
-- user, and the S3 storage integration.
-- Phase 5 replaces this with Terraform; kept here as the documented origin.

use role accountadmin;

-- Separate warehouses so loading and transforming are metered independently.
-- XSMALL + 60s auto-suspend: costs nothing while idle.
create warehouse if not exists load_wh
  warehouse_size = xsmall auto_suspend = 60 auto_resume = true
  initially_suspended = true
  comment = 'COPY INTO loads from S3';

create warehouse if not exists transform_wh
  warehouse_size = xsmall auto_suspend = 60 auto_resume = true
  initially_suspended = true
  comment = 'dbt transformations';

create database if not exists airline_otp;
create schema if not exists airline_otp.raw
  comment = 'Landed flight records from the S3 lake, loaded via COPY INTO';

-- Functional roles: LOADER writes raw, TRANSFORMER builds models on top.
create role if not exists loader;
create role if not exists transformer;
grant role loader to role sysadmin;
grant role transformer to role sysadmin;

grant usage on database airline_otp to role loader;
grant usage, create table, create stage, create file format
  on schema airline_otp.raw to role loader;
grant usage on warehouse load_wh to role loader;

grant usage, create schema on database airline_otp to role transformer;
grant usage on warehouse transform_wh to role transformer;
grant usage on schema airline_otp.raw to role transformer;
grant select on all tables in schema airline_otp.raw to role transformer;
grant select on future tables in schema airline_otp.raw to role transformer;

-- Service user: key-pair auth only, no password. TYPE=SERVICE exempts it
-- from human MFA policies (it is not a person).
create user if not exists pipeline_svc
  type = service
  rsa_public_key = 'MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAtAij3pn4xXR/v0uLspMkvbxCqAwpgmCmZkh0TWCoboCKSgGW/DOSbsb6zh9RLVk3J2Hq6yrj9tmoCBAQ2rzYSLe2H8fjzYg+9s+9k6NzUoZQP/Wp5s+TkPs5FHEFnEaZ39rQgfPu4Lj7tYXoNKl8G3Y1dByV/4CkqqGg+UHlgGyOC2cUdzMyEwqZJKc++RYJVC9wn7xcbkoIhlw1Y7R/Nycr51zhdqZWGwRdr9uCNNGbL18sKD3mrW34A4kjOI1PMNfPl0kZAB9BA2IewWWJVQpj8s9tkb9HlmiMc7LAmA/kIe8Vna0HAw2gN/Y52+Z5ki7D3zoY4njhnMv4vPfjJQIDAQAB'
  default_role = transformer
  default_warehouse = transform_wh
  default_namespace = airline_otp.raw
  comment = 'Pipeline service account: COPY loads + dbt runs';

grant role loader to user pipeline_svc;
grant role transformer to user pipeline_svc;

-- Storage integration: Snowflake assumes this IAM role to read the bucket.
create storage integration if not exists s3_airline_otp
  type = external_stage
  storage_provider = 's3'
  enabled = true
  storage_aws_role_arn = 'arn:aws:iam::481154549214:role/snowflake-s3-access'
  storage_allowed_locations = ('s3://swayam-airline-otp/raw/', 's3://swayam-airline-otp/lake/');

grant usage on integration s3_airline_otp to role loader;

-- Copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID from this
-- output — they wire the AWS trust policy.
desc integration s3_airline_otp;
