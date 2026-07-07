provider "aws" {
  region = var.aws_region
}

# Authenticates as TERRAFORM_SVC via key-pair (no passwords anywhere).
# Dev shortcut: the service user holds ACCOUNTADMIN; production would use
# scoped SYSADMIN/SECURITYADMIN roles per resource domain.
provider "snowflake" {
  organization_name = var.snowflake_organization
  account_name      = var.snowflake_account
  user              = "TERRAFORM_SVC"
  role              = "ACCOUNTADMIN"
  authenticator     = "SNOWFLAKE_JWT"
  private_key       = file(var.snowflake_private_key_path)

  # Legacy storage_integration resource (see snowflake.tf) is
  # preview-gated in provider v2.
  preview_features_enabled = ["snowflake_storage_integration_resource"]
}
