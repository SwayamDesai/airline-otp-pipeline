variable "aws_region" {
  type    = string
  default = "us-east-2"
}

variable "bucket_name" {
  type    = string
  default = "swayam-airline-otp"
}

variable "snowflake_organization" {
  type    = string
  default = "QRFICHI"
}

variable "snowflake_account" {
  type    = string
  default = "TQ35414"
}

variable "snowflake_private_key_path" {
  type    = string
  default = "~/.snowflake/keys/pipeline_svc_rsa.p8"
}

# Snowflake's cloud identity for the storage integration trust policy.
# These come from `desc integration s3_airline_otp` and change only if the
# integration is recreated.
variable "snowflake_iam_user_arn" {
  type    = string
  default = "arn:aws:iam::389656351827:user/luiy1000-s"
}

variable "snowflake_external_id" {
  type    = string
  default = "RJ97994_SFCRole=2_yyfM/gKqgS52PJ1SNHPzB+EPXOk="
}

# Public half of the pipeline service user's key pair (safe to commit).
variable "pipeline_svc_public_key" {
  type    = string
  default = "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAtAij3pn4xXR/v0uLspMkvbxCqAwpgmCmZkh0TWCoboCKSgGW/DOSbsb6zh9RLVk3J2Hq6yrj9tmoCBAQ2rzYSLe2H8fjzYg+9s+9k6NzUoZQP/Wp5s+TkPs5FHEFnEaZ39rQgfPu4Lj7tYXoNKl8G3Y1dByV/4CkqqGg+UHlgGyOC2cUdzMyEwqZJKc++RYJVC9wn7xcbkoIhlw1Y7R/Nycr51zhdqZWGwRdr9uCNNGbL18sKD3mrW34A4kjOI1PMNfPl0kZAB9BA2IewWWJVQpj8s9tkb9HlmiMc7LAmA/kIe8Vna0HAw2gN/Y52+Z5ki7D3zoY4njhnMv4vPfjJQIDAQAB"
}
