output "bucket_name" {
  value = aws_s3_bucket.lake.bucket
}

output "snowflake_role_arn" {
  value = aws_iam_role.snowflake_s3_access.arn
}

output "warehouses" {
  value = [snowflake_warehouse.load_wh.name, snowflake_warehouse.transform_wh.name]
}
