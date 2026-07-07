# S3 data lake: raw (bronze) + conformed parquet zones in one versioned
# bucket. Versioning makes the raw zone effectively immutable.

resource "aws_s3_bucket" "lake" {
  bucket = var.bucket_name
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Role Snowflake assumes to read the lake — trust is pinned to Snowflake's
# exact IAM user plus the integration's external ID.
resource "aws_iam_role" "snowflake_s3_access" {
  name = "snowflake-s3-access"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { AWS = var.snowflake_iam_user_arn }
      Action    = "sts:AssumeRole"
      Condition = {
        StringEquals = { "sts:ExternalId" = var.snowflake_external_id }
      }
    }]
  })
}

# Read-only, single bucket — Snowflake never needs write access.
resource "aws_iam_role_policy" "bucket_read" {
  name = "airline-otp-bucket-read"
  role = aws_iam_role.snowflake_s3_access.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:GetObjectVersion"]
        Resource = "${aws_s3_bucket.lake.arn}/*"
      },
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = aws_s3_bucket.lake.arn
      }
    ]
  })
}
