output "bronze_bucket_name" {
  description = "S3 Bronze bucket name — upload raw CSV here"
  value       = aws_s3_bucket.bronze.bucket
}

output "silver_bucket_name" {
  description = "S3 Silver bucket name — Glue Job 1 writes here"
  value       = aws_s3_bucket.silver.bucket
}

output "gold_bucket_name" {
  description = "S3 Gold bucket name — Glue Job 2 writes here, Redshift COPY reads from here"
  value       = aws_s3_bucket.gold.bucket
}

output "glue_role_arn" {
  description = "IAM Role ARN for Glue jobs — paste this in Glue Console"
  value       = aws_iam_role.glue_role.arn
}

output "redshift_role_arn" {
  description = "IAM Role ARN for Redshift COPY — paste into create_tables.sql"
  value       = aws_iam_role.redshift_role.arn
}

output "lambda_role_arn" {
  description = "IAM Role ARN for Lambda function"
  value       = aws_iam_role.lambda_role.arn
}
