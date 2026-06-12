terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  required_version = ">= 1.5.0"
}

provider "aws" {
  region = var.aws_region
}

# ── S3 Buckets — Medallion Architecture ──────────────────────────────────────

resource "aws_s3_bucket" "bronze" {
  bucket = "${var.project_name}-bronze-${var.suffix}"
  tags = {
    Project     = var.project_name
    Layer       = "bronze"
    Environment = var.environment
    Dataset     = "COVID-19"
  }
}

resource "aws_s3_bucket" "silver" {
  bucket = "${var.project_name}-silver-${var.suffix}"
  tags = {
    Project     = var.project_name
    Layer       = "silver"
    Environment = var.environment
    Dataset     = "COVID-19"
  }
}

resource "aws_s3_bucket" "gold" {
  bucket = "${var.project_name}-gold-${var.suffix}"
  tags = {
    Project     = var.project_name
    Layer       = "gold"
    Environment = var.environment
    Dataset     = "COVID-19"
  }
}

# Enable versioning on Bronze to preserve raw data history
resource "aws_s3_bucket_versioning" "bronze_versioning" {
  bucket = aws_s3_bucket.bronze.id
  versioning_configuration {
    status = "Enabled"
  }
}

# ── IAM Role for AWS Glue ─────────────────────────────────────────────────────

resource "aws_iam_role" "glue_role" {
  name = "CovidGlueServiceRole"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
  tags = { Project = var.project_name }
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy_attachment" "glue_s3" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

resource "aws_iam_role_policy_attachment" "glue_cloudwatch" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/CloudWatchFullAccess"
}

# ── IAM Role for Amazon Redshift ──────────────────────────────────────────────

resource "aws_iam_role" "redshift_role" {
  name = "CovidRedshiftRole"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "redshift.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
  tags = { Project = var.project_name }
}

resource "aws_iam_role_policy_attachment" "redshift_s3" {
  role       = aws_iam_role.redshift_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
}

# ── IAM Role for Lambda ───────────────────────────────────────────────────────

resource "aws_iam_role" "lambda_role" {
  name = "CovidLambdaRole"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
  tags = { Project = var.project_name }
}

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy_attachment" "lambda_s3_read" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
}
