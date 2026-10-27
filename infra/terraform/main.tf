terraform {
  required_version = ">= 1.5"
  required_providers {
    aws     = { source = "hashicorp/aws", version = "~> 5.0" }
    archive = { source = "hashicorp/archive", version = "~> 2.4" }
  }
}

provider "aws" {
  region = var.aws_region
}

# ---------------------------------------------------------------- storage
resource "aws_s3_bucket" "raw" {
  bucket = var.raw_bucket_name
}

resource "aws_s3_bucket_public_access_block" "raw" {
  bucket                  = aws_s3_bucket.raw.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id
  rule {
    id     = "expire-raw-after-30-days"
    status = "Enabled"
    filter {}
    expiration {
      days = 30
    }
  }
}

# ----------------------------------------------------------------- secret
resource "aws_ssm_parameter" "databricks_token" {
  name  = "/gh-lakehouse/databricks-token"
  type  = "SecureString"
  value = var.databricks_token
}

# ------------------------------------------------------------------- iam
data "aws_iam_policy_document" "assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ingest" {
  name               = "gh-lakehouse-ingest"
  assume_role_policy = data.aws_iam_policy_document.assume.json
}

data "aws_iam_policy_document" "ingest" {
  statement {
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.raw.arn}/*"]
  }
  statement {
    actions   = ["ssm:GetParameter"]
    resources = [aws_ssm_parameter.databricks_token.arn]
  }
}

resource "aws_iam_role_policy" "ingest" {
  role   = aws_iam_role.ingest.id
  policy = data.aws_iam_policy_document.ingest.json
}

resource "aws_iam_role_policy_attachment" "logs" {
  role       = aws_iam_role.ingest.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# ---------------------------------------------------------------- lambda
data "archive_file" "lambda" {
  type        = "zip"
  source_file = "${path.module}/../../ingestion/gharchive.py"
  output_path = "${path.module}/lambda.zip"
}

resource "aws_lambda_function" "ingest" {
  function_name    = "gh-lakehouse-ingest"
  role             = aws_iam_role.ingest.arn
  runtime          = "python3.12"
  handler          = "gharchive.handler"
  filename         = data.archive_file.lambda.output_path
  source_code_hash = data.archive_file.lambda.output_base64sha256
  timeout          = 600
  memory_size      = 1024

  ephemeral_storage {
    size = 1024 # one hourly file is a few hundred MB at most
  }

  environment {
    variables = {
      DATABRICKS_HOST        = var.databricks_host
      DATABRICKS_TOKEN_PARAM = aws_ssm_parameter.databricks_token.name
      VOLUME_DIR             = var.volume_dir
      S3_BUCKET              = aws_s3_bucket.raw.bucket
    }
  }
}

# -------------------------------------------------------------- schedule
resource "aws_cloudwatch_event_rule" "hourly" {
  name                = "gh-lakehouse-hourly"
  schedule_expression = "rate(1 hour)"
}

resource "aws_cloudwatch_event_target" "hourly" {
  rule = aws_cloudwatch_event_rule.hourly.name
  arn  = aws_lambda_function.ingest.arn
}

resource "aws_lambda_permission" "events" {
  statement_id  = "AllowEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingest.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.hourly.arn
}

output "lambda_name" {
  value = aws_lambda_function.ingest.function_name
}

output "raw_bucket" {
  value = aws_s3_bucket.raw.bucket
}
