terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

variable "name" {
  type        = string
  description = "Environment-specific resource prefix, for example pdbi-dev."
}

variable "image_uri" {
  type        = string
  description = "Linux amd64 Lambda image in regional ECR, pinned by digest."
  validation {
    condition     = can(regex("@sha256:[0-9a-f]{64}$", var.image_uri))
    error_message = "Supply an immutable ECR image URI with its SHA-256 digest."
  }
}

resource "aws_s3_bucket" "results" {
  bucket_prefix = "${var.name}-results-"
}

resource "aws_s3_bucket_public_access_block" "results" {
  bucket                  = aws_s3_bucket.results.id
  block_public_acls       = true
  ignore_public_acls      = true
  block_public_policy     = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "results" {
  bucket = aws_s3_bucket.results.id
  rule {
    id     = "abort-abandoned-multipart-uploads"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

resource "aws_sqs_queue" "dead" {
  name                      = "${var.name}-csv-dead"
  message_retention_seconds = 1209600
}

resource "aws_sqs_queue" "exports" {
  name                       = "${var.name}-csv"
  visibility_timeout_seconds = 5400
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dead.arn
    maxReceiveCount     = 5
  })
}

resource "aws_iam_role" "worker" {
  name = "${var.name}-csv"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow", Principal = { Service = "lambda.amazonaws.com" }, Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_cloudwatch_log_group" "worker" {
  name              = "/aws/lambda/${var.name}-csv"
  retention_in_days = 14
}

resource "aws_iam_role_policy" "worker" {
  role = aws_iam_role.worker.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"], Resource = aws_sqs_queue.exports.arn },
      { Effect = "Allow", Action = "s3:ListBucket", Resource = aws_s3_bucket.results.arn },
      { Effect = "Allow", Action = "s3:GetObject", Resource = ["${aws_s3_bucket.results.arn}/runs/*/result.parquet", "${aws_s3_bucket.results.arn}/runs/*/result.csv"] },
      { Effect = "Allow", Action = ["s3:PutObject", "s3:AbortMultipartUpload"], Resource = "${aws_s3_bucket.results.arn}/runs/*/result.csv" },
      { Effect = "Allow", Action = ["logs:CreateLogStream", "logs:PutLogEvents"], Resource = "${aws_cloudwatch_log_group.worker.arn}:*" }
    ]
  })
}

resource "aws_lambda_function" "worker" {
  function_name = "${var.name}-csv"
  role          = aws_iam_role.worker.arn
  package_type  = "Image"
  image_uri     = var.image_uri
  architectures = ["x86_64"]
  memory_size   = 2048
  timeout       = 900
  ephemeral_storage {
    size = 10240
  }
  environment {
    variables = { RESULT_BUCKET = aws_s3_bucket.results.id }
  }
  depends_on = [aws_iam_role_policy.worker]
}

resource "aws_lambda_event_source_mapping" "exports" {
  event_source_arn        = aws_sqs_queue.exports.arn
  function_name           = aws_lambda_function.worker.arn
  batch_size              = 1
  function_response_types = ["ReportBatchItemFailures"]
}

output "result_bucket" { value = aws_s3_bucket.results.id }
output "csv_queue_url" { value = aws_sqs_queue.exports.url }
