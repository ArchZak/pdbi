resource "aws_sqs_queue" "ingest_dead" {
  name                      = "${var.name}-ingest-dead"
  message_retention_seconds = 1209600
}

resource "aws_sqs_queue" "ingest" {
  name                       = "${var.name}-ingest"
  visibility_timeout_seconds = 5400
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.ingest_dead.arn
    maxReceiveCount     = 5
  })
}

resource "aws_cloudwatch_log_group" "ingest" {
  name              = "/aws/lambda/${var.name}-ingest"
  retention_in_days = 14
}

resource "aws_iam_role" "ingest" {
  name               = "${var.name}-ingest"
  assume_role_policy = aws_iam_role.worker.assume_role_policy
}

resource "aws_iam_role_policy" "ingest" {
  role = aws_iam_role.ingest.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes", "sqs:SendMessage"], Resource = aws_sqs_queue.ingest.arn },
      { Effect = "Allow", Action = "s3:ListBucket", Resource = aws_s3_bucket.results.arn },
      { Effect = "Allow", Action = ["s3:GetObject", "s3:PutObject"], Resource = ["${aws_s3_bucket.results.arn}/runs/*/query.json", "${aws_s3_bucket.results.arn}/runs/*/parts/*", "${aws_s3_bucket.results.arn}/runs/*/result.parquet"] },
      { Effect = "Allow", Action = "s3:AbortMultipartUpload", Resource = "${aws_s3_bucket.results.arn}/runs/*/result.parquet" },
      { Effect = "Allow", Action = ["logs:CreateLogStream", "logs:PutLogEvents"], Resource = "${aws_cloudwatch_log_group.ingest.arn}:*" }
    ]
  })
}

resource "aws_lambda_function" "ingest" {
  function_name = "${var.name}-ingest"
  role          = aws_iam_role.ingest.arn
  package_type  = "Image"
  image_uri     = var.image_uri
  architectures = ["x86_64"]
  memory_size   = 2048
  timeout       = 900
  image_config {
    command = ["cloud_ingest.handler"]
  }
  ephemeral_storage {
    size = 10240
  }
  environment {
    variables = {
      RESULT_BUCKET    = aws_s3_bucket.results.id
      INGEST_QUEUE_URL = aws_sqs_queue.ingest.url
    }
  }
  depends_on = [aws_iam_role_policy.ingest]
}

# Each successful page deliberately schedules its successor on the same queue.
resource "aws_lambda_function_recursion_config" "ingest" {
  function_name  = aws_lambda_function.ingest.function_name
  recursive_loop = "Allow"
}

resource "aws_lambda_event_source_mapping" "ingest" {
  event_source_arn        = aws_sqs_queue.ingest.arn
  function_name           = aws_lambda_function.ingest.arn
  batch_size              = 1
  function_response_types = ["ReportBatchItemFailures"]
  depends_on              = [aws_lambda_function_recursion_config.ingest]
}

output "ingest_queue_url" { value = aws_sqs_queue.ingest.url }
