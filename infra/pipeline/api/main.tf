variable "auth" {
  type = object({ issuer = string, audience = string, origins = list(string) })
}
variable "name" { type = string }
variable "image" { type = string }
variable "bucket" { type = string }
variable "bucket_arn" { type = string }
variable "queues" { type = map(string) }
variable "queue_arns" { type = list(string) }
variable "dead" { type = map(string) }

resource "aws_apigatewayv2_api" "query" {
  name          = "${var.name}-query"
  protocol_type = "HTTP"
  cors_configuration {
    allow_origins = var.auth.origins
    allow_methods = ["POST", "GET", "OPTIONS"]
    allow_headers = ["authorization", "content-type", "idempotency-key"]
  }
}

resource "aws_apigatewayv2_authorizer" "user" {
  api_id           = aws_apigatewayv2_api.query.id
  name             = "cognito"
  authorizer_type  = "JWT"
  identity_sources = ["$request.header.Authorization"]
  jwt_configuration {
    audience = [var.auth.audience]
    issuer   = var.auth.issuer
  }
}
resource "aws_cloudwatch_log_group" "api" {
  name              = "/aws/lambda/${var.name}-query-api"
  retention_in_days = 14
}
resource "aws_iam_role" "api" {
  name = "${var.name}-query-api"
  assume_role_policy = jsonencode({ Version = "2012-10-17", Statement = [{
    Effect = "Allow", Action = "sts:AssumeRole", Principal = { Service = "lambda.amazonaws.com" }
  }] })
}
resource "aws_iam_role_policy" "api" {
  role = aws_iam_role.api.id
  policy = jsonencode({ Version = "2012-10-17", Statement = [
    { Effect = "Allow", Action = ["logs:CreateLogStream", "logs:PutLogEvents"], Resource = "${aws_cloudwatch_log_group.api.arn}:*" },
    { Effect = "Allow", Action = "s3:ListBucket", Resource = var.bucket_arn },
    { Effect = "Allow", Action = ["s3:GetObject", "s3:PutObject"], Resource = [for key in ["query.json", "csv-request.json"] : "${var.bucket_arn}/runs/*/${key}"] },
    { Effect = "Allow", Action = "s3:GetObject", Resource = [for fmt in ["csv", "parquet"] : "${var.bucket_arn}/runs/*/result.${fmt}"] },
    { Effect = "Allow", Action = "sqs:SendMessage", Resource = var.queue_arns },
    { Effect = "Allow", Action = ["sqs:ReceiveMessage", "sqs:GetQueueAttributes", "sqs:DeleteMessage"], Resource = values(var.dead) }
  ] })
}
resource "aws_lambda_function" "api" {
  function_name = "${var.name}-query-api"
  package_type  = "Image"
  role          = aws_iam_role.api.arn
  image_uri     = var.image
  timeout       = 25
  memory_size   = 1024
  image_config { command = ["query_api.entrypoint"] }
  environment {
    variables = { RESULT_BUCKET = var.bucket, INGEST_QUEUE_URL = var.queues.ingest, CSV_QUEUE_URL = var.queues.csv, CSV_DLQ_ARN = var.dead.csv }
  }
  depends_on = [aws_iam_role_policy.api]
}
resource "aws_lambda_event_source_mapping" "dead" {
  for_each                = var.dead
  event_source_arn        = each.value
  function_name           = aws_lambda_function.api.arn
  batch_size              = 1
  function_response_types = ["ReportBatchItemFailures"]
}
resource "aws_lambda_permission" "gateway" {
  action        = "lambda:InvokeFunction"
  principal     = "apigateway.amazonaws.com"
  function_name = aws_lambda_function.api.function_name
  source_arn    = "${aws_apigatewayv2_api.query.execution_arn}/*/*/api/query*"
}
resource "aws_apigatewayv2_integration" "api" {
  api_id                 = aws_apigatewayv2_api.query.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.api.invoke_arn
  payload_format_version = "2.0"
}
resource "aws_apigatewayv2_route" "query" {
  for_each           = toset(["POST /api/query/validate", "POST /api/query", "GET /api/query/{run_id}", "POST /api/query/{run_id}/resume", "POST /api/query/{run_id}/download", "GET /api/query/{run_id}/download"])
  api_id             = aws_apigatewayv2_api.query.id
  route_key          = each.key
  target             = "integrations/${aws_apigatewayv2_integration.api.id}"
  authorization_type = "JWT"
  authorizer_id      = aws_apigatewayv2_authorizer.user.id
}
resource "aws_apigatewayv2_stage" "default" {
  api_id      = aws_apigatewayv2_api.query.id
  name        = "$default"
  auto_deploy = true
}
output "url" { value = aws_apigatewayv2_api.query.api_endpoint }

resource "aws_s3_bucket_cors_configuration" "download" {
  count  = length(var.auth.origins) > 0 ? 1 : 0
  bucket = var.bucket
  cors_rule {
    allowed_methods = ["GET", "HEAD"]
    allowed_origins = var.auth.origins
  }
}
