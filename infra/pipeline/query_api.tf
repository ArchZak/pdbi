variable "query_auth" {
  description = "Enable the frontend API with the Cognito issuer, app client audience and allowed browser origins."
  type = object({
    issuer   = string
    audience = string
    origins  = list(string)
  })
  default = null
}

module "query_api" {
  count      = var.query_auth == null ? 0 : 1
  source     = "./api"
  auth       = var.query_auth
  name       = var.name
  image      = var.image_uri
  bucket     = aws_s3_bucket.results.id
  bucket_arn = aws_s3_bucket.results.arn
  queues = {
    ingest = aws_sqs_queue.ingest.url
    csv    = aws_sqs_queue.exports.url
  }
  queue_arns = [aws_sqs_queue.ingest.arn, aws_sqs_queue.exports.arn]
  dead       = { ingest = aws_sqs_queue.ingest_dead.arn, csv = aws_sqs_queue.dead.arn }
}
output "query_api_url" { value = one(module.query_api[*].url) }
