# -----------------------------------------------------------------------------
# Secrets Manager: OAuth client credentials read by the Lambda ESM
# Format: https://docs.aws.amazon.com/lambda/latest/dg/kafka-cluster-auth.html#smaa-auth-oauth-secret
# -----------------------------------------------------------------------------

resource "aws_secretsmanager_secret" "oauth" {
  name_prefix             = "${var.name_prefix}-entra-oauth-"
  description             = "Entra ID client credentials used by the Lambda Kafka ESM for Confluent Cloud"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "oauth" {
  secret_id = aws_secretsmanager_secret.oauth.id
  secret_string = jsonencode({
    oauthTokenEndpointUrl = local.entra_token_endpoint
    oauthClientId         = azuread_application.consumer.client_id
    oauthClientSecret     = azuread_application_password.consumer.value
  })
}

# -----------------------------------------------------------------------------
# IAM execution role
# -----------------------------------------------------------------------------

data "aws_iam_policy_document" "assume_lambda" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "consumer" {
  name_prefix        = "${var.name_prefix}-"
  assume_role_policy = data.aws_iam_policy_document.assume_lambda.json
}

resource "aws_iam_role_policy_attachment" "basic_execution" {
  role       = aws_iam_role.consumer.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# The ESM reads the OAuth secret using the function's execution role.
data "aws_iam_policy_document" "read_oauth_secret" {
  statement {
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [aws_secretsmanager_secret.oauth.arn]
  }
}

resource "aws_iam_role_policy" "read_oauth_secret" {
  name   = "read-entra-oauth-secret"
  role   = aws_iam_role.consumer.id
  policy = data.aws_iam_policy_document.read_oauth_secret.json
}

# -----------------------------------------------------------------------------
# Lambda function (no dependencies: the ESM does all Kafka work)
# -----------------------------------------------------------------------------

data "archive_file" "handler" {
  type        = "zip"
  source_file = "${path.module}/../handler.py"
  output_path = "${path.module}/.build/handler.zip"
}

resource "aws_cloudwatch_log_group" "consumer" {
  name              = "/aws/lambda/${var.name_prefix}"
  retention_in_days = var.log_retention_days
}

resource "aws_lambda_function" "consumer" {
  function_name    = var.name_prefix
  role             = aws_iam_role.consumer.arn
  runtime          = "python3.14"
  handler          = "handler.lambda_handler"
  filename         = data.archive_file.handler.output_path
  source_code_hash = data.archive_file.handler.output_base64sha256
  timeout          = var.lambda_timeout_seconds
  memory_size      = 256

  logging_config {
    log_format = "Text"
    log_group  = aws_cloudwatch_log_group.consumer.name
  }

  depends_on = [aws_iam_role_policy_attachment.basic_execution]
}

# -----------------------------------------------------------------------------
# Event source mapping: Confluent Cloud -> Lambda over SASL/OAUTHBEARER (Entra)
# -----------------------------------------------------------------------------

resource "aws_lambda_event_source_mapping" "confluent" {
  function_name                      = aws_lambda_function.consumer.arn
  topics                             = [var.topic_name]
  starting_position                  = var.starting_position
  batch_size                         = var.batch_size
  maximum_batching_window_in_seconds = var.maximum_batching_window_in_seconds

  self_managed_event_source {
    endpoints = {
      KAFKA_BOOTSTRAP_SERVERS = local.bootstrap_servers
    }
  }

  self_managed_kafka_event_source_config {
    consumer_group_id = var.consumer_group_id
  }

  # client_credentials grant: Lambda fetches and refreshes the Entra token.
  source_access_configuration {
    type = "OAUTHBEARER_AUTH"
    uri  = aws_secretsmanager_secret.oauth.arn
  }

  # Entra v2 endpoint requires a scope; <App ID URI>/.default -> aud = client ID.
  source_access_configuration {
    type = "OAUTHBEARER_SCOPE"
    uri  = local.entra_scope
  }

  # Confluent Cloud SASL extensions (literal values, not ARNs).
  source_access_configuration {
    type = "OAUTHBEARER_LOGICAL_CLUSTER"
    uri  = var.confluent_cluster_id
  }

  source_access_configuration {
    type = "OAUTHBEARER_IDENTITY_POOL"
    uri  = confluent_identity_pool.consumer.id
  }

  # RBAC and the secret must exist before Lambda starts polling, otherwise the
  # first connection attempts fail with auth/authorization errors.
  depends_on = [
    aws_secretsmanager_secret_version.oauth,
    aws_iam_role_policy.read_oauth_secret,
    confluent_role_binding.topic_read,
    confluent_role_binding.group_read,
  ]
}
