# -----------------------------------------------------------------------------
# OPTIONAL: hub-and-spoke with the Lambda native Kafka trigger (ESM).
# Expected result: authentication FAILS.
#
# The ESM's IAM_OAUTHBEARER_AUTH mode calls sts:GetWebIdentityToken as the
# function's execution role, with no way to assume the hub role first. The
# token's `sub` is therefore the spoke execution role, which the hub identity
# pool (filter: sub == hub role ARN) rejects.
#
# Single-account caveat: here the spoke shares the hub's issuer, so the
# failure is at the pool filter. In a real spoke account it would fail even
# earlier: outbound federation is not enabled there (and the root SCP denies
# it), so either GetWebIdentityToken is denied or the token carries an issuer
# Confluent Cloud doesn't trust.
#
# Enable with: terraform apply -var enable_esm_test=true
# -----------------------------------------------------------------------------

locals {
  esm_enabled  = var.enable_esm_test ? 1 : 0
  esm_name     = "${local.hub_role_name}-esm-hubtest"
  esm_group_id = "${var.consumer_group_id}-esm"
}

resource "aws_iam_role" "esm_runtime" {
  count              = local.esm_enabled
  name_prefix        = "${local.hub_role_name}-esm-runtime-"
  assume_role_policy = data.aws_iam_policy_document.assume_lambda.json
}

resource "aws_iam_role_policy_attachment" "esm_logs" {
  count      = local.esm_enabled
  role       = aws_iam_role.esm_runtime[0].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# What IAM_OAUTHBEARER_AUTH requires of the execution role: permission to mint
# a token as itself. Also allowed to assume the hub role, to show the ESM
# still doesn't use it.
data "aws_iam_policy_document" "esm_runtime" {
  count = local.esm_enabled

  statement {
    actions   = ["sts:GetWebIdentityToken"]
    resources = ["*"]
    condition {
      test     = "ForAllValues:StringEquals"
      variable = "sts:IdentityTokenAudience"
      values   = [var.token_audience]
    }
  }

  statement {
    actions   = ["sts:AssumeRole"]
    resources = [local.hub_role_arn]
  }
}

resource "aws_iam_role_policy" "esm_runtime" {
  count  = local.esm_enabled
  name   = "esm-mint-token"
  role   = aws_iam_role.esm_runtime[0].id
  policy = data.aws_iam_policy_document.esm_runtime[0].json
}

data "archive_file" "esm_function" {
  count       = local.esm_enabled
  type        = "zip"
  source_file = "${path.module}/../handler_esm.py"
  output_path = "${path.module}/.build/esm_function.zip"
}

resource "aws_cloudwatch_log_group" "esm" {
  count             = local.esm_enabled
  name              = "/aws/lambda/${local.esm_name}"
  retention_in_days = 7
}

resource "aws_lambda_function" "esm" {
  count            = local.esm_enabled
  function_name    = local.esm_name
  role             = aws_iam_role.esm_runtime[0].arn
  runtime          = "python3.13"
  handler          = "handler_esm.lambda_handler"
  filename         = data.archive_file.esm_function[0].output_path
  source_code_hash = data.archive_file.esm_function[0].output_base64sha256
  layers           = [aws_lambda_layer_version.deps.arn] # recent boto3 for diagnostics
  timeout          = 60
  memory_size      = 256

  environment {
    variables = {
      TOKEN_AUDIENCE = var.token_audience
      HUB_ROLE_ARN   = local.hub_role_arn
    }
  }

  logging_config {
    log_format = "Text"
    log_group  = aws_cloudwatch_log_group.esm[0].name
  }

  depends_on = [
    aws_iam_role_policy_attachment.esm_logs,
    aws_iam_role_policy.esm_runtime,
  ]
}

# Give the hub pool read access on the ESM's consumer group too, so the only
# thing that can fail is identity (pool match), not authorization.
resource "confluent_role_binding" "esm_group_read" {
  count       = local.esm_enabled
  principal   = "User:${confluent_identity_pool.hub_workload.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${local.cluster_crn}/group=${local.esm_group_id}"
}

resource "aws_lambda_event_source_mapping" "esm" {
  count             = local.esm_enabled
  function_name     = aws_lambda_function.esm[0].arn
  topics            = [var.topic_name]
  starting_position = "TRIM_HORIZON"
  batch_size        = 10

  self_managed_event_source {
    endpoints = {
      KAFKA_BOOTSTRAP_SERVERS = local.bootstrap_servers
    }
  }

  self_managed_kafka_event_source_config {
    consumer_group_id = local.esm_group_id
  }

  # Lambda mints the token as the execution role (no hop possible).
  # AWS requires the URI to be omitted for this type; the provider marks `uri`
  # as required but only sends it when non-empty, so "" omits it.
  source_access_configuration {
    type = "IAM_OAUTHBEARER_AUTH"
    uri  = ""
  }

  # The only extra setting this mode accepts: no logicalCluster or
  # identityPoolId extensions can be sent.
  source_access_configuration {
    type = "OAUTHBEARER_AUDIENCE"
    uri  = var.token_audience
  }

  depends_on = [
    aws_iam_role_policy.esm_runtime,
    confluent_role_binding.topic_read,
    confluent_role_binding.esm_group_read,
  ]
}
