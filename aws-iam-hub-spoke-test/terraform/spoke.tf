# -----------------------------------------------------------------------------
# SPOKE side (in a real deployment: the workload's own account).
# The runtime role is the Lambda execution role; its only extra right is
# sts:AssumeRole on its hub role.
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

resource "aws_iam_role" "runtime" {
  name_prefix        = "${local.hub_role_name}-runtime-"
  assume_role_policy = data.aws_iam_policy_document.assume_lambda.json
}

resource "aws_iam_role_policy_attachment" "runtime_logs" {
  role       = aws_iam_role.runtime.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "aws_iam_policy_document" "runtime_assume_hub" {
  statement {
    actions   = ["sts:AssumeRole", "sts:SetSourceIdentity"]
    resources = [local.hub_role_arn]
  }
}

resource "aws_iam_role_policy" "runtime_assume_hub" {
  name   = "assume-kafka-hub-role"
  role   = aws_iam_role.runtime.id
  policy = data.aws_iam_policy_document.runtime_assume_hub.json
}

# --- Lambda ------------------------------------------------------------------

resource "aws_lambda_layer_version" "deps" {
  layer_name          = "${local.hub_role_name}-deps"
  filename            = "${path.module}/.build/layer.zip" # ../build-layer.sh
  source_code_hash    = filebase64sha256("${path.module}/.build/layer.zip")
  compatible_runtimes = ["python3.13"]
}

data "archive_file" "function" {
  type        = "zip"
  output_path = "${path.module}/.build/function.zip"

  source {
    filename = "handler.py"
    content  = file("${path.module}/../handler.py")
  }

  source {
    filename = "aws-config"
    content = templatefile("${path.module}/../aws-config.tftpl", {
      profile      = "kafka-hub"
      hub_role_arn = local.hub_role_arn
      session_name = local.hub_role_name
      region       = var.aws_region
    })
  }
}

resource "aws_cloudwatch_log_group" "consumer" {
  name              = "/aws/lambda/${local.hub_role_name}-hubtest"
  retention_in_days = 7
}

resource "aws_lambda_function" "consumer" {
  function_name    = "${local.hub_role_name}-hubtest"
  role             = aws_iam_role.runtime.arn
  runtime          = "python3.13"
  handler          = "handler.lambda_handler"
  filename         = data.archive_file.function.output_path
  source_code_hash = data.archive_file.function.output_base64sha256
  layers           = [aws_lambda_layer_version.deps.arn]
  timeout          = 120
  memory_size      = 512

  environment {
    variables = {
      AWS_CONFIG_FILE        = "/var/task/aws-config"
      AWS_PROFILE            = "kafka-hub"
      KAFKA_HUB_PROFILE      = "kafka-hub"
      TOKEN_AUDIENCE         = var.token_audience
      TOKEN_DURATION_SECONDS = tostring(var.token_max_duration_seconds)
      BOOTSTRAP_SERVERS      = local.bootstrap_servers
      LOGICAL_CLUSTER        = var.confluent_cluster_id
      IDENTITY_POOL_ID       = confluent_identity_pool.hub_workload.id
      TOPIC                  = var.topic_name
      CONSUMER_GROUP_ID      = var.consumer_group_id
    }
  }

  logging_config {
    log_format = "Text"
    log_group  = aws_cloudwatch_log_group.consumer.name
  }

  depends_on = [
    aws_iam_role_policy_attachment.runtime_logs,
    aws_iam_role_policy.runtime_assume_hub,
    aws_iam_role_policy.hub_mint,
  ]
}
