variable "name_prefix" {
  description = "Prefix for all created resources (Lambda, IAM role, secret, Entra app, identity pool)."
  type        = string
  default     = "cc-entra-consumer"
}

variable "aws_region" {
  description = "AWS region for the Lambda function, event source mapping and secret."
  type        = string
  default     = "us-west-2"
}

variable "entra_tenant_id" {
  description = "Azure Entra ID tenant ID (GUID)."
  type        = string
}

variable "client_secret_rotation_days" {
  description = "Entra client secret lifetime. Terraform creates a new secret when it expires (on the next apply)."
  type        = number
  default     = 180
}

variable "confluent_identity_provider_id" {
  description = "Existing Confluent Cloud identity provider (op-xxxx) for this Entra tenant. Its issuer must be https://login.microsoftonline.com/<tenant>/v2.0. Find it with: confluent iam provider list"
  type        = string
}

variable "confluent_environment_id" {
  description = "Confluent Cloud environment ID (env-xxxxx)."
  type        = string
}

variable "confluent_cluster_id" {
  description = "Confluent Cloud Kafka cluster ID (lkc-xxxxx)."
  type        = string
}

variable "topic_name" {
  description = "Topic the Lambda consumes from. Must already exist."
  type        = string
}

variable "consumer_group_id" {
  description = "Kafka consumer group used by the event source mapping."
  type        = string
  default     = "lambda-entra-consumer"
}

variable "starting_position" {
  description = "Where to start reading when the consumer group has no committed offsets: TRIM_HORIZON or LATEST."
  type        = string
  default     = "TRIM_HORIZON"

  validation {
    condition     = contains(["TRIM_HORIZON", "LATEST"], var.starting_position)
    error_message = "starting_position must be TRIM_HORIZON or LATEST."
  }
}

variable "batch_size" {
  description = "Maximum records per Lambda invocation."
  type        = number
  default     = 100
}

variable "maximum_batching_window_in_seconds" {
  description = "How long the ESM waits to fill a batch before invoking."
  type        = number
  default     = 1
}

variable "lambda_timeout_seconds" {
  description = "Function timeout. Kafka event source mappings support at most 14 minutes (840s)."
  type        = number
  default     = 60

  validation {
    condition     = var.lambda_timeout_seconds <= 840
    error_message = "Kafka event source mappings support function timeouts of at most 840 seconds."
  }
}

variable "log_retention_days" {
  description = "CloudWatch log retention for the function."
  type        = number
  default     = 14
}
