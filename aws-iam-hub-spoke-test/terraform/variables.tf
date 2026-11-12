variable "aws_region" {
  type    = string
  default = "eu-west-1"
}

variable "environment" {
  description = "Environment segment of the hub role name (/kafka/<env>-<workload>)."
  type        = string
  default     = "dev"
}

variable "workload" {
  description = "Workload segment of the hub role name and identity pool name."
  type        = string
  default     = "lambda-consumer"
}

variable "owner" {
  description = "Value of the required `owner` tag on the hub role."
  type        = string
}

variable "token_audience" {
  description = "The only audience the hub role may mint tokens for."
  type        = string
  default     = "confluent-cloud-kafka"
}

variable "token_max_duration_seconds" {
  description = "Cap enforced by the hub role permission policy (sts:DurationSeconds)."
  type        = number
  default     = 900
}

variable "confluent_environment_id" {
  type = string
}

variable "confluent_cluster_id" {
  type = string
}

variable "topic_name" {
  type = string
}

variable "consumer_group_id" {
  type    = string
  default = "dev-lambda-consumer-hubtest"
}
