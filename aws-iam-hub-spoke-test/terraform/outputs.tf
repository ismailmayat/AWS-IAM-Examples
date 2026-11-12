output "lambda_function_name" {
  value = aws_lambda_function.consumer.function_name
}

output "runtime_role_arn" {
  value = aws_iam_role.runtime.arn
}

output "hub_role_arn" {
  value = aws_iam_role.hub.arn
}

output "issuer" {
  value = local.issuer
}

output "confluent_identity_provider_id" {
  value = confluent_identity_provider.hub.id
}

output "confluent_identity_pool_id" {
  value = confluent_identity_pool.hub_workload.id
}

output "identity_pool_filter" {
  value = confluent_identity_pool.hub_workload.filter
}
