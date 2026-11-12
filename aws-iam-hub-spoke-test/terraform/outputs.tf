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

output "esm_function_name" {
  value = var.enable_esm_test ? aws_lambda_function.esm[0].function_name : null
}

output "esm_uuid" {
  value = var.enable_esm_test ? aws_lambda_event_source_mapping.esm[0].uuid : null
}

output "esm_runtime_role_arn" {
  value = var.enable_esm_test ? aws_iam_role.esm_runtime[0].arn : null
}
