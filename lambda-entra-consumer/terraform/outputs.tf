output "lambda_function_name" {
  value = aws_lambda_function.consumer.function_name
}

output "event_source_mapping_uuid" {
  value = aws_lambda_event_source_mapping.confluent.uuid
}

output "bootstrap_servers" {
  value = local.bootstrap_servers
}

output "entra_client_id" {
  value = azuread_application.consumer.client_id
}

output "entra_scope" {
  value = local.entra_scope
}

output "confluent_identity_provider_id" {
  value = data.confluent_identity_provider.entra.id
}

output "confluent_identity_pool_id" {
  value = confluent_identity_pool.consumer.id
}

output "oauth_secret_arn" {
  value = aws_secretsmanager_secret.oauth.arn
}
