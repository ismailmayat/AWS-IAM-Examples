# -----------------------------------------------------------------------------
# Confluent Cloud: trust Entra ID, map the app to an identity pool, grant RBAC
# -----------------------------------------------------------------------------

data "confluent_kafka_cluster" "this" {
  id = var.confluent_cluster_id
  environment {
    id = var.confluent_environment_id
  }
}

# Existing organization-level OIDC identity provider for the Entra tenant
# (not managed here). Find it with: confluent iam provider list
data "confluent_identity_provider" "entra" {
  id = var.confluent_identity_provider_id
}

# Only tokens issued to (azp) and for (aud) this app registration land in the pool.
resource "confluent_identity_pool" "consumer" {
  identity_provider {
    id = data.confluent_identity_provider.entra.id
  }
  display_name   = "${var.name_prefix}-pool"
  description    = "Lambda ESM consumer (Entra app ${azuread_application.consumer.client_id})"
  identity_claim = "claims.sub"
  filter         = "claims.aud == \"${azuread_application.consumer.client_id}\" && claims.azp == \"${azuread_application.consumer.client_id}\""

  lifecycle {
    precondition {
      condition     = trimsuffix(data.confluent_identity_provider.entra.issuer, "/") == local.entra_issuer
      error_message = "Identity provider ${var.confluent_identity_provider_id} has issuer ${data.confluent_identity_provider.entra.issuer}, but tokens from tenant ${var.entra_tenant_id} are issued by ${local.entra_issuer}."
    }
  }
}

locals {
  cluster_crn = "${data.confluent_kafka_cluster.this.rbac_crn}/kafka=${data.confluent_kafka_cluster.this.id}"

  # "SASL_SSL://pkc-xxxxx.region.provider.confluent.cloud:9092" -> host:port
  bootstrap_servers = replace(data.confluent_kafka_cluster.this.bootstrap_endpoint, "SASL_SSL://", "")
}

resource "confluent_role_binding" "topic_read" {
  principal   = "User:${confluent_identity_pool.consumer.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${local.cluster_crn}/topic=${var.topic_name}"
}

resource "confluent_role_binding" "group_read" {
  principal   = "User:${confluent_identity_pool.consumer.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${local.cluster_crn}/group=${var.consumer_group_id}"
}
